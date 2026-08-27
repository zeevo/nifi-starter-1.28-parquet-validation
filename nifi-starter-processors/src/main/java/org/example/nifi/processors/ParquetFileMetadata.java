/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.example.nifi.processors;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.avro.Schema;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.schema.MessageType;

/**
 * Everything about an incoming Parquet file that a filtered copy should inherit.
 *
 * <p>This all comes out of the footer, which is why it needs parquet-java rather than NiFi's
 * record API: file-level key/value metadata sits below the record abstraction, and neither
 * {@code RecordReader} nor {@code RecordSetWriter} has any notion of it. See
 * {@code RecordApiMetadataLimitsTest} for that spelled out and pinned down.
 */
final class ParquetFileMetadata {

    /**
     * Keys the Avro write support emits for itself. Passing any of these back to
     * {@code withExtraMetaData} is not merely redundant, it is fatal: parquet-java throws
     * {@code IllegalArgumentException: Duplicate metadata key ...}. They are excluded on the way in
     * and the writer puts them back on the way out, so nothing is actually lost.
     */
    private static final Set<String> WRITER_GENERATED_KEYS = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList("parquet.avro.schema", "writer.model.name")));

    private final Map<String, String> keyValueMetadata;
    private final MessageType parquetSchema;
    private final Schema avroSchema;
    private final CompressionCodecName codec;
    private final long rowGroupSize;
    private final long rowCount;
    private final int rowGroupCount;

    private ParquetFileMetadata(final Map<String, String> keyValueMetadata,
            final MessageType parquetSchema, final Schema avroSchema,
            final CompressionCodecName codec, final long rowGroupSize, final long rowCount,
            final int rowGroupCount) {
        this.keyValueMetadata = keyValueMetadata;
        this.parquetSchema = parquetSchema;
        this.avroSchema = avroSchema;
        this.codec = codec;
        this.rowGroupSize = rowGroupSize;
        this.rowCount = rowCount;
        this.rowGroupCount = rowGroupCount;
    }

    /** Reads the footer only. No row groups are decoded. */
    static ParquetFileMetadata read(final InputFile inputFile, final ParquetConfiguration conf) throws IOException {
        final ParquetMetadata footer;
        try (ParquetFileReader reader = ParquetFileReader.open(
                inputFile, ParquetReadOptions.builder(conf).build())) {
            footer = reader.getFooter();
        }

        final MessageType parquetSchema = footer.getFileMetaData().getSchema();
        final Map<String, String> all =
                new TreeMap<>(footer.getFileMetaData().getKeyValueMetaData());

        // The file's own Avro schema, when it carries one, is a more faithful thing to write with
        // than a schema converted back out of the Parquet types.
        final String embedded = all.get("parquet.avro.schema");
        final Schema schema = embedded == null
                ? new AvroSchemaConverter().convert(parquetSchema)
                : new Schema.Parser().parse(embedded);

        long rows = 0;
        long uncompressed = 0;
        for (final BlockMetaData block : footer.getBlocks()) {
            rows += block.getRowCount();
            uncompressed += block.getTotalByteSize();
        }

        return new ParquetFileMetadata(
                Collections.unmodifiableMap(all),
                parquetSchema,
                schema,
                codecOf(footer),
                footer.getBlocks().isEmpty() ? 0 : Math.max(1L, uncompressed / footer.getBlocks().size()),
                rows,
                footer.getBlocks().size());
    }

    /** Every key/value pair in the file's footer, including the writer generated ones. */
    Map<String, String> all() {
        return keyValueMetadata;
    }

    /**
     * The subset safe to hand to {@code ParquetWriter.Builder.withExtraMetaData}: everything the
     * incoming file carried, less the keys the writer insists on generating itself.
     */
    Map<String, String> inheritable() {
        final Map<String, String> inheritable = new LinkedHashMap<>();
        for (final Map.Entry<String, String> entry : keyValueMetadata.entrySet()) {
            if (!WRITER_GENERATED_KEYS.contains(entry.getKey())) {
                inheritable.put(entry.getKey(), entry.getValue());
            }
        }
        return inheritable;
    }

    static boolean isWriterGenerated(final String key) {
        return WRITER_GENERATED_KEYS.contains(key);
    }

    /** The Parquet schema as the footer records it, needed to open a ParquetFileWriter. */
    MessageType parquetSchema() {
        return parquetSchema;
    }

    Schema avroSchema() {
        return avroSchema;
    }

    CompressionCodecName codec() {
        return codec;
    }

    /** Zero when the file has no row groups, in which case the writer default should be used. */
    long rowGroupSize() {
        return rowGroupSize;
    }

    int rowGroupCount() {
        return rowGroupCount;
    }

    long rowCount() {
        return rowCount;
    }

    private static CompressionCodecName codecOf(final ParquetMetadata footer) {
        if (footer.getBlocks().isEmpty() || footer.getBlocks().get(0).getColumns().isEmpty()) {
            return CompressionCodecName.UNCOMPRESSED;
        }
        return footer.getBlocks().get(0).getColumns().get(0).getCodec();
    }
}
