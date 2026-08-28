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
package org.example.parquet.sdk;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.avro.AvroWriteSupport;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalOutputFile;

/**
 * A thin wrapper over {@link AvroParquetWriter} so the samples read as "what is being demonstrated"
 * rather than as builder boilerplate.
 *
 * <p>The builder options worth knowing about:
 *
 * <ul>
 *   <li><b>compression codec</b> applies per column chunk. UNCOMPRESSED, SNAPPY, GZIP and ZSTD are
 *       the common ones; SNAPPY is the usual default in the wider ecosystem, though
 *       {@code AvroParquetWriter} itself defaults to UNCOMPRESSED.</li>
 *   <li><b>row group size</b> is a target in <em>uncompressed</em> bytes. It decides how the rows
 *       are divided, and a row group is the unit a reader can skip or parallelise over. The default
 *       is 128 MB, so small files come out as a single row group unless you lower it.</li>
 *   <li><b>page size</b> divides a column chunk further. Pages are the unit of compression and of
 *       column index based skipping.</li>
 *   <li><b>dictionary encoding</b> is on by default. Parquet builds a dictionary per column chunk
 *       and falls back to plain encoding if the dictionary grows past its own size limit, which is
 *       why a high cardinality column can silently end up PLAIN.</li>
 *   <li><b>parquet uuid</b> decides whether Avro's uuid logical type survives as a Parquet UUID
 *       annotation. It is off by default, which surprises people.</li>
 *   <li><b>extra metadata</b> is arbitrary key/value data stored in the footer. Note that
 *       {@code parquet.avro.schema} and {@code writer.model.name} are written by the Avro support
 *       itself, and passing either of them here throws.</li>
 * </ul>
 */
public final class SampleWriter {

    private SampleWriter() {
    }

    /** Options for one sample file, so a sample only states what it cares about. */
    public static final class Options {

        private CompressionCodecName codec = CompressionCodecName.UNCOMPRESSED;
        private Long rowGroupSize;
        private Integer pageSize;
        private Boolean dictionaryEncoding;
        private Map<String, String> extraMetadata;
        private boolean parquetUuid;

        public Options codec(final CompressionCodecName value) {
            this.codec = value;
            return this;
        }

        public Options rowGroupSize(final long bytes) {
            this.rowGroupSize = bytes;
            return this;
        }

        public Options pageSize(final int bytes) {
            this.pageSize = bytes;
            return this;
        }

        public Options dictionaryEncoding(final boolean enabled) {
            this.dictionaryEncoding = enabled;
            return this;
        }

        public Options extraMetadata(final Map<String, String> metadata) {
            this.extraMetadata = metadata;
            return this;
        }

        /**
         * Write Avro's uuid logical type as a Parquet UUID annotation rather than a plain string.
         * Off by default in parquet-java, so without this an Avro uuid silently loses its
         * annotation on the way into Parquet.
         */
        public Options parquetUuid(final boolean enabled) {
            this.parquetUuid = enabled;
            return this;
        }
    }

    public static Options options() {
        return new Options();
    }

    /**
     * Writes {@code rows} to {@code file}, creating the parent directory and replacing any file
     * already there.
     */
    public static void write(final Path file, final Schema schema,
            final List<GenericRecord> rows, final Options options) throws IOException {

        Files.createDirectories(file.getParent());
        Files.deleteIfExists(file);

        final AvroParquetWriter.Builder<GenericRecord> builder = AvroParquetWriter
                .<GenericRecord>builder(new LocalOutputFile(file))
                .withSchema(schema)
                // GenericData is the plain map-like record model. The alternatives are SpecificData
                // for generated classes and ReflectData for POJOs.
                .withDataModel(GenericData.get())
                .withCompressionCodec(options.codec);

        if (options.rowGroupSize != null) {
            builder.withRowGroupSize(options.rowGroupSize);
        }
        if (options.pageSize != null) {
            builder.withPageSize(options.pageSize);
            // Without this, parquet only checks whether a page is full every 100 records, so a
            // small page size has no visible effect on a small file.
            builder.withMinRowCountForPageSizeCheck(1);
        }
        if (options.dictionaryEncoding != null) {
            builder.withDictionaryEncoding(options.dictionaryEncoding);
        }
        if (options.extraMetadata != null) {
            builder.withExtraMetaData(options.extraMetadata);
        }
        if (options.parquetUuid) {
            builder.config(AvroWriteSupport.WRITE_PARQUET_UUID, "true");
        }

        try (ParquetWriter<GenericRecord> writer = builder.build()) {
            for (final GenericRecord row : rows) {
                writer.write(row);
            }
        }
    }
}
