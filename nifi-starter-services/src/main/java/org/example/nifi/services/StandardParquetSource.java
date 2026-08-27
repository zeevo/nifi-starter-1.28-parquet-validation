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
package org.example.nifi.services;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.MessageType;
import org.example.nifi.api.ParquetSource;

@Tags({"parquet", "reader", "metadata", "record"})
@CapabilityDescription("Opens Parquet content and exposes both its rows and its file-level "
        + "key/value metadata. NiFi's own ParquetReader exposes only the rows, because the "
        + "RecordReader API it satisfies has no notion of anything file-level.")
public class StandardParquetSource extends AbstractControllerService implements ParquetSource {

    private static final ParquetConfiguration CONFIGURATION = new PlainParquetConfiguration();

    @Override
    public Handle open(final byte[] content, final String name) throws IOException {
        final InputFile inputFile = new ByteArrayInputFile(content, name);

        final ParquetMetadata footer;
        try (ParquetFileReader reader = ParquetFileReader.open(
                inputFile, ParquetReadOptions.builder(CONFIGURATION).build())) {
            footer = reader.getFooter();
        }

        final Map<String, String> metadata =
                Collections.unmodifiableMap(new TreeMap<>(footer.getFileMetaData().getKeyValueMetaData()));
        final MessageType parquetSchema = footer.getFileMetaData().getSchema();
        final String embedded = metadata.get("parquet.avro.schema");
        final Schema avroSchema = embedded == null
                ? new AvroSchemaConverter().convert(parquetSchema)
                : new Schema.Parser().parse(embedded);

        long rows = 0;
        long uncompressed = 0;
        for (final BlockMetaData block : footer.getBlocks()) {
            rows += block.getRowCount();
            uncompressed += block.getTotalByteSize();
        }

        return new ParquetHandle(inputFile, metadata, avroSchema, codecOf(footer),
                footer.getBlocks().isEmpty() ? 0 : Math.max(1L, uncompressed / footer.getBlocks().size()),
                rows);
    }

    private static CompressionCodecName codecOf(final ParquetMetadata footer) {
        if (footer.getBlocks().isEmpty() || footer.getBlocks().get(0).getColumns().isEmpty()) {
            return CompressionCodecName.UNCOMPRESSED;
        }
        return footer.getBlocks().get(0).getColumns().get(0).getCodec();
    }

    private static final class ParquetHandle implements Handle {

        private final Map<String, String> metadata;
        private final Schema avroSchema;
        private final CompressionCodecName codec;
        private final long rowGroupSize;
        private final long rowCount;
        private final ParquetReader<GenericRecord> reader;

        ParquetHandle(final InputFile inputFile, final Map<String, String> metadata,
                final Schema avroSchema, final CompressionCodecName codec,
                final long rowGroupSize, final long rowCount) throws IOException {
            this.metadata = metadata;
            this.avroSchema = avroSchema;
            this.codec = codec;
            this.rowGroupSize = rowGroupSize;
            this.rowCount = rowCount;
            this.reader = AvroParquetReader.<GenericRecord>builder(inputFile, CONFIGURATION)
                    .withDataModel(GenericData.get())
                    .build();
        }

        @Override
        public Map<String, String> fileMetadata() {
            return metadata;
        }

        @Override
        public String avroSchema() {
            return avroSchema.toString();
        }

        @Override
        public String compressionCodec() {
            return codec.name();
        }

        @Override
        public long rowGroupSize() {
            return rowGroupSize;
        }

        @Override
        public long rowCount() {
            return rowCount;
        }

        @Override
        public Object nextRecord() throws IOException {
            return reader.read();
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }
    }

    /** A parquet InputFile over a byte array; the footer sits at the end, so reads must seek. */
    private static final class ByteArrayInputFile implements InputFile {

        private final byte[] data;
        private final String name;

        ByteArrayInputFile(final byte[] data, final String name) {
            this.data = data;
            this.name = name;
        }

        @Override
        public long getLength() {
            return data.length;
        }

        @Override
        public SeekableInputStream newStream() {
            final CursorStream cursor = new CursorStream(data);
            return new DelegatingSeekableInputStream(cursor) {
                @Override
                public long getPos() {
                    return cursor.position();
                }

                @Override
                public void seek(final long newPos) {
                    cursor.position((int) newPos);
                }
            };
        }

        @Override
        public String toString() {
            return name;
        }

        private static final class CursorStream extends ByteArrayInputStream {
            CursorStream(final byte[] buf) {
                super(buf);
            }

            int position() {
                return this.pos;
            }

            void position(final int newPos) {
                this.pos = newPos;
            }
        }
    }
}
