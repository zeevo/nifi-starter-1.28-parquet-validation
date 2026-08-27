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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.example.nifi.api.ParquetSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The service on its own: it has to hand back the file-level metadata as well as the rows, which is
 * the whole reason it exists.
 */
public class StandardParquetSourceTest {

    private static final Schema SCHEMA = new Schema.Parser().parse(
            "{\"type\":\"record\",\"name\":\"Row\",\"fields\":["
            + "{\"name\":\"id\",\"type\":[\"null\",\"long\"],\"default\":null},"
            + "{\"name\":\"name\",\"type\":[\"null\",\"string\"],\"default\":null}]}");

    private StandardParquetSource source;

    @BeforeEach
    public void init() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(ServiceTestProcessor.class);
        source = new StandardParquetSource();
        runner.addControllerService("parquet-source", source);
        runner.enableControllerService(source);
    }

    @Test
    public void testExposesFileLevelMetadata() throws IOException {
        final Map<String, String> custom = new LinkedHashMap<>();
        custom.put("source.system", "billing");
        custom.put("ingest.date", "2026-08-26");

        try (ParquetSource.Handle handle = source.open(write(custom, 4), "probe")) {
            final Map<String, String> metadata = handle.fileMetadata();

            assertEquals("billing", metadata.get("source.system"));
            assertEquals("2026-08-26", metadata.get("ingest.date"));
            // The writer generated keys are reported too: this is the footer verbatim.
            assertNotNull(metadata.get("parquet.avro.schema"));
            assertEquals("avro", metadata.get("writer.model.name"));
        }
    }

    @Test
    public void testExposesEncodingAndCounts() throws IOException {
        try (ParquetSource.Handle handle = source.open(write(new LinkedHashMap<>(), 4), "probe")) {
            assertEquals("SNAPPY", handle.compressionCodec());
            assertEquals(4, handle.rowCount());
            assertEquals(SCHEMA, new Schema.Parser().parse(handle.avroSchema()));
        }
    }

    @Test
    public void testIteratesRowsAndEndsWithNull() throws IOException {
        try (ParquetSource.Handle handle = source.open(write(new LinkedHashMap<>(), 3), "probe")) {
            for (long expected = 1; expected <= 3; expected++) {
                final GenericRecord record = (GenericRecord) handle.nextRecord();
                assertNotNull(record);
                assertEquals(expected, record.get("id"));
            }
            assertNull(handle.nextRecord());
        }
    }

    /**
     * Unlike NiFi's ParquetReader, which derives its schema from the first record and throws
     * EOFException when there is none, this can open a file with no rows in it.
     */
    @Test
    public void testOpensAZeroRowFile() throws IOException {
        try (ParquetSource.Handle handle = source.open(write(new LinkedHashMap<>(), 0), "probe")) {
            assertEquals(0, handle.rowCount());
            assertNull(handle.nextRecord());
            assertNotNull(handle.avroSchema());
        }
    }

    @Test
    public void testRejectsContentThatIsNotParquet() {
        assertThrows(RuntimeException.class,
                () -> source.open("hello".getBytes(java.nio.charset.StandardCharsets.UTF_8), "probe"));
    }

    private static byte[] write(final Map<String, String> metadata, final int rows) throws IOException {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter
                .<GenericRecord>builder(new BufferOutputFile(bytes))
                .withSchema(SCHEMA)
                .withDataModel(GenericData.get())
                .withConf(new PlainParquetConfiguration())
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withExtraMetaData(metadata)
                .build()) {
            for (long i = 1; i <= rows; i++) {
                final GenericRecord row = new GenericData.Record(SCHEMA);
                row.put("id", i);
                row.put("name", "user-" + i);
                writer.write(row);
            }
        }
        return bytes.toByteArray();
    }

    private static final class BufferOutputFile implements OutputFile {

        private final OutputStream out;

        BufferOutputFile(final OutputStream out) {
            this.out = out;
        }

        @Override
        public PositionOutputStream create(final long blockSizeHint) {
            return stream();
        }

        @Override
        public PositionOutputStream createOrOverwrite(final long blockSizeHint) {
            return stream();
        }

        @Override
        public boolean supportsBlockSize() {
            return false;
        }

        @Override
        public long defaultBlockSize() {
            return 0;
        }

        private PositionOutputStream stream() {
            return new PositionOutputStream() {
                private long position;

                @Override
                public long getPos() {
                    return position;
                }

                @Override
                public void write(final int b) throws IOException {
                    out.write(b);
                    position++;
                }

                @Override
                public void write(final byte[] buffer, final int offset, final int length) throws IOException {
                    out.write(buffer, offset, length);
                    position += length;
                }
            };
        }
    }
}
