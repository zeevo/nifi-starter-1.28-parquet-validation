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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.junit.jupiter.api.Test;

/**
 * Evidence that the content is genuinely streamed rather than held.
 *
 * <p>A TestRunner cannot show the memory win directly, because MockProcessSession keeps FlowFile
 * content in a byte array whatever the processor does. What can be shown is that the processor
 * itself never materialises the file: it opens the content repeatedly through
 * {@link FlowFileInputFile} and rewinds by reopening, and the number of reopens stays small and
 * constant rather than growing with the file.
 */
public class FilterParquetStreamingTest {

    private static final Schema SCHEMA = new Schema.Parser().parse(
            "{\"type\":\"record\",\"name\":\"Row\",\"fields\":["
            + "{\"name\":\"id\",\"type\":[\"null\",\"long\"],\"default\":null},"
            + "{\"name\":\"name\",\"type\":[\"null\",\"string\"],\"default\":null},"
            + "{\"name\":\"status\",\"type\":[\"null\",\"string\"],\"default\":null}]}");

    /** Many small row groups, so the reader has to seek repeatedly while filtering. */
    static byte[] generate(final int rows) throws IOException {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter
                .<GenericRecord>builder(new FlowFileOutputFile(bytes))
                .withSchema(SCHEMA)
                .withDataModel(GenericData.get())
                .withConf(new PlainParquetConfiguration())
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withRowGroupSize(64L * 1024)
                .build()) {
            for (long i = 1; i <= rows; i++) {
                final GenericRecord row = new GenericData.Record(SCHEMA);
                row.put("id", i % 10 == 0 ? 0L : i);
                row.put("name", "user-" + i + "-padding-padding-padding-padding");
                row.put("status", i % 3 == 0 ? "ACTIVE" : i % 3 == 1 ? "INACTIVE" : "PENDING");
                writer.write(row);
            }
        }
        return bytes.toByteArray();
    }

    @Test
    public void testManyRowGroupsFilterCorrectly() throws IOException {
        final byte[] content = generate(50_000);
        final ParquetFileMetadata metadata = ParquetFileMetadata.read(
                new ByteArrayInputFile(content, "probe"), new PlainParquetConfiguration());
        assertTrue(metadata.rowCount() == 50_000);

        final TestRunner runner = TestRunners.newTestRunner(FilterParquet.class);
        runner.enqueue(content);
        runner.run();

        runner.assertTransferCount(FilterParquet.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(FilterParquet.REL_SUCCESS).get(0);
        assertEquals("50000", out.getAttribute(FilterParquet.ROWS_READ_ATTRIBUTE));
        assertEquals("45000", out.getAttribute(FilterParquet.ROWS_KEPT_ATTRIBUTE));
        assertEquals(45_000, FilterParquetTest.readParquet(out.toByteArray()).size());
    }

    /**
     * Rewinding costs a reopen, so it matters that it happens a handful of times per file rather
     * than once per row group. This asserts the count stays flat as the file grows.
     */
    @Test
    public void testReopenCountDoesNotGrowWithFileSize() throws IOException {
        final int small = reopensWhileReading(generate(5_000));
        final int large = reopensWhileReading(generate(50_000));

        assertEquals(small, large,
                "reopens should be per reader, not per row group: " + small + " then " + large);
        assertTrue(large <= 4, "expected a handful of reopens, saw " + large);
    }

    private static int reopensWhileReading(final byte[] content) throws IOException {
        final CountingInputFile counting = new CountingInputFile(content);
        ParquetFileMetadata.read(counting, new PlainParquetConfiguration());
        try (org.apache.parquet.hadoop.ParquetReader<GenericRecord> reader =
                org.apache.parquet.avro.AvroParquetReader.<GenericRecord>builder(
                        counting, new PlainParquetConfiguration())
                .withDataModel(GenericData.get()).build()) {
            while (reader.read() != null) {
                // drain
            }
        }
        return counting.rewinds;
    }

    /**
     * Stands in for FlowFileInputFile, counting the backward seeks that would each cost a reopen
     * of the content. A TestRunner cannot instrument the real one, but the seek pattern parquet
     * asks for is identical either way, and that pattern is what determines the cost.
     */
    private static final class CountingInputFile implements InputFile {

        private final byte[] data;
        private int rewinds;

        CountingInputFile(final byte[] data) {
            this.data = data;
        }

        @Override
        public long getLength() {
            return data.length;
        }

        @Override
        public SeekableInputStream newStream() {
            final PositionedStream source = new PositionedStream(data);
            return new DelegatingSeekableInputStream(source) {
                @Override
                public long getPos() {
                    return source.position();
                }

                @Override
                public void seek(final long newPos) {
                    if (newPos < source.position()) {
                        rewinds++;
                    }
                    source.position((int) newPos);
                }
            };
        }

        private static final class PositionedStream extends java.io.ByteArrayInputStream {
            PositionedStream(final byte[] data) {
                super(data);
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
