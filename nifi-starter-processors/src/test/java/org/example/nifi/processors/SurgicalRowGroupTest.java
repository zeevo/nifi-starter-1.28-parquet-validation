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
import java.util.function.LongPredicate;

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
import org.junit.jupiter.api.Test;

/**
 * The property this branch exists for: a row group with nothing wrong in it is copied across as
 * bytes rather than decoded and re-encoded.
 */
public class SurgicalRowGroupTest {

    private static final Schema SCHEMA = new Schema.Parser().parse(
            "{\"type\":\"record\",\"name\":\"Row\",\"fields\":["
            + "{\"name\":\"id\",\"type\":[\"null\",\"long\"],\"default\":null},"
            + "{\"name\":\"name\",\"type\":[\"null\",\"string\"],\"default\":null},"
            + "{\"name\":\"status\",\"type\":[\"null\",\"string\"],\"default\":null}]}");

    /** 20,000 rows over many small row groups; {@code bad} decides which ids are invalid. */
    private static byte[] generate(final LongPredicate bad) throws IOException {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter
                .<GenericRecord>builder(new FlowFileOutputFile(bytes))
                .withSchema(SCHEMA)
                .withDataModel(GenericData.get())
                .withConf(new PlainParquetConfiguration())
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withRowGroupSize(32L * 1024)
                .build()) {
            for (long i = 1; i <= 20_000; i++) {
                final GenericRecord row = new GenericData.Record(SCHEMA);
                row.put("id", bad.test(i) ? 0L : i);
                row.put("name", "user-" + i + "-padding-padding-padding");
                row.put("status", i % 3 == 0 ? "ACTIVE" : i % 3 == 1 ? "INACTIVE" : "PENDING");
                writer.write(row);
            }
        }
        return bytes.toByteArray();
    }

    private static MockFlowFile filter(final byte[] content) {
        final TestRunner runner = TestRunners.newTestRunner(FilterParquet.class);
        runner.enqueue(content);
        runner.run();
        runner.assertTransferCount(FilterParquet.REL_SUCCESS, 1);
        return runner.getFlowFilesForRelationship(FilterParquet.REL_SUCCESS).get(0);
    }

    private static int copied(final MockFlowFile f) {
        return Integer.parseInt(f.getAttribute(FilterParquet.ROW_GROUPS_COPIED_ATTRIBUTE));
    }

    private static int rewritten(final MockFlowFile f) {
        return Integer.parseInt(f.getAttribute(FilterParquet.ROW_GROUPS_REWRITTEN_ATTRIBUTE));
    }

    /** Bad rows confined to the first 500: almost every row group should be copied untouched. */
    @Test
    public void testConcentratedFailuresLeaveMostRowGroupsUntouched() throws IOException {
        final MockFlowFile out = filter(generate(i -> i <= 500));

        assertTrue(copied(out) > rewritten(out) * 3,
                "expected most row groups copied, copied=" + copied(out)
                        + " rewritten=" + rewritten(out));
        assertEquals("20000", out.getAttribute(FilterParquet.ROWS_READ_ATTRIBUTE));
        assertEquals("19500", out.getAttribute(FilterParquet.ROWS_KEPT_ATTRIBUTE));
        assertEquals(19_500, FilterParquetTest.readParquet(out.toByteArray()).size());
    }

    /** A file with nothing wrong is copied in full: no row group is re-encoded at all. */
    @Test
    public void testCleanFileIsCopiedEntirely() throws IOException {
        final MockFlowFile out = filter(generate(i -> false));

        assertEquals(0, rewritten(out));
        assertTrue(copied(out) > 1, "fixture should have several row groups");
        assertEquals("20000", out.getAttribute(FilterParquet.ROWS_KEPT_ATTRIBUTE));
    }

    /** Failures spread through every row group: nothing can be copied, and that is correct too. */
    @Test
    public void testFailuresEverywhereRewriteEverything() throws IOException {
        final MockFlowFile out = filter(generate(i -> i % 10 == 0));

        assertEquals(0, copied(out));
        assertTrue(rewritten(out) > 1);
        assertEquals("18000", out.getAttribute(FilterParquet.ROWS_KEPT_ATTRIBUTE));
        assertEquals(18_000, FilterParquetTest.readParquet(out.toByteArray()).size());
    }

    /**
     * The payoff. Copied row groups keep the input's exact boundaries, so a mostly clean file comes
     * out with essentially the same layout, where a full re-encode would reflow all of it.
     */
    @Test
    public void testCopiedRowGroupsKeepTheirOriginalBoundaries() throws IOException {
        final byte[] input = generate(i -> i <= 500);
        final ParquetFileMetadata before = ParquetFileMetadata.read(
                new ByteArrayInputFile(input, "in"), new PlainParquetConfiguration());
        final MockFlowFile out = filter(input);
        final ParquetFileMetadata after = ParquetFileMetadata.read(
                new ByteArrayInputFile(out.toByteArray(), "out"), new PlainParquetConfiguration());

        assertEquals(before.codec(), after.codec());
        // Same number of row groups: the dirty ones were rewritten one-for-one, the rest copied.
        assertEquals(copied(out) + rewritten(out), before.rowGroupCount());
        assertEquals(before.all(), after.all());
    }
}
