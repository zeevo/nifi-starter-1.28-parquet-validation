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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.InputFile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/*
 * Fixtures are the ones described in ValidateParquetTest, plus:
 *   snappy-multi-row-group.parquet  4000 rows, SNAPPY, many small row groups, every 10th row bad
 */
public class FilterParquetTest {

    private TestRunner runner;

    @BeforeEach
    public void init() {
        runner = TestRunners.newTestRunner(FilterParquet.class);
    }

    @Test
    public void testValidFileIsUnchangedInContent() throws IOException {
        final MockFlowFile out = filter("valid.parquet");

        assertEquals("2", out.getAttribute(FilterParquet.ROWS_READ_ATTRIBUTE));
        assertEquals("2", out.getAttribute(FilterParquet.ROWS_KEPT_ATTRIBUTE));
        assertEquals("0", out.getAttribute(FilterParquet.ROWS_REMOVED_ATTRIBUTE));
        assertEquals(2, readRows(out).size());
    }

    @Test
    public void testInvalidRowIsRemoved() throws IOException {
        final MockFlowFile out = filter("invalid-zero-id.parquet");

        assertEquals("1", out.getAttribute(FilterParquet.ROWS_READ_ATTRIBUTE));
        assertEquals("0", out.getAttribute(FilterParquet.ROWS_KEPT_ATTRIBUTE));
        assertEquals("1", out.getAttribute(FilterParquet.ROWS_REMOVED_ATTRIBUTE));
        // Removing every row leaves a real Parquet file that simply has no rows in it.
        assertEquals(0, readRows(out).size());
    }

    @Test
    public void testOnlyFailingRowsAreRemoved() throws IOException {
        final MockFlowFile out = filter("invalid-many.parquet");

        assertEquals("20", out.getAttribute(FilterParquet.ROWS_READ_ATTRIBUTE));
        assertEquals("5", out.getAttribute(FilterParquet.ROWS_KEPT_ATTRIBUTE));
        assertEquals("15", out.getAttribute(FilterParquet.ROWS_REMOVED_ATTRIBUTE));

        // Rows 1 to 5 are the valid ones, so those exact ids should survive.
        final List<GenericRecord> kept = readRows(out);
        assertEquals(5, kept.size());
        for (int i = 0; i < kept.size(); i++) {
            assertEquals((long) (i + 1), kept.get(i).get("id"));
        }
    }

    /** Every surviving row must satisfy the same rules ValidateParquet enforces. */
    @Test
    public void testOutputPassesValidateParquet() throws IOException {
        final MockFlowFile filtered = filter("invalid-many.parquet");

        final TestRunner validator = TestRunners.newTestRunner(ValidateParquet.class);
        validator.enqueue(filtered.toByteArray());
        validator.run();

        validator.assertAllFlowFilesTransferred(ValidateParquet.REL_VALID, 1);
        assertEquals("5", validator.getFlowFilesForRelationship(ValidateParquet.REL_VALID).get(0)
                .getAttribute(ValidateParquet.RECORD_COUNT_ATTRIBUTE));
    }

    /** Columns the rules never look at still have to survive the round trip. */
    @Test
    public void testAllColumnsAreCarriedThrough() throws IOException {
        final MockFlowFile out = filter("valid.parquet");

        final List<GenericRecord> rows = readRows(out);
        assertEquals(3, rows.get(0).getSchema().getFields().size());
        assertEquals("bob", rows.get(0).get("name").toString());
        assertEquals("ACTIVE", rows.get(0).get("status").toString());
    }

    @Test
    public void testOriginalIsForwardedUnchanged() throws IOException {
        final byte[] content = fixtureBytes("valid.parquet");
        runner.enqueue(content);
        runner.run();

        runner.assertTransferCount(FilterParquet.REL_ORIGINAL, 1);
        runner.getFlowFilesForRelationship(FilterParquet.REL_ORIGINAL).get(0).assertContentEquals(content);
    }

    @Test
    public void testEmptyFileProducesEmptyFile() throws IOException {
        final MockFlowFile out = filter("empty.parquet");

        assertEquals("0", out.getAttribute(FilterParquet.ROWS_READ_ATTRIBUTE));
        assertEquals(0, readRows(out).size());
    }

    /** Without every field the rules cannot run, so filtering is refused rather than guessed at. */
    @Test
    public void testMissingColumnRoutesToFailure() throws IOException {
        runner.enqueue(fixtureBytes("missing-status-column.parquet"));
        runner.run();

        runner.assertAllFlowFilesTransferred(FilterParquet.REL_FAILURE, 1);
    }

    @Test
    public void testNonParquetContentRoutesToFailure() {
        runner.enqueue("hello".getBytes(StandardCharsets.UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred(FilterParquet.REL_FAILURE, 1);
        runner.getFlowFilesForRelationship(FilterParquet.REL_FAILURE).get(0).assertContentEquals("hello");
    }

    /**
     * Rewriting a Parquet file re-decides how it is encoded. These two properties are the ones
     * worth holding onto: the codec, because dropping to the writer default can make the filtered
     * copy larger than the original, and the row group sizing, because row groups are what a
     * downstream reader splits and skips on.
     */
    @Test
    public void testCodecAndRowGroupSizingSurviveTheRewrite() throws IOException {
        final byte[] input = fixtureBytes("snappy-multi-row-group.parquet");
        final ParquetMetadata before = footerOf(input);

        final MockFlowFile out = filter("snappy-multi-row-group.parquet");
        final ParquetMetadata after = footerOf(out.toByteArray());

        assertEquals(CompressionCodecName.SNAPPY, codecOf(after));
        assertEquals(codecOf(before), codecOf(after));

        assertEquals("4000", out.getAttribute(FilterParquet.ROWS_READ_ATTRIBUTE));
        assertEquals("3600", out.getAttribute(FilterParquet.ROWS_KEPT_ATTRIBUTE));
        assertEquals("400", out.getAttribute(FilterParquet.ROWS_REMOVED_ATTRIBUTE));

        // 10% of rows are gone, so the copy should have proportionally fewer row groups of about
        // the same size, not one big one. Without withRowGroupSize this collapses to a single group.
        final int groupsBefore = before.getBlocks().size();
        final int groupsAfter = after.getBlocks().size();
        assertTrue(groupsBefore > 5, "fixture should have many row groups, had " + groupsBefore);
        assertTrue(groupsAfter > groupsBefore / 2,
                "row groups collapsed from " + groupsBefore + " to " + groupsAfter);
    }

    private static ParquetMetadata footerOf(final byte[] content) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(
                new ByteArrayInputFile(content, "x"),
                ParquetReadOptions.builder(new PlainParquetConfiguration()).build())) {
            return reader.getFooter();
        }
    }

    private static CompressionCodecName codecOf(final ParquetMetadata footer) {
        final BlockMetaData first = footer.getBlocks().get(0);
        return first.getColumns().get(0).getCodec();
    }

    private MockFlowFile filter(final String fixture) throws IOException {
        runner.enqueue(fixtureBytes(fixture));
        runner.run();

        runner.assertTransferCount(FilterParquet.REL_SUCCESS, 1);
        runner.assertTransferCount(FilterParquet.REL_ORIGINAL, 1);
        return runner.getFlowFilesForRelationship(FilterParquet.REL_SUCCESS).get(0);
    }

    private static List<GenericRecord> readRows(final MockFlowFile flowFile) throws IOException {
        final InputFile in = new ByteArrayInputFile(flowFile.toByteArray(), "filtered");
        final List<GenericRecord> rows = new ArrayList<>();
        try (ParquetReader<GenericRecord> reader = AvroParquetReader
                .<GenericRecord>builder(in, new PlainParquetConfiguration())
                .withDataModel(GenericData.get())
                .build()) {
            GenericRecord record;
            while ((record = reader.read()) != null) {
                rows.add(record);
            }
        }
        return rows;
    }

    private static byte[] fixtureBytes(final String fixture) throws IOException {
        try (InputStream in = FilterParquetTest.class.getResourceAsStream("/parquet/" + fixture)) {
            assertNotNull(in, "missing fixture " + fixture);
            return in.readAllBytes();
        }
    }
}
