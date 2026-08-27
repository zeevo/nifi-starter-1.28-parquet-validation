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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Filtering behaviour, driven through NiFi's real ParquetReader controller service.
 *
 * <p>These cannot use a mock reader: the processor reads the incoming file's footer directly to
 * pick up its metadata and encoding, so the content has to be genuine Parquet whatever reader is
 * configured. {@link RecordApiMetadataLimitsTest} covers why that footer read is unavoidable and
 * {@link FilterParquetMetadataTest} covers what it preserves.
 *
 * <p>Fixtures are the ones described in ValidateParquetTest, plus:
 * <pre>
 *   metadata-rich.parquet  1000 rows, SNAPPY, small row groups, a payload column no rule reads,
 *                          four custom file-level metadata keys, every 10th row invalid
 * </pre>
 */
public class FilterParquetTest {

    private TestRunner runner;

    @BeforeEach
    public void init() {
        runner = TestRunners.newTestRunner(FilterParquet.class);

// No controller services: this variant reads and writes Parquet itself.
    }

    @Test
    public void testNeedsNoConfiguration() {
        assertTrue(TestRunners.newTestRunner(FilterParquet.class)
                .getProcessor().getPropertyDescriptors().isEmpty());
        runner.assertValid();
    }

    @Test
    public void testValidFileKeepsEveryRow() throws IOException {
        final MockFlowFile out = filter("valid.parquet");

        assertCounts(out, 2, 2, 0);
        final List<GenericRecord> rows = readParquet(out.toByteArray());
        assertEquals(2, rows.size());
        assertEquals("bob", rows.get(0).get("name").toString());
        assertEquals("carol", rows.get(1).get("name").toString());
    }

    /** The stated example: name null but status present survives. */
    @Test
    public void testNullNameWithStatusSurvives() throws IOException {
        assertCounts(filter("valid-null-name.parquet"), 1, 1, 0);
    }

    @Test
    public void testNullStatusWithNameSurvives() throws IOException {
        assertCounts(filter("valid-null-status.parquet"), 1, 1, 0);
    }

    @Test
    public void testNullIdIsRemoved() throws IOException {
        assertCounts(filter("invalid-null-id.parquet"), 1, 0, 1);
    }

    @Test
    public void testNonPositiveIdIsRemoved() throws IOException {
        assertCounts(filter("invalid-zero-id.parquet"), 1, 0, 1);
    }

    @Test
    public void testNameAndStatusBothNullIsRemoved() throws IOException {
        assertCounts(filter("invalid-both-null.parquet"), 1, 0, 1);
    }

    @Test
    public void testBlankNameIsRemoved() throws IOException {
        assertCounts(filter("invalid-blank-name.parquet"), 1, 0, 1);
    }

    @Test
    public void testDisallowedStatusIsRemoved() throws IOException {
        assertCounts(filter("invalid-bad-status.parquet"), 1, 0, 1);
    }

    @Test
    public void testOnlyFailingRowsAreRemoved() throws IOException {
        final MockFlowFile out = filter("invalid-many.parquet");

        assertCounts(out, 20, 5, 15);
        final List<GenericRecord> kept = readParquet(out.toByteArray());
        assertEquals(5, kept.size());
        for (int i = 0; i < kept.size(); i++) {
            assertEquals((long) (i + 1), kept.get(i).get("id"));
        }
    }

    /** The strongest statement of the contract: the output must satisfy ValidateParquet. */
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

    /** A column no rule looks at still has to reach the copy. */
    @Test
    public void testUnrelatedColumnsAreCarriedThrough() throws IOException {
        final MockFlowFile out = filter("metadata-rich.parquet");

        assertCounts(out, 1000, 900, 100);
        final List<GenericRecord> rows = readParquet(out.toByteArray());
        assertEquals(4, rows.get(0).getSchema().getFields().size());
        assertEquals("extra-column-not-touched-by-any-rule-1", rows.get(0).get("payload").toString());
    }

    @Test
    public void testEveryRowRemovedStillProducesReadableParquet() throws IOException {
        final MockFlowFile out = filter("invalid-zero-id.parquet");

        assertEquals(0, readParquet(out.toByteArray()).size());
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
    public void testMimeTypeIsParquet() throws IOException {
        assertEquals("application/parquet", filter("valid.parquet").getAttribute("mime.type"));
    }

    @Test
    public void testMissingRuleFieldRoutesToFailure() throws IOException {
        runner.enqueue(fixtureBytes("missing-status-column.parquet"));
        runner.run();

        runner.assertAllFlowFilesTransferred(FilterParquet.REL_FAILURE, 1);
        runner.assertTransferCount(FilterParquet.REL_SUCCESS, 0);
    }

    @Test
    public void testNonParquetContentRoutesToFailure() {
        runner.enqueue("hello".getBytes(StandardCharsets.UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred(FilterParquet.REL_FAILURE, 1);
        runner.assertTransferCount(FilterParquet.REL_SUCCESS, 0);
        runner.getFlowFilesForRelationship(FilterParquet.REL_FAILURE).get(0).assertContentEquals("hello");
    }

    /**
     * Reading Parquet directly sidesteps a NiFi 1.28.1 limitation: ParquetRecordReader reads the
     * first record in its constructor to derive the schema and throws EOFException when there is
     * none, so the record API cannot read a zero row file at all. parquet-java can, which means
     * two of these can be chained even when everything gets filtered out.
     */
    @Test
    public void testZeroRowInputIsHandled() throws IOException {
        final MockFlowFile out = filter("empty.parquet");

        assertCounts(out, 0, 0, 0);
        assertEquals(0, readParquet(out.toByteArray()).size());
    }

    private MockFlowFile filter(final String fixture) throws IOException {
        runner.enqueue(fixtureBytes(fixture));
        runner.run();

        runner.assertTransferCount(FilterParquet.REL_SUCCESS, 1);
        runner.assertTransferCount(FilterParquet.REL_ORIGINAL, 1);
        return runner.getFlowFilesForRelationship(FilterParquet.REL_SUCCESS).get(0);
    }

    private static void assertCounts(final MockFlowFile out, final long read, final long kept, final long removed) {
        assertEquals(Long.toString(read), out.getAttribute(FilterParquet.ROWS_READ_ATTRIBUTE));
        assertEquals(Long.toString(kept), out.getAttribute(FilterParquet.ROWS_KEPT_ATTRIBUTE));
        assertEquals(Long.toString(removed), out.getAttribute(FilterParquet.ROWS_REMOVED_ATTRIBUTE));
    }

    static List<GenericRecord> readParquet(final byte[] content) throws IOException {
        final List<GenericRecord> rows = new ArrayList<>();
        try (org.apache.parquet.hadoop.ParquetReader<GenericRecord> reader = AvroParquetReader
                .<GenericRecord>builder(new ByteArrayInputFile(content, "filtered"),
                        new PlainParquetConfiguration())
                .withDataModel(GenericData.get())
                .build()) {
            GenericRecord record;
            while ((record = reader.read()) != null) {
                rows.add(record);
            }
        }
        return rows;
    }

    static byte[] fixtureBytes(final String fixture) throws IOException {
        try (InputStream in = FilterParquetTest.class.getResourceAsStream("/parquet/" + fixture)) {
            assertNotNull(in, "missing fixture " + fixture);
            return in.readAllBytes();
        }
    }
}
