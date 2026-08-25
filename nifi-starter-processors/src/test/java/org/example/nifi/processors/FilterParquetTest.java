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

import java.util.ArrayList;
import java.util.List;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The filtering rules, driven through mock reader and writer services.
 *
 * <p>Mocks keep these tests about which rows survive rather than about any one file format.
 * {@link FilterParquetRoundTripTest} covers the real Parquet services.
 */
public class FilterParquetTest {

    private TestRunner runner;
    private MockRecordParser reader;

    @BeforeEach
    public void init() throws InitializationException {
        runner = TestRunners.newTestRunner(FilterParquet.class);

        reader = new MockRecordParser();
        reader.addSchemaField(Item.ID, RecordFieldType.LONG);
        reader.addSchemaField(Item.NAME, RecordFieldType.STRING);
        reader.addSchemaField(Item.STATUS, RecordFieldType.STRING);
        runner.addControllerService("reader", reader);
        runner.enableControllerService(reader);

        // header false, quote false: one plain comma separated line per record, easy to assert on.
        final MockRecordWriter writer = new MockRecordWriter(null, false, false);
        runner.addControllerService("writer", writer);
        runner.enableControllerService(writer);

        runner.setProperty(FilterParquet.RECORD_READER, "reader");
        runner.setProperty(FilterParquet.RECORD_WRITER, "writer");
    }

    @Test
    public void testReaderAndWriterAreRequired() {
        final TestRunner bare = TestRunners.newTestRunner(FilterParquet.class);
        bare.assertNotValid();
    }

    @Test
    public void testAllRowsValidKeepsEverything() {
        reader.addRecord(1L, "bob", "ACTIVE");
        reader.addRecord(2L, "carol", "PENDING");

        final MockFlowFile out = filter();
        assertCounts(out, 2, 2, 0);
        assertEquals(2, lines(out).size());
    }

    /** The stated example: name null but status present survives. */
    @Test
    public void testNullNameWithStatusSurvives() {
        reader.addRecord(1L, null, "ACTIVE");

        assertCounts(filter(), 1, 1, 0);
    }

    @Test
    public void testNullStatusWithNameSurvives() {
        reader.addRecord(1L, "bob", null);

        assertCounts(filter(), 1, 1, 0);
    }

    @Test
    public void testNullIdIsRemoved() {
        reader.addRecord(null, "bob", "ACTIVE");

        assertCounts(filter(), 1, 0, 1);
    }

    @Test
    public void testNonPositiveIdIsRemoved() {
        reader.addRecord(0L, "bob", "ACTIVE");

        assertCounts(filter(), 1, 0, 1);
    }

    @Test
    public void testNameAndStatusBothNullIsRemoved() {
        reader.addRecord(1L, null, null);

        assertCounts(filter(), 1, 0, 1);
    }

    @Test
    public void testBlankNameIsRemoved() {
        reader.addRecord(1L, "   ", "ACTIVE");

        assertCounts(filter(), 1, 0, 1);
    }

    @Test
    public void testDisallowedStatusIsRemoved() {
        reader.addRecord(1L, "bob", "BOGUS");

        assertCounts(filter(), 1, 0, 1);
    }

    /** Only the failing rows go, and the survivors keep their order and values. */
    @Test
    public void testOnlyFailingRowsAreRemoved() {
        reader.addRecord(1L, "keep-one", "ACTIVE");
        reader.addRecord(0L, "drop-bad-id", "ACTIVE");
        reader.addRecord(2L, "keep-two", "PENDING");
        reader.addRecord(3L, null, null);
        reader.addRecord(4L, "keep-three", "INACTIVE");

        final MockFlowFile out = filter();
        assertCounts(out, 5, 3, 2);

        final List<String> lines = lines(out);
        assertEquals(3, lines.size());
        assertTrue(lines.get(0).contains("keep-one"), lines.get(0));
        assertTrue(lines.get(1).contains("keep-two"), lines.get(1));
        assertTrue(lines.get(2).contains("keep-three"), lines.get(2));
        assertTrue(out.getContent().indexOf("drop-bad-id") < 0, out.getContent());
    }

    @Test
    public void testEveryRowRemovedStillProducesAFile() {
        reader.addRecord(0L, "bad", "ACTIVE");
        reader.addRecord(null, "bad", "ACTIVE");

        final MockFlowFile out = filter();
        assertCounts(out, 2, 0, 2);
        assertEquals(0, lines(out).size());
    }

    @Test
    public void testEmptyInputProducesEmptyOutput() {
        assertCounts(filter(), 0, 0, 0);
    }

    /** A field the rules never look at still has to reach the output. */
    @Test
    public void testUnrelatedFieldsAreCarriedThrough() throws InitializationException {
        final TestRunner extra = TestRunners.newTestRunner(FilterParquet.class);
        final MockRecordParser wide = new MockRecordParser();
        wide.addSchemaField(Item.ID, RecordFieldType.LONG);
        wide.addSchemaField(Item.NAME, RecordFieldType.STRING);
        wide.addSchemaField(Item.STATUS, RecordFieldType.STRING);
        wide.addSchemaField("payload", RecordFieldType.STRING);
        wide.addRecord(1L, "bob", "ACTIVE", "carry-me");
        wide.addRecord(0L, "bad", "ACTIVE", "drop-me");
        extra.addControllerService("reader", wide);
        extra.enableControllerService(wide);
        final MockRecordWriter writer = new MockRecordWriter(null, false, false);
        extra.addControllerService("writer", writer);
        extra.enableControllerService(writer);
        extra.setProperty(FilterParquet.RECORD_READER, "reader");
        extra.setProperty(FilterParquet.RECORD_WRITER, "writer");

        extra.enqueue(new byte[0]);
        extra.run();

        extra.assertTransferCount(FilterParquet.REL_SUCCESS, 1);
        final MockFlowFile out = extra.getFlowFilesForRelationship(FilterParquet.REL_SUCCESS).get(0);
        assertTrue(out.getContent().contains("carry-me"), out.getContent());
        assertTrue(out.getContent().indexOf("drop-me") < 0, out.getContent());
    }

    /** Without every field the rules cannot run, so filtering is refused rather than guessed at. */
    @Test
    public void testMissingRuleFieldRoutesToFailure() throws InitializationException {
        final TestRunner narrow = TestRunners.newTestRunner(FilterParquet.class);
        final MockRecordParser noStatus = new MockRecordParser();
        noStatus.addSchemaField(Item.ID, RecordFieldType.LONG);
        noStatus.addSchemaField(Item.NAME, RecordFieldType.STRING);
        noStatus.addRecord(1L, "bob");
        narrow.addControllerService("reader", noStatus);
        narrow.enableControllerService(noStatus);
        final MockRecordWriter writer = new MockRecordWriter(null, false, false);
        narrow.addControllerService("writer", writer);
        narrow.enableControllerService(writer);
        narrow.setProperty(FilterParquet.RECORD_READER, "reader");
        narrow.setProperty(FilterParquet.RECORD_WRITER, "writer");

        narrow.enqueue(new byte[0]);
        narrow.run();

        narrow.assertAllFlowFilesTransferred(FilterParquet.REL_FAILURE, 1);
        // The half written copy must not escape.
        narrow.assertTransferCount(FilterParquet.REL_SUCCESS, 0);
    }

    @Test
    public void testUnparseableInputRoutesToFailure() throws InitializationException {
        final TestRunner broken = TestRunners.newTestRunner(FilterParquet.class);
        final MockRecordParser thrower = new MockRecordParser();
        thrower.failAfter(0);
        thrower.addSchemaField(Item.ID, RecordFieldType.LONG);
        thrower.addSchemaField(Item.NAME, RecordFieldType.STRING);
        thrower.addSchemaField(Item.STATUS, RecordFieldType.STRING);
        thrower.addRecord(1L, "bob", "ACTIVE");
        broken.addControllerService("reader", thrower);
        broken.enableControllerService(thrower);
        final MockRecordWriter writer = new MockRecordWriter(null, false, false);
        broken.addControllerService("writer", writer);
        broken.enableControllerService(writer);
        broken.setProperty(FilterParquet.RECORD_READER, "reader");
        broken.setProperty(FilterParquet.RECORD_WRITER, "writer");

        broken.enqueue(new byte[0]);
        broken.run();

        broken.assertAllFlowFilesTransferred(FilterParquet.REL_FAILURE, 1);
        broken.assertTransferCount(FilterParquet.REL_SUCCESS, 0);
    }

    @Test
    public void testOriginalIsForwardedUnchanged() {
        reader.addRecord(1L, "bob", "ACTIVE");

        runner.enqueue("the original bytes");
        runner.run();

        runner.assertTransferCount(FilterParquet.REL_ORIGINAL, 1);
        runner.getFlowFilesForRelationship(FilterParquet.REL_ORIGINAL).get(0)
                .assertContentEquals("the original bytes");
    }

    @Test
    public void testMimeTypeComesFromTheWriter() {
        reader.addRecord(1L, "bob", "ACTIVE");

        assertEquals("text/plain", filter().getAttribute("mime.type"));
    }

    private MockFlowFile filter() {
        runner.enqueue(new byte[0]);
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

    /** MockRecordWriter emits one line per record, so the surviving rows are countable. */
    private static List<String> lines(final MockFlowFile out) {
        final List<String> lines = new ArrayList<>();
        for (final String line : out.getContent().split("\n")) {
            if (!line.trim().isEmpty()) {
                lines.add(line);
            }
        }
        return lines;
    }
}
