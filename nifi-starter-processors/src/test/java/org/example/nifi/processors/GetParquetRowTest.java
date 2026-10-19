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

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/* Uses the fixtures described in ValidateParquetTest. */
public class GetParquetRowTest {

    private TestRunner runner;

    @BeforeEach
    public void init() throws InitializationException {
        runner = TestRunners.newTestRunner(GetParquetRow.class);

        final MockRecordWriter writer = new MockRecordWriter(null, false);
        runner.addControllerService("writer", writer);
        runner.enableControllerService(writer);
        runner.setProperty(GetParquetRow.RECORD_WRITER, "writer");
    }

    @Test
    public void testFirstRow() throws IOException {
        assertRow("valid.parquet", "0", "1,bob,ACTIVE\n");
    }

    @Test
    public void testLastRow() throws IOException {
        assertRow("valid.parquet", "1", "2,carol,PENDING\n");
    }

    /** Index 1500 sits well past the first of 161 row groups, so the reader has to cross many of them. */
    @Test
    public void testRowInLaterRowGroup() throws IOException {
        final MockFlowFile out = assertSuccess("multi-row-group.parquet", "1500");
        final String row = out.getContent();
        assertEquals("1501,", row.substring(0, 5), row);
    }

    @Test
    public void testIndexPastLastRowIsFailure() throws IOException {
        assertFailure(fixtureBytes("valid.parquet"), "2");
    }

    @Test
    public void testFileWithNoRowsIsFailure() throws IOException {
        assertFailure(fixtureBytes("empty.parquet"), "0");
    }

    @Test
    public void testNonParquetContentIsFailure() throws IOException {
        assertFailure("hello".getBytes(StandardCharsets.UTF_8), "0");
    }

    @Test
    public void testNegativeIndexIsInvalid() {
        runner.setProperty(GetParquetRow.INDEX, "-1");
        runner.assertNotValid();
    }

    private void assertRow(final String fixture, final String index, final String expected) throws IOException {
        assertSuccess(fixture, index).assertContentEquals(expected);
    }

    private MockFlowFile assertSuccess(final String fixture, final String index) throws IOException {
        runner.setProperty(GetParquetRow.INDEX, index);
        runner.enqueue(fixtureBytes(fixture));
        runner.run();

        runner.assertAllFlowFilesTransferred(GetParquetRow.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(GetParquetRow.REL_SUCCESS).get(0);
        out.assertAttributeEquals("record.count", "1");
        out.assertAttributeEquals("mime.type", "text/plain");
        return out;
    }

    /** Failures carry the original content through unchanged. */
    private void assertFailure(final byte[] content, final String index) throws IOException {
        runner.setProperty(GetParquetRow.INDEX, index);
        runner.enqueue(content);
        runner.run();

        runner.assertAllFlowFilesTransferred(GetParquetRow.REL_FAILURE, 1);
        runner.getFlowFilesForRelationship(GetParquetRow.REL_FAILURE).get(0).assertContentEquals(content);
    }

    private static byte[] fixtureBytes(final String fixture) throws IOException {
        try (InputStream in = GetParquetRowTest.class.getResourceAsStream("/parquet/" + fixture)) {
            assertNotNull(in, "missing fixture " + fixture);
            return in.readAllBytes();
        }
    }
}
