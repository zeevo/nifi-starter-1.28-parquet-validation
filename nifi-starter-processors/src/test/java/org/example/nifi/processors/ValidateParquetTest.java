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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.nifi.processor.Relationship;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Fixtures live in src/test/resources/parquet. All of them use the Avro schema
 * {@code {id: ["null","long"], name: ["null","string"], status: ["null","string"]}} except
 * missing-status-column.parquet, which omits the status field entirely. None of them set
 * avro.java.string, so string values come back as Utf8 and the processor's Utf8 handling is
 * exercised for real.
 *
 * <pre>
 * valid.parquet                  (1,"bob","ACTIVE"), (2,"carol","PENDING")
 * valid-null-name.parquet        (1,null,"ACTIVE")
 * valid-null-status.parquet      (1,"bob",null)
 * empty.parquet                  no rows, full schema
 * invalid-null-id.parquet        (null,"bob","ACTIVE")
 * invalid-zero-id.parquet        (0,"bob","ACTIVE")
 * invalid-both-null.parquet      (1,null,null)
 * invalid-blank-name.parquet     (1,"   ","ACTIVE")
 * invalid-bad-status.parquet     (1,"bob","BOGUS")
 * invalid-many.parquet           rows 1-5 valid, rows 6-20 have id 0
 * missing-status-column.parquet  (1,"bob"), no status field
 * </pre>
 */
public class ValidateParquetTest {

    private TestRunner runner;

    @BeforeEach
    public void init() {
        runner = TestRunners.newTestRunner(ValidateParquet.class);
    }

    @Test
    public void testValidFile() throws IOException {
        final MockFlowFile out = runFixture("valid.parquet", ValidateParquet.REL_VALID);

        assertEquals("2", out.getAttribute(ValidateParquet.RECORD_COUNT_ATTRIBUTE));
        assertEquals("0", out.getAttribute(ValidateParquet.INVALID_COUNT_ATTRIBUTE));
        assertNull(out.getAttribute(ValidateParquet.VIOLATIONS_ATTRIBUTE));
    }

    @Test
    public void testNullNameWithStatusIsValid() throws IOException {
        runFixture("valid-null-name.parquet", ValidateParquet.REL_VALID);
    }

    @Test
    public void testNullStatusWithNameIsValid() throws IOException {
        runFixture("valid-null-status.parquet", ValidateParquet.REL_VALID);
    }

    @Test
    public void testFileWithNoRecordsIsValid() throws IOException {
        final MockFlowFile out = runFixture("empty.parquet", ValidateParquet.REL_VALID);

        assertEquals("0", out.getAttribute(ValidateParquet.RECORD_COUNT_ATTRIBUTE));
        assertEquals("0", out.getAttribute(ValidateParquet.INVALID_COUNT_ATTRIBUTE));
    }

    @Test
    public void testNullIdIsInvalid() throws IOException {
        assertViolation("invalid-null-id.parquet", "row 1: id is null");
    }

    @Test
    public void testNonPositiveIdIsInvalid() throws IOException {
        assertViolation("invalid-zero-id.parquet", "row 1: id must be positive");
    }

    @Test
    public void testNullNameAndNullStatusIsInvalid() throws IOException {
        assertViolation("invalid-both-null.parquet", "row 1: name and status are both null");
    }

    @Test
    public void testBlankNameIsInvalid() throws IOException {
        assertViolation("invalid-blank-name.parquet", "row 1: name is blank");
    }

    @Test
    public void testDisallowedStatusIsInvalid() throws IOException {
        assertViolation("invalid-bad-status.parquet", "row 1: status 'BOGUS' is not an allowed value");
    }

    @Test
    public void testViolationsAreCappedButStillCounted() throws IOException {
        final MockFlowFile out = runFixture("invalid-many.parquet", ValidateParquet.REL_INVALID);

        assertEquals("20", out.getAttribute(ValidateParquet.RECORD_COUNT_ATTRIBUTE));
        assertEquals("15", out.getAttribute(ValidateParquet.INVALID_COUNT_ATTRIBUTE));

        final String violations = out.getAttribute(ValidateParquet.VIOLATIONS_ATTRIBUTE);
        assertTrue(violations.startsWith("row 6: id must be positive"), violations);
        assertTrue(violations.contains("row 15: id must be positive"), violations);
        assertTrue(violations.endsWith("; ... and 5 more"), violations);
        // The cap is 10 messages, so row 16 onwards is only reflected in the count.
        assertTrue(!violations.contains("row 16:"), violations);
    }

    @Test
    public void testMissingRequiredColumnIsInvalid() throws IOException {
        final MockFlowFile out = runFixture("missing-status-column.parquet", ValidateParquet.REL_INVALID);

        assertEquals("schema is missing required field(s): status",
                out.getAttribute(ValidateParquet.VIOLATIONS_ATTRIBUTE));
        // No rows are read once the schema check fails, so no counts are reported.
        assertNull(out.getAttribute(ValidateParquet.RECORD_COUNT_ATTRIBUTE));
    }

    @Test
    public void testNonParquetContentIsInvalid() {
        runner.enqueue("hello");
        runner.run();

        runner.assertAllFlowFilesTransferred(ValidateParquet.REL_INVALID, 1);
        final String violations = runner.getFlowFilesForRelationship(ValidateParquet.REL_INVALID)
                .get(0).getAttribute(ValidateParquet.VIOLATIONS_ATTRIBUTE);
        assertTrue(violations.startsWith("not a readable Parquet file: "), violations);
    }

    @Test
    public void testEmptyContentIsInvalid() {
        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertAllFlowFilesTransferred(ValidateParquet.REL_INVALID, 1);
    }

    @Test
    public void testContentIsNotModified() throws IOException {
        final byte[] original = fixtureBytes("valid.parquet");

        runner.enqueue(original);
        runner.run();

        runner.getFlowFilesForRelationship(ValidateParquet.REL_VALID).get(0).assertContentEquals(original);
    }

    private void assertViolation(final String fixture, final String expected) throws IOException {
        final MockFlowFile out = runFixture(fixture, ValidateParquet.REL_INVALID);

        assertEquals("1", out.getAttribute(ValidateParquet.INVALID_COUNT_ATTRIBUTE));
        assertEquals(expected, out.getAttribute(ValidateParquet.VIOLATIONS_ATTRIBUTE));
    }

    private MockFlowFile runFixture(final String fixture, final Relationship expected) throws IOException {
        runner.enqueue(fixtureBytes(fixture));
        runner.run();

        runner.assertAllFlowFilesTransferred(expected, 1);
        return runner.getFlowFilesForRelationship(expected).get(0);
    }

    private byte[] fixtureBytes(final String fixture) throws IOException {
        try (InputStream in = getClass().getResourceAsStream("/parquet/" + fixture)) {
            assertNotNull(in, "missing fixture " + fixture);
            final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            StreamUtils.copy(in, buffer);
            return buffer.toByteArray();
        }
    }

}
