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

import java.io.IOException;
import java.io.InputStream;

import org.apache.nifi.processor.Relationship;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/*
 * The optional key column, which may hold text or 16 raw bytes, singly or as an array, or nothing.
 *
 * Each fixture is one row, (1,"bob","ACTIVE"), so the key is the only thing under test. The Java
 * type in each comment is what parquet-avro actually hands back, which is the whole reason this
 * needs more than one fixture: the column's Parquet type does not decide it on its own.
 *
 *   valid-key-text.parquet           optional binary (STRING)                 -> Utf8
 *   valid-key-uuid.parquet           optional fixed_len_byte_array(16) (UUID) -> String, rendered
 *                                    by parquet-avro from those 16 bytes before this code sees it
 *   valid-key-fixed.parquet          optional fixed_len_byte_array(16)        -> GenericData.Fixed
 *   valid-key-bytes.parquet          optional binary                          -> ByteBuffer
 *   valid-key-null.parquet           optional fixed_len_byte_array(16), null  -> null
 *   valid-key-array.parquet          LIST of fixed_len_byte_array(16), 2 of them
 *   valid-key-array-text.parquet     LIST of binary (STRING), 2 of them
 *   invalid-key-short.parquet        optional binary holding 8 bytes
 *   invalid-key-blank.parquet        optional binary (STRING) holding "   "
 *   invalid-key-numeric.parquet      optional int64, so neither text nor binary
 *   invalid-key-array-short.parquet  LIST of binary: 16 bytes then 8
 *   invalid-key-array-null.parquet   LIST of optional binary: 16 bytes then null. Needs parquet's
 *                                    three level list encoding, since the two level one it writes
 *                                    by default cannot represent a null element at all.
 */
public class ValidateParquetKeyTest {

    private TestRunner runner;

    @BeforeEach
    public void init() {
        runner = TestRunners.newTestRunner(ValidateParquet.class);
    }

    /** The column is optional, so every file that predates it has to validate exactly as before. */
    @Test
    public void testFileWithNoKeyColumnIsUnaffected() throws IOException {
        final MockFlowFile out = runFixture("valid.parquet", ValidateParquet.REL_VALID);

        assertEquals("2", out.getAttribute(ValidateParquet.RECORD_COUNT_ATTRIBUTE));
        assertNull(out.getAttribute(ValidateParquet.VIOLATIONS_ATTRIBUTE));
    }

    @Test
    public void testTextKeyIsValid() throws IOException {
        runFixture("valid-key-text.parquet", ValidateParquet.REL_VALID);
    }

    /**
     * The same 16 bytes as the fixed fixture, written with Parquet's UUID annotation. parquet-avro
     * renders them as canonical text, so this file exercises the text half of the rules even though
     * what is on disk is binary.
     */
    @Test
    public void testUuidAnnotatedKeyArrivesAsTextAndIsValid() throws IOException {
        runFixture("valid-key-uuid.parquet", ValidateParquet.REL_VALID);
    }

    @Test
    public void testFixedBinaryKeyIsValid() throws IOException {
        runFixture("valid-key-fixed.parquet", ValidateParquet.REL_VALID);
    }

    @Test
    public void testVariableWidthBinaryKeyOfSixteenBytesIsValid() throws IOException {
        runFixture("valid-key-bytes.parquet", ValidateParquet.REL_VALID);
    }

    /** Nullable is part of the requirement: a null key is nothing to check, not a violation. */
    @Test
    public void testNullKeyIsValid() throws IOException {
        runFixture("valid-key-null.parquet", ValidateParquet.REL_VALID);
    }

    @Test
    public void testArrayOfBinaryKeysIsValid() throws IOException {
        runFixture("valid-key-array.parquet", ValidateParquet.REL_VALID);
    }

    /** An array is not required to hold binary. Each element is judged on the form it arrived in. */
    @Test
    public void testArrayOfTextKeysIsValid() throws IOException {
        runFixture("valid-key-array-text.parquet", ValidateParquet.REL_VALID);
    }

    @Test
    public void testBinaryKeyOfTheWrongWidthIsInvalid() throws IOException {
        assertViolation("invalid-key-short.parquet", "row 1: key is 8 bytes, not 16");
    }

    @Test
    public void testBlankTextKeyIsInvalid() throws IOException {
        assertViolation("invalid-key-blank.parquet", "row 1: key is blank");
    }

    /** A numeric key column is a schema problem, and saying so beats calling it a missing value. */
    @Test
    public void testKeyThatIsNeitherTextNorBinaryIsInvalid() throws IOException {
        assertViolation("invalid-key-numeric.parquet", "row 1: key is neither text nor binary");
    }

    @Test
    public void testArrayReportsTheOffendingElement() throws IOException {
        assertViolation("invalid-key-array-short.parquet", "row 1: key[1] is 8 bytes, not 16");
    }

    @Test
    public void testNullInsideAnArrayIsInvalid() throws IOException {
        assertViolation("invalid-key-array-null.parquet", "row 1: key[1] is neither text nor binary");
    }

    private void assertViolation(final String fixture, final String expected) throws IOException {
        final MockFlowFile out = runFixture(fixture, ValidateParquet.REL_INVALID);

        assertEquals("1", out.getAttribute(ValidateParquet.INVALID_COUNT_ATTRIBUTE));
        assertEquals(expected, out.getAttribute(ValidateParquet.VIOLATIONS_ATTRIBUTE));
    }

    private MockFlowFile runFixture(final String fixture, final Relationship expected) throws IOException {
        final byte[] content = fixtureBytes(fixture);
        runner.enqueue(content);
        runner.run();

        runner.assertAllFlowFilesTransferred(expected, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(expected).get(0);
        out.assertContentEquals(content);
        return out;
    }

    private static byte[] fixtureBytes(final String fixture) throws IOException {
        try (InputStream in = ValidateParquetKeyTest.class.getResourceAsStream("/parquet/" + fixture)) {
            assertNotNull(in, "missing fixture " + fixture);
            return in.readAllBytes();
        }
    }
}
