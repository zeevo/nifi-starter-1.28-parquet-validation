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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import org.apache.nifi.processor.Relationship;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.io.LocalInputFile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/*
 * Fixtures live in src/test/resources/parquet. Every one uses an all-nullable Avro schema with no
 * "avro.java.string" property, so string columns come back as Utf8 and these tests exercise the
 * same path a file from Spark, pyarrow or DuckDB would take.
 *
 *   valid.parquet                  (1,"bob","ACTIVE"), (2,"carol","PENDING")
 *   valid-null-name.parquet        (1,null,"ACTIVE")
 *   valid-null-status.parquet      (1,"bob",null)
 *   empty.parquet                  correct schema, zero rows
 *   invalid-null-id.parquet        (null,"bob","ACTIVE")
 *   invalid-zero-id.parquet        (0,"bob","ACTIVE")
 *   invalid-both-null.parquet      (1,null,null)
 *   invalid-blank-name.parquet     (1,"   ","ACTIVE")
 *   invalid-bad-status.parquet     (1,"bob","BOGUS")
 *   invalid-string-id.parquet      id column typed string: ("abc","bob","ACTIVE")
 *   invalid-many.parquet           20 rows, 1 to 5 pass, 6 to 20 have id 0
 *   missing-status-column.parquet  schema is id and name only
 *   multi-row-group.parquet        2000 valid rows spread over 161 row groups
 *
 * The spark-* fixtures were written by PySpark 3.5.3 with its defaults, coalesced to one file. They
 * declare no Avro schema, and alongside id, name and status they carry the columns that real Spark
 * tables have and Avro chokes on: an INT96 timestamp, a column called "unit price", decimals, a
 * 3-level list, a map and a struct. Unless noted the rows are (1,"bob","ACTIVE"),
 * (2,"carol","PENDING"), (3,null,"INACTIVE"), and Spark marks nullable columns optional and
 * non-nullable ones required.
 *
 *   spark-wide.parquet              snappy, the default
 *   spark-zstd.parquet              zstd
 *   spark-gzip.parquet              gzip
 *   spark-legacy.parquet            spark.sql.parquet.writeLegacyFormat=true: 2-level list, decimal
 *                                   as fixed_len_byte_array
 *   spark-required-columns.parquet  id, name and status are all required
 *   spark-int-id.parquet            id is INT32
 *   spark-decimal-id.parquet        id is decimal(18,0), stored as INT64
 *   spark-string-id.parquet         id is a string
 *   spark-array-id.parquet          id is a 3-level list
 *   spark-struct-id.parquet         id is a struct
 *   spark-invalid.parquet           one passing row, then one row breaking each rule
 *   spark-double-id.parquet         id is a double, no other columns added
 *   spark-boolean-id.parquet        id is a boolean, no other columns added
 *   spark-int-name.parquet          name is INT32, no other columns added
 *   spark-struct-status.parquet     status is a struct, no other columns added
 */
public class ValidateParquetTest {

    private TestRunner runner;

    @BeforeEach
    public void init() {
        runner = TestRunners.newTestRunner(ValidateParquet.class);
    }

    @Test
    public void testHasNoProperties() {
        assertTrue(runner.getProcessor().getPropertyDescriptors().isEmpty());
        runner.assertValid();
    }

    @Test
    public void testValidFile() throws IOException {
        final MockFlowFile out = runFixture("valid.parquet", ValidateParquet.REL_VALID);

        assertEquals("2", out.getAttribute(ValidateParquet.RECORD_COUNT_ATTRIBUTE));
        assertEquals("0", out.getAttribute(ValidateParquet.INVALID_COUNT_ATTRIBUTE));
        assertNull(out.getAttribute(ValidateParquet.VIOLATIONS_ATTRIBUTE));
    }

    /** The stated example: name null but status present still passes. */
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

    /**
     * 161 row groups over 2000 rows, so parquet seeks repeatedly while reading. This is the guard
     * on FlowFileInputFile's rewind-by-reopening logic, which a single row group file barely
     * touches: it reads the footer at the end, then has to get back to the first row group.
     */
    @Test
    public void testFileWithManyRowGroupsIsValid() throws IOException {
        final MockFlowFile out = runFixture("multi-row-group.parquet", ValidateParquet.REL_VALID);

        assertEquals("2000", out.getAttribute(ValidateParquet.RECORD_COUNT_ATTRIBUTE));
        assertEquals("0", out.getAttribute(ValidateParquet.INVALID_COUNT_ATTRIBUTE));
    }

    @Test
    public void testNullIdIsInvalid() throws IOException {
        assertViolation("invalid-null-id.parquet", "row 1: id is null");
    }

    /** A string id column must be reported as a rule violation, not as an unreadable file. */
    @Test
    public void testNonNumericIdIsInvalid() throws IOException {
        assertViolation("invalid-string-id.parquet", "row 1: id is not numeric");
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
        // Rows 1 to 5 pass, so the first reported violation is row 6, not row 1.
        assertTrue(violations.startsWith("row 6: id must be positive"), violations);
        assertTrue(violations.contains("row 15: id must be positive"), violations);
        // The cap is 10 messages, so rows 16 onward survive only in the count.
        assertFalse(violations.contains("row 16:"), violations);
        assertTrue(violations.endsWith("; ... and 5 more"), violations);
    }

    @Test
    public void testMissingRequiredColumnIsInvalid() throws IOException {
        final MockFlowFile out = runFixture("missing-status-column.parquet", ValidateParquet.REL_INVALID);

        assertEquals("schema is missing required field(s): status",
                out.getAttribute(ValidateParquet.VIOLATIONS_ATTRIBUTE));
        // No rows are read once the schema check fails, so no counts are reported.
        assertNull(out.getAttribute(ValidateParquet.RECORD_COUNT_ATTRIBUTE));
        assertNull(out.getAttribute(ValidateParquet.INVALID_COUNT_ATTRIBUTE));
    }

    @Test
    public void testSparkFileWithColumnsAvroCannotRepresentIsValid() throws IOException {
        assertValidSparkFile("spark-wide.parquet");
    }

    @Test
    public void testSparkZstdFileIsValid() throws IOException {
        assertValidSparkFile("spark-zstd.parquet");
    }

    @Test
    public void testSparkGzipFileIsValid() throws IOException {
        assertValidSparkFile("spark-gzip.parquet");
    }

    @Test
    public void testSparkLegacyFormatIsValid() throws IOException {
        assertValidSparkFile("spark-legacy.parquet");
    }

    /** A projection that assumed nullable columns would not match this file. */
    @Test
    public void testSparkRequiredColumnsAreValid() throws IOException {
        assertValidSparkFile("spark-required-columns.parquet");
    }

    @Test
    public void testSparkInt32IdIsValid() throws IOException {
        assertValidSparkFile("spark-int-id.parquet");
    }

    @Test
    public void testSparkDecimalIdIsValid() throws IOException {
        assertValidSparkFile("spark-decimal-id.parquet");
    }

    @Test
    public void testSparkStringIdIsInvalid() throws IOException {
        assertEveryRowHasNonNumericId("spark-string-id.parquet");
    }

    /** A list has to survive projection with its 3-level structure intact, or the read fails. */
    @Test
    public void testSparkArrayIdIsInvalid() throws IOException {
        assertEveryRowHasNonNumericId("spark-array-id.parquet");
    }

    @Test
    public void testSparkStructIdIsInvalid() throws IOException {
        assertEveryRowHasNonNumericId("spark-struct-id.parquet");
    }

    @Test
    public void testSparkDoubleIdIsValid() throws IOException {
        assertValidSparkFile("spark-double-id.parquet");
    }

    @Test
    public void testSparkBooleanIdIsInvalid() throws IOException {
        assertEveryRowHasNonNumericId("spark-boolean-id.parquet");
    }

    /** A numeric name still counts as present, and as not blank. */
    @Test
    public void testSparkInt32NameIsValid() throws IOException {
        assertValidSparkFile("spark-int-name.parquet");
    }

    @Test
    public void testSparkStructStatusIsNotAllowed() throws IOException {
        final MockFlowFile out = runFixture("spark-struct-status.parquet", ValidateParquet.REL_INVALID);

        assertEquals("3", out.getAttribute(ValidateParquet.INVALID_COUNT_ATTRIBUTE));
        assertTrue(out.getAttribute(ValidateParquet.VIOLATIONS_ATTRIBUTE)
                .startsWith("row 1: status '<group>' is not an allowed value"));
    }

    @Test
    public void testSparkFileReportsEveryRule() throws IOException {
        final MockFlowFile out = runFixture("spark-invalid.parquet", ValidateParquet.REL_INVALID);

        assertEquals("6", out.getAttribute(ValidateParquet.RECORD_COUNT_ATTRIBUTE));
        assertEquals("5", out.getAttribute(ValidateParquet.INVALID_COUNT_ATTRIBUTE));
        assertEquals("row 2: id must be positive; row 3: name is blank; "
                        + "row 4: status 'BOGUS' is not an allowed value; row 5: id is null; "
                        + "row 6: name and status are both null",
                out.getAttribute(ValidateParquet.VIOLATIONS_ATTRIBUTE));
    }

    /**
     * Overwrites every column chunk outside Item.FIELDS with garbage. The file can only still pass
     * if those chunks are never decoded, which is the whole point of projecting.
     */
    @Test
    public void testUnrelatedColumnsAreNotRead(@TempDir final Path dir) throws IOException {
        final byte[] content = withUnrelatedColumnsCorrupted(fixtureBytes("spark-wide.parquet"), dir);

        runner.enqueue(content);
        runner.run();

        runner.assertAllFlowFilesTransferred(ValidateParquet.REL_VALID, 1);
        assertEquals("3", runner.getFlowFilesForRelationship(ValidateParquet.REL_VALID).get(0)
                .getAttribute(ValidateParquet.RECORD_COUNT_ATTRIBUTE));
    }

    @Test
    public void testNonParquetContentIsInvalidNotFailure() {
        runner.enqueue("hello".getBytes(StandardCharsets.UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred(ValidateParquet.REL_INVALID, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ValidateParquet.REL_INVALID).get(0);
        out.assertContentEquals("hello");

        final String violations = out.getAttribute(ValidateParquet.VIOLATIONS_ATTRIBUTE);
        assertTrue(violations.startsWith("not a readable Parquet file: "), violations);
    }

    @Test
    public void testEmptyContentIsInvalidNotFailure() {
        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertAllFlowFilesTransferred(ValidateParquet.REL_INVALID, 1);
        assertNotNull(runner.getFlowFilesForRelationship(ValidateParquet.REL_INVALID).get(0)
                .getAttribute(ValidateParquet.VIOLATIONS_ATTRIBUTE));
    }

    private void assertViolation(final String fixture, final String expected) throws IOException {
        final MockFlowFile out = runFixture(fixture, ValidateParquet.REL_INVALID);

        assertEquals("1", out.getAttribute(ValidateParquet.INVALID_COUNT_ATTRIBUTE));
        assertEquals(expected, out.getAttribute(ValidateParquet.VIOLATIONS_ATTRIBUTE));
    }

    private void assertValidSparkFile(final String fixture) throws IOException {
        final MockFlowFile out = runFixture(fixture, ValidateParquet.REL_VALID);

        assertEquals("3", out.getAttribute(ValidateParquet.RECORD_COUNT_ATTRIBUTE));
        assertEquals("0", out.getAttribute(ValidateParquet.INVALID_COUNT_ATTRIBUTE));
    }

    private void assertEveryRowHasNonNumericId(final String fixture) throws IOException {
        final MockFlowFile out = runFixture(fixture, ValidateParquet.REL_INVALID);

        assertEquals("3", out.getAttribute(ValidateParquet.INVALID_COUNT_ATTRIBUTE));
        assertEquals("row 1: id is not numeric; row 2: id is not numeric; row 3: id is not numeric",
                out.getAttribute(ValidateParquet.VIOLATIONS_ATTRIBUTE));
    }

    private static byte[] withUnrelatedColumnsCorrupted(final byte[] content, final Path dir) throws IOException {
        final Path file = Files.write(dir.resolve("input.parquet"), content);
        final byte[] corrupted = content.clone();
        try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(file))) {
            for (final BlockMetaData block : reader.getFooter().getBlocks()) {
                for (final ColumnChunkMetaData chunk : block.getColumns()) {
                    if (!Item.FIELDS.contains(chunk.getPath().toArray()[0])) {
                        final int start = Math.toIntExact(chunk.getStartingPos());
                        Arrays.fill(corrupted, start, start + Math.toIntExact(chunk.getTotalSize()), (byte) 0xFF);
                    }
                }
            }
        }
        return corrupted;
    }

    /**
     * Enqueues a fixture, runs, asserts the routing, and asserts the content came back untouched.
     * Every fixture-driven test therefore covers the "content is never modified" guarantee.
     */
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
        try (InputStream in = ValidateParquetTest.class.getResourceAsStream("/parquet/" + fixture)) {
            assertNotNull(in, "missing fixture " + fixture);
            return in.readAllBytes();
        }
    }
}
