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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.io.LocalInputFile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/*
 * Fixtures are the ones described in ValidateParquetTest, plus:
 *
 *   metadata-rich.parquet   1000 rows, SNAPPY, small row groups, a payload column no rule reads,
 *                           four custom key/value entries, every 10th row invalid
 *   foreign-writer.parquet  20 rows written without Avro: Spark style keys, no parquet.avro.schema
 *                           and no writer.model.name, 4 rows invalid
 */
public class FilterParquetTest {

    private static final String AVRO_SCHEMA_KEY = "parquet.avro.schema";

    private static final PlainParquetConfiguration CONF = new PlainParquetConfiguration();

    @TempDir
    Path tempDir;

    private TestRunner runner;

    @BeforeEach
    public void init() {
        runner = TestRunners.newTestRunner(FilterParquet.class);
    }

    @Test
    public void testHasNoProperties() {
        assertTrue(runner.getProcessor().getPropertyDescriptors().isEmpty());
        runner.assertValid();
    }

    @Test
    public void testValidFileKeepsEveryRow() throws IOException {
        final MockFlowFile out = filter("valid.parquet");

        assertCounts(out, 2, 2, 0);
        final List<GenericRecord> rows = readRows(out.toByteArray());
        assertEquals("bob", rows.get(0).get("name").toString());
        assertEquals("carol", rows.get(1).get("name").toString());
    }

    @Test
    public void testOnlyFailingRowsAreRemovedInOrder() throws IOException {
        final MockFlowFile out = filter("invalid-many.parquet");

        assertCounts(out, 20, 5, 15);
        final List<Long> ids = new ArrayList<>();
        for (final GenericRecord row : readRows(out.toByteArray())) {
            ids.add(((Number) row.get("id")).longValue());
        }
        assertEquals(List.of(1L, 2L, 3L, 4L, 5L), ids);
    }

    @Test
    public void testOriginalIsUntouched() throws IOException {
        final byte[] input = fixtureBytes("metadata-rich.parquet");
        filter("metadata-rich.parquet");

        runner.assertTransferCount(FilterParquet.REL_ORIGINAL, 1);
        runner.getFlowFilesForRelationship(FilterParquet.REL_ORIGINAL).get(0).assertContentEquals(input);
    }

    @Test
    public void testEmptyFileStaysEmptyWithItsSchema() throws IOException {
        final MockFlowFile out = filter("empty.parquet");

        assertCounts(out, 0, 0, 0);
        assertEquals(footer(fixtureBytes("empty.parquet")).getSchema(), footer(out.toByteArray()).getSchema());
    }

    @Test
    public void testManyRowGroups() throws IOException {
        assertCounts(filter("multi-row-group.parquet"), 2000, 2000, 0);
    }

    /** Checks the two processors agree row for row on every fixture, including a string typed id. */
    @Test
    public void testRemovesExactlyWhatValidateParquetRejects() throws IOException {
        for (final String fixture : new String[] {
                "valid.parquet", "valid-null-name.parquet", "valid-null-status.parquet",
                "invalid-null-id.parquet", "invalid-zero-id.parquet", "invalid-both-null.parquet",
                "invalid-blank-name.parquet", "invalid-bad-status.parquet",
                "invalid-string-id.parquet", "invalid-many.parquet", "metadata-rich.parquet",
                "foreign-writer.parquet"}) {
            final TestRunner validate = TestRunners.newTestRunner(ValidateParquet.class);
            validate.enqueue(fixtureBytes(fixture));
            validate.run();
            final MockFlowFile validated = validate.getFlowFilesForRelationship(ValidateParquet.REL_VALID).isEmpty()
                    ? validate.getFlowFilesForRelationship(ValidateParquet.REL_INVALID).get(0)
                    : validate.getFlowFilesForRelationship(ValidateParquet.REL_VALID).get(0);

            runner.clearTransferState();
            final MockFlowFile out = filter(fixture);
            assertEquals(validated.getAttribute(ValidateParquet.INVALID_COUNT_ATTRIBUTE),
                    out.getAttribute(FilterParquet.ROWS_REMOVED_ATTRIBUTE), fixture);

            // And what is left must pass validation.
            validate.clearTransferState();
            validate.enqueue(out.toByteArray());
            validate.run();
            validate.assertAllFlowFilesTransferred(ValidateParquet.REL_VALID, 1);
        }
    }

    /** The requirement: same schema, and every key/value entry the writer lets us set. */
    @Test
    public void testSchemaAndKeyValuesAreKeptForAnAvroWrittenFile() throws IOException {
        final Map<String, String> after = assertSchemaAndKeyValuesKept("metadata-rich.parquet", 900);
        assertEquals("billing", after.get("source.system"));
        assertEquals("2026-08-26", after.get("ingest.date"));
        assertEquals("7", after.get("pipeline.version"));
        assertEquals("90d", after.get("retention.policy"));
    }

    /** The Avro writer always writes parquet.avro.schema, so a file without one gains it. */
    @Test
    public void testSchemaAndKeyValuesAreKeptForAForeignWrittenFile() throws IOException {
        assertFalse(footer(fixtureBytes("foreign-writer.parquet")).getKeyValueMetaData()
                .containsKey(AVRO_SCHEMA_KEY));
        assertTrue(footer(filter("foreign-writer.parquet").toByteArray()).getKeyValueMetaData()
                .containsKey(AVRO_SCHEMA_KEY));

        runner.clearTransferState();
        assertSchemaAndKeyValuesKept("foreign-writer.parquet", 16);
    }

    /** The one documented difference: parquet-java always writes its own writer.model.name. */
    @Test
    public void testWriterModelNameIsTheWritersOwn() throws IOException {
        assertEquals("avro", footer(fixtureBytes("metadata-rich.parquet"))
                .getKeyValueMetaData().get(ParquetWriter.OBJECT_MODEL_NAME_PROP));
        assertEquals("avro", footer(filter("metadata-rich.parquet").toByteArray())
                .getKeyValueMetaData().get(ParquetWriter.OBJECT_MODEL_NAME_PROP));
    }

    @Test
    public void testCopyIsSnappyCompressed() throws IOException {
        final byte[] out = filter("multi-row-group.parquet").toByteArray();
        try (ParquetFileReader reader = ParquetFileReader.open(
                new LocalInputFile(write(out)), ParquetReadOptions.builder(CONF).build())) {
            assertFalse(reader.getRowGroups().isEmpty());
            for (final BlockMetaData rowGroup : reader.getRowGroups()) {
                for (final ColumnChunkMetaData column : rowGroup.getColumns()) {
                    assertEquals(CompressionCodecName.SNAPPY, column.getCodec());
                }
            }
        }
    }

    @Test
    public void testFilteringTwiceKeepsKeyValues() throws IOException {
        final byte[] once = filter("metadata-rich.parquet").toByteArray();
        runner.clearTransferState();
        runner.enqueue(once);
        runner.run();
        runner.assertTransferCount(FilterParquet.REL_SUCCESS, 1);
        final MockFlowFile twice = runner.getFlowFilesForRelationship(FilterParquet.REL_SUCCESS).get(0);

        assertCounts(twice, 900, 900, 0);
        assertEquals(footer(once).getKeyValueMetaData(), footer(twice.toByteArray()).getKeyValueMetaData());
        assertEquals(footer(once).getSchema(), footer(twice.toByteArray()).getSchema());
    }

    @Test
    public void testMissingColumnRoutesToFailure() throws IOException {
        runner.enqueue(fixtureBytes("missing-status-column.parquet"));
        runner.run();

        runner.assertAllFlowFilesTransferred(FilterParquet.REL_FAILURE, 1);
    }

    @Test
    public void testNonParquetRoutesToFailure() {
        runner.enqueue("not parquet".getBytes(StandardCharsets.UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred(FilterParquet.REL_FAILURE, 1);
    }

    private Map<String, String> assertSchemaAndKeyValuesKept(final String fixture, final long kept)
            throws IOException {
        final FileMetaData before = footer(fixtureBytes(fixture));
        final MockFlowFile out = filter(fixture);
        final FileMetaData after = footer(out.toByteArray());

        assertEquals(Long.toString(kept), out.getAttribute(FilterParquet.ROWS_KEPT_ATTRIBUTE));
        assertEquals(before.getSchema(), after.getSchema());

        final Map<String, String> expected = new HashMap<>(before.getKeyValueMetaData());
        final Map<String, String> actual = new HashMap<>(after.getKeyValueMetaData());
        expected.remove(ParquetWriter.OBJECT_MODEL_NAME_PROP);
        actual.remove(ParquetWriter.OBJECT_MODEL_NAME_PROP);
        if (!expected.containsKey(AVRO_SCHEMA_KEY)) {
            actual.remove(AVRO_SCHEMA_KEY);
        }
        assertFalse(expected.isEmpty());
        assertEquals(expected, actual);
        return actual;
    }

    private MockFlowFile filter(final String fixture) throws IOException {
        runner.enqueue(fixtureBytes(fixture));
        runner.run();

        runner.assertTransferCount(FilterParquet.REL_SUCCESS, 1);
        runner.assertTransferCount(FilterParquet.REL_ORIGINAL, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(FilterParquet.REL_SUCCESS).get(0);
        out.assertAttributeEquals("mime.type", "application/parquet");
        return out;
    }

    private static void assertCounts(final MockFlowFile out, final long read, final long kept,
            final long removed) {
        out.assertAttributeEquals(FilterParquet.ROWS_READ_ATTRIBUTE, Long.toString(read));
        out.assertAttributeEquals(FilterParquet.ROWS_KEPT_ATTRIBUTE, Long.toString(kept));
        out.assertAttributeEquals(FilterParquet.ROWS_REMOVED_ATTRIBUTE, Long.toString(removed));
    }

    private FileMetaData footer(final byte[] parquet) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(
                new LocalInputFile(write(parquet)), ParquetReadOptions.builder(CONF).build())) {
            return reader.getFileMetaData();
        }
    }

    private List<GenericRecord> readRows(final byte[] parquet) throws IOException {
        final List<GenericRecord> rows = new ArrayList<>();
        try (ParquetReader<GenericRecord> reader = AvroParquetReader
                .<GenericRecord>builder(new LocalInputFile(write(parquet)), CONF)
                .withDataModel(GenericData.get())
                .build()) {
            GenericRecord row;
            while ((row = reader.read()) != null) {
                rows.add(row);
            }
        }
        return rows;
    }

    private Path write(final byte[] parquet) throws IOException {
        return Files.write(Files.createTempFile(tempDir, "out", ".parquet"), parquet);
    }

    private static byte[] fixtureBytes(final String fixture) throws IOException {
        try (InputStream in = FilterParquetTest.class.getResourceAsStream("/parquet/" + fixture)) {
            assertNotNull(in, "missing fixture " + fixture);
            return in.readAllBytes();
        }
    }
}
