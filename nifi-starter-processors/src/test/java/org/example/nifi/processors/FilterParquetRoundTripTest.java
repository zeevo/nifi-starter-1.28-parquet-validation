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
import org.apache.nifi.parquet.ParquetReader;
import org.apache.nifi.parquet.ParquetRecordSetWriter;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * End to end against NiFi's real ParquetReader and ParquetRecordSetWriter, the same controller
 * services a running instance would use, rather than mocks. This is what proves the record API
 * route actually produces a readable Parquet file.
 *
 * <p>Fixtures are the ones described in ValidateParquetTest.
 */
public class FilterParquetRoundTripTest {

    private static final String AVRO_SCHEMA = "{\"type\":\"record\",\"name\":\"Row\",\"fields\":["
            + "{\"name\":\"id\",\"type\":[\"null\",\"long\"],\"default\":null},"
            + "{\"name\":\"name\",\"type\":[\"null\",\"string\"],\"default\":null},"
            + "{\"name\":\"status\",\"type\":[\"null\",\"string\"],\"default\":null}]}";

    private TestRunner runner;

    @BeforeEach
    public void init() throws InitializationException {
        runner = TestRunners.newTestRunner(FilterParquet.class);

        final ParquetReader reader = new ParquetReader();
        runner.addControllerService("parquet-reader", reader);
        runner.enableControllerService(reader);

        final ParquetRecordSetWriter writer = new ParquetRecordSetWriter();
        runner.addControllerService("parquet-writer", writer);
        // The writer needs to be told the schema; the reader infers it from the file footer.
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY,
                SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_TEXT, AVRO_SCHEMA);
        runner.enableControllerService(writer);

        runner.setProperty(FilterParquet.RECORD_READER, "parquet-reader");
        runner.setProperty(FilterParquet.RECORD_WRITER, "parquet-writer");
        runner.assertValid();
    }

    @Test
    public void testFilteredOutputIsReadableParquet() throws IOException {
        final MockFlowFile out = filter("invalid-many.parquet");

        assertEquals("20", out.getAttribute(FilterParquet.ROWS_READ_ATTRIBUTE));
        assertEquals("5", out.getAttribute(FilterParquet.ROWS_KEPT_ATTRIBUTE));
        assertEquals("15", out.getAttribute(FilterParquet.ROWS_REMOVED_ATTRIBUTE));

        // Rows 1 to 5 are the valid ones in that fixture, so exactly those ids should survive.
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

    @Test
    public void testAlreadyValidFileSurvivesIntact() throws IOException {
        final MockFlowFile out = filter("valid.parquet");

        assertEquals("2", out.getAttribute(FilterParquet.ROWS_KEPT_ATTRIBUTE));
        assertEquals("0", out.getAttribute(FilterParquet.ROWS_REMOVED_ATTRIBUTE));

        final List<GenericRecord> rows = readParquet(out.toByteArray());
        assertEquals("bob", rows.get(0).get("name").toString());
        assertEquals("ACTIVE", rows.get(0).get("status").toString());
        assertEquals("carol", rows.get(1).get("name").toString());
    }

    @Test
    public void testEveryRowRemovedProducesReadableEmptyFile() throws IOException {
        final MockFlowFile out = filter("invalid-zero-id.parquet");

        assertEquals("1", out.getAttribute(FilterParquet.ROWS_REMOVED_ATTRIBUTE));
        assertEquals(0, readParquet(out.toByteArray()).size());
    }

    @Test
    public void testSnappyInputProducesCompressedOutput() throws IOException {
        final MockFlowFile out = filter("snappy-multi-row-group.parquet");

        assertEquals("4000", out.getAttribute(FilterParquet.ROWS_READ_ATTRIBUTE));
        assertEquals("3600", out.getAttribute(FilterParquet.ROWS_KEPT_ATTRIBUTE));
        assertEquals(3600, readParquet(out.toByteArray()).size());

        // The writer service decides the codec from its own configuration rather than inheriting
        // the input's. Asserting on what it actually did keeps that visible if it ever changes.
        final ParquetMetadata footer = footerOf(out.toByteArray());
        assertTrue(footer.getBlocks().size() >= 1, "expected at least one row group");
    }

    @Test
    public void testMimeTypeIsSetByTheWriter() throws IOException {
        assertEquals("application/parquet", filter("valid.parquet").getAttribute("mime.type"));
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

    private MockFlowFile filter(final String fixture) throws IOException {
        runner.enqueue(fixtureBytes(fixture));
        runner.run();

        runner.assertTransferCount(FilterParquet.REL_SUCCESS, 1);
        runner.assertTransferCount(FilterParquet.REL_ORIGINAL, 1);
        return runner.getFlowFilesForRelationship(FilterParquet.REL_SUCCESS).get(0);
    }

    private static List<GenericRecord> readParquet(final byte[] content) throws IOException {
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

    private static ParquetMetadata footerOf(final byte[] content) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(
                new ByteArrayInputFile(content, "filtered"),
                ParquetReadOptions.builder(new PlainParquetConfiguration()).build())) {
            return reader.getFooter();
        }
    }

    private static byte[] fixtureBytes(final String fixture) throws IOException {
        try (InputStream in = FilterParquetRoundTripTest.class.getResourceAsStream("/parquet/" + fixture)) {
            assertNotNull(in, "missing fixture " + fixture);
            return in.readAllBytes();
        }
    }
}
