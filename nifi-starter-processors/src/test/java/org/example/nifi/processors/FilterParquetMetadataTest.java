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

import static org.example.nifi.processors.FilterParquetTest.fixtureBytes;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Map;

import org.apache.nifi.parquet.ParquetReader;
import org.apache.nifi.parquet.ParquetRecordSetWriter;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * What a filtered copy inherits from the file it was filtered out of.
 *
 * <p>The fixture carries four custom file-level metadata keys, SNAPPY compression and deliberately
 * small row groups, so each of those can be checked independently.
 */
public class FilterParquetMetadataTest {

    private TestRunner runner;

    @BeforeEach
    public void init() throws InitializationException {
        runner = TestRunners.newTestRunner(FilterParquet.class);
        final ParquetReader reader = new ParquetReader();
        runner.addControllerService("parquet-reader", reader);
        runner.enableControllerService(reader);
        runner.setProperty(FilterParquet.RECORD_READER, "parquet-reader");

        // Inherit the incoming file's schema rather than pinning one, so a fixture with an extra
        // column is written back with that column.
        final ParquetRecordSetWriter writer = new ParquetRecordSetWriter();
        runner.addControllerService("parquet-writer", writer);
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY,
                SchemaAccessUtils.INHERIT_RECORD_SCHEMA);
        runner.enableControllerService(writer);
        runner.setProperty(FilterParquet.RECORD_WRITER, "parquet-writer");
    }

    /** The requirement: every custom key on the input is present on the copy, with the same value. */
    @Test
    public void testAllCustomMetadataIsInherited() throws IOException {
        final Map<String, String> before = metadataOf(fixtureBytes("metadata-rich.parquet"));
        final Map<String, String> after = metadataOf(filter("metadata-rich.parquet").toByteArray());

        assertEquals("billing", after.get("source.system"));
        assertEquals("2026-08-26", after.get("ingest.date"));
        assertEquals("7", after.get("pipeline.version"));
        assertEquals("90d", after.get("retention.policy"));

        // Nothing the input carried may go missing, writer generated keys included.
        for (final Map.Entry<String, String> entry : before.entrySet()) {
            assertNotNull(after.get(entry.getKey()), "lost metadata key " + entry.getKey());
        }
        assertEquals(before.keySet(), after.keySet());
    }

    /**
     * The two keys the writer regenerates are excluded on the way in to avoid the duplicate key
     * error, so they must still come out with the right values rather than merely being present.
     */
    @Test
    public void testWriterGeneratedKeysAreCorrectNotJustPresent() throws IOException {
        final Map<String, String> before = metadataOf(fixtureBytes("metadata-rich.parquet"));
        final Map<String, String> after = metadataOf(filter("metadata-rich.parquet").toByteArray());

        assertEquals("avro", after.get("writer.model.name"));
        // Same schema in, same schema out: the copy is written with the input's own Avro schema.
        assertEquals(before.get("parquet.avro.schema"), after.get("parquet.avro.schema"));
    }

    /** The attribute should name exactly the keys that were copied, not the regenerated ones. */
    @Test
    public void testInheritedKeysAreReportedAsAnAttribute() throws IOException {
        final String reported = filter("metadata-rich.parquet")
                .getAttribute(FilterParquet.METADATA_KEYS_ATTRIBUTE);

        for (final String expected : new String[] {
                "source.system", "ingest.date", "pipeline.version", "retention.policy"}) {
            assertTrue(reported.contains(expected), reported);
        }
        // The copy's footer is the input's footer, so the attribute names every key on it.
        assertEquals(new java.util.TreeSet<>(metadataOf(fixtureBytes("metadata-rich.parquet")).keySet()),
                new java.util.TreeSet<>(java.util.Arrays.asList(reported.split(","))));
    }

    /** A file with no custom metadata must still work, and report nothing inherited. */
    @Test
    public void testFileWithNoCustomMetadataIsFine() throws IOException {
        final MockFlowFile out = filter("valid.parquet");
        assertEquals(metadataOf(fixtureBytes("valid.parquet")), metadataOf(out.toByteArray()));

        assertEquals("parquet.avro.schema,writer.model.name",
                out.getAttribute(FilterParquet.METADATA_KEYS_ATTRIBUTE));
        final Map<String, String> after = metadataOf(out.toByteArray());
        assertEquals("avro", after.get("writer.model.name"));
        assertNotNull(after.get("parquet.avro.schema"));
    }

    /**
     * The cost of keeping the RecordSetWriter: the output's encoding comes from the writer service
     * configuration, not from the incoming file. The metadata travels, the compression does not.
     * Configure the ParquetRecordSetWriter to match if that matters.
     */
    @Test
    public void testEncodingComesFromTheWriterServiceNotTheInput() throws IOException {
        final ParquetFileMetadata before = read(fixtureBytes("metadata-rich.parquet"));
        final ParquetFileMetadata after = read(filter("metadata-rich.parquet").toByteArray());

        assertEquals(CompressionCodecName.SNAPPY, before.codec());
        // Not inherited: this is the writer service default, and asserting it keeps the tradeoff
        // visible rather than letting it be discovered in production.
        assertEquals(CompressionCodecName.UNCOMPRESSED, after.codec());
    }

    /** Same story for row group sizing: the writer service decides, so small groups consolidate. */
    @Test
    public void testRowGroupSizingComesFromTheWriterServiceNotTheInput() throws IOException {
        final long before = read(fixtureBytes("metadata-rich.parquet")).rowGroupSize();
        final long after = read(filter("metadata-rich.parquet").toByteArray()).rowGroupSize();

        assertTrue(before > 0, "fixture should have row groups");
        assertTrue(after > before, "expected the writer service default to produce larger row groups, "
                + "was " + before + " then " + after);
    }

    /**
     * The requirement stated strictly: the copy's file-level metadata must equal the input's,
     * key for key and value for value. Nothing added, nothing dropped, nothing altered.
     */
    @Test
    public void testMetadataIsExactlyEqualForAnAvroWrittenFile() throws IOException {
        final Map<String, String> before = metadataOf(fixtureBytes("metadata-rich.parquet"));
        final Map<String, String> after = metadataOf(filter("metadata-rich.parquet").toByteArray());

        assertEquals(before, after);
    }

    /**
     * Same requirement, on a file no Avro writer produced: it carries Spark style keys and has
     * neither parquet.avro.schema nor writer.model.name.
     */
    @Test
    public void testMetadataIsExactlyEqualForAForeignWrittenFile() throws IOException {
        final Map<String, String> before = metadataOf(fixtureBytes("foreign-writer.parquet"));
        final Map<String, String> after = metadataOf(filter("foreign-writer.parquet").toByteArray());

        assertEquals(before, after);
    }

    /**
     * The whole requirement in one place: Parquet in, invalid rows out, a copy whose file-level
     * metadata equals the input's byte for byte on success, and the untouched input on original.
     */
    @Test
    public void testFullRequirement() throws IOException {
        final byte[] input = fixtureBytes("metadata-rich.parquet");

        runner.enqueue(input);
        runner.run();

        // 3. the filtered copy on success
        runner.assertTransferCount(FilterParquet.REL_SUCCESS, 1);
        final MockFlowFile success = runner.getFlowFilesForRelationship(FilterParquet.REL_SUCCESS).get(0);

        // 4. the original, untouched, on original
        runner.assertTransferCount(FilterParquet.REL_ORIGINAL, 1);
        runner.getFlowFilesForRelationship(FilterParquet.REL_ORIGINAL).get(0).assertContentEquals(input);

        // 2. invalid rows are gone and only those
        assertEquals("1000", success.getAttribute(FilterParquet.ROWS_READ_ATTRIBUTE));
        assertEquals("900", success.getAttribute(FilterParquet.ROWS_KEPT_ATTRIBUTE));
        assertEquals("100", success.getAttribute(FilterParquet.ROWS_REMOVED_ATTRIBUTE));
        assertEquals(900, FilterParquetTest.readParquet(success.toByteArray()).size());

        // 3. identical file-level metadata
        assertEquals(metadataOf(input), metadataOf(success.toByteArray()));
    }

    /**
     * A copy of a file that never declared an Avro schema does not declare one either, so it has to
     * still be readable: parquet-java falls back to converting the Parquet types.
     */
    @Test
    public void testForeignWrittenCopyIsStillReadable() throws IOException {
        final MockFlowFile out = filter("foreign-writer.parquet");

        assertEquals("20", out.getAttribute(FilterParquet.ROWS_READ_ATTRIBUTE));
        assertEquals("16", out.getAttribute(FilterParquet.ROWS_KEPT_ATTRIBUTE));
        assertEquals(16, FilterParquetTest.readParquet(out.toByteArray()).size());
        assertTrue(metadataOf(out.toByteArray()).get("parquet.avro.schema") == null,
                "the copy should not invent a schema key its input never had");
    }

    /** Metadata has to survive two hops, not just one. */
    @Test
    public void testMetadataSurvivesFilteringTwice() throws IOException {
        final byte[] once = filter("metadata-rich.parquet").toByteArray();

        runner.clearTransferState();
        runner.enqueue(once);
        runner.run();
        runner.assertTransferCount(FilterParquet.REL_SUCCESS, 1);
        final byte[] twice = runner.getFlowFilesForRelationship(FilterParquet.REL_SUCCESS).get(0).toByteArray();

        final Map<String, String> after = metadataOf(twice);
        assertEquals("billing", after.get("source.system"));
        assertEquals("90d", after.get("retention.policy"));
        // The second pass has nothing left to remove.
        assertEquals(900, FilterParquetTest.readParquet(twice).size());
    }

    private MockFlowFile filter(final String fixture) throws IOException {
        runner.enqueue(fixtureBytes(fixture));
        runner.run();

        runner.assertTransferCount(FilterParquet.REL_SUCCESS, 1);
        return runner.getFlowFilesForRelationship(FilterParquet.REL_SUCCESS).get(0);
    }

    private static ParquetFileMetadata read(final byte[] content) throws IOException {
        return ParquetFileMetadata.read(
                new ByteArrayInputFile(content, "probe"), new PlainParquetConfiguration());
    }

    private static Map<String, String> metadataOf(final byte[] content) throws IOException {
        return read(content).all();
    }
}
