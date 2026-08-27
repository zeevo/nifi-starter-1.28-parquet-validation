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
        assertTrue(reported.indexOf("parquet.avro.schema") < 0, reported);
        assertTrue(reported.indexOf("writer.model.name") < 0, reported);
    }

    /** A file with no custom metadata must still work, and report nothing inherited. */
    @Test
    public void testFileWithNoCustomMetadataIsFine() throws IOException {
        final MockFlowFile out = filter("valid.parquet");

        assertEquals("", out.getAttribute(FilterParquet.METADATA_KEYS_ATTRIBUTE));
        final Map<String, String> after = metadataOf(out.toByteArray());
        assertEquals("avro", after.get("writer.model.name"));
        assertNotNull(after.get("parquet.avro.schema"));
    }

    @Test
    public void testCompressionCodecIsInherited() throws IOException {
        final ParquetFileMetadata before = read(fixtureBytes("metadata-rich.parquet"));
        final ParquetFileMetadata after = read(filter("metadata-rich.parquet").toByteArray());

        assertEquals(CompressionCodecName.SNAPPY, before.codec());
        assertEquals(before.codec(), after.codec());
    }

    /**
     * Row group sizing is inherited too, so a file written with many small row groups does not come
     * back as one large one. Row groups are what a downstream reader splits and skips on.
     */
    @Test
    public void testRowGroupSizingIsInherited() throws IOException {
        final byte[] input = fixtureBytes("metadata-rich.parquet");
        final long inputRowGroupSize = read(input).rowGroupSize();
        final long outputRowGroupSize = read(filter("metadata-rich.parquet").toByteArray()).rowGroupSize();

        assertTrue(inputRowGroupSize > 0, "fixture should have row groups");
        // 10% of rows are gone, so the copy's row groups should be about the same size, not 128 MB.
        assertTrue(outputRowGroupSize < inputRowGroupSize * 3,
                "row group size ballooned from " + inputRowGroupSize + " to " + outputRowGroupSize);
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
