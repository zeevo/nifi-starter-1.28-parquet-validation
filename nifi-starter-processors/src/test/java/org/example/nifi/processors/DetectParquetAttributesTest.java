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
import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;

import org.apache.nifi.processor.Relationship;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/*
 * Fixtures live in src/test/resources/parquet, alongside ValidateParquetTest's. All three were
 * written by parquet-avro 1.17.1, so every footer carries the two keys that writer adds for itself,
 * parquet.avro.schema and writer.model.name.
 *
 *   valid.parquet          2 rows in 1 row group, no custom metadata
 *   empty.parquet          a schema and zero rows, and so zero row groups
 *   metadata-rich.parquet  1000 rows in 7 row groups, with four custom keys: source.system=billing,
 *                          ingest.date=2026-08-26, pipeline.version=7, retention.policy=90d
 */
public class DetectParquetAttributesTest {

    private static final String CREATED_BY =
            "parquet-mr version 1.17.1 (build 78a8d3230eb4769db93de5f2f2e18363c04cae81)";

    private TestRunner runner;

    @BeforeEach
    public void init() {
        runner = TestRunners.newTestRunner(DetectParquetAttributes.class);
    }

    @Test
    public void testHasNoProperties() {
        assertTrue(runner.getProcessor().getPropertyDescriptors().isEmpty());
        runner.assertValid();
    }

    @Test
    public void testFileLevelMetadataIsWritten() throws IOException {
        final MockFlowFile out = runFixture("valid.parquet", DetectParquetAttributes.REL_SUCCESS);

        assertEquals(CREATED_BY, out.getAttribute(DetectParquetAttributes.CREATED_BY_ATTRIBUTE));
        assertEquals("2", out.getAttribute(DetectParquetAttributes.RECORD_COUNT_ATTRIBUTE));
        assertEquals("1", out.getAttribute(DetectParquetAttributes.ROW_GROUP_COUNT_ATTRIBUTE));
        assertEquals("message Row {\n"
                + "  optional int64 id;\n"
                + "  optional binary name (STRING);\n"
                + "  optional binary status (STRING);\n"
                + "}\n", out.getAttribute(DetectParquetAttributes.SCHEMA_ATTRIBUTE));
        assertNull(out.getAttribute(DetectParquetAttributes.ERROR_ATTRIBUTE));

        // The writer's own keys are file-level metadata like any other, so they come through too.
        assertEquals(keys("parquet.avro.schema", "writer.model.name"), metadataKeys(out));
        assertEquals("avro", out.getAttribute("parquet.metadata.writer.model.name"));
    }

    @Test
    public void testCustomKeyValueMetadataIsWritten() throws IOException {
        final MockFlowFile out = runFixture("metadata-rich.parquet", DetectParquetAttributes.REL_SUCCESS);

        assertEquals("billing", out.getAttribute("parquet.metadata.source.system"));
        assertEquals("2026-08-26", out.getAttribute("parquet.metadata.ingest.date"));
        assertEquals("7", out.getAttribute("parquet.metadata.pipeline.version"));
        assertEquals("90d", out.getAttribute("parquet.metadata.retention.policy"));

        // Every key in the footer, and nothing else.
        assertEquals(keys("ingest.date", "parquet.avro.schema", "pipeline.version", "retention.policy",
                "source.system", "writer.model.name"), metadataKeys(out));
    }

    /** The counts have to come from every row group, not just the first. */
    @Test
    public void testCountsCoverEveryRowGroup() throws IOException {
        final MockFlowFile out = runFixture("metadata-rich.parquet", DetectParquetAttributes.REL_SUCCESS);

        assertEquals("1000", out.getAttribute(DetectParquetAttributes.RECORD_COUNT_ATTRIBUTE));
        assertEquals("7", out.getAttribute(DetectParquetAttributes.ROW_GROUP_COUNT_ATTRIBUTE));
    }

    @Test
    public void testFileWithNoRecordsStillHasMetadata() throws IOException {
        final MockFlowFile out = runFixture("empty.parquet", DetectParquetAttributes.REL_SUCCESS);

        assertEquals("0", out.getAttribute(DetectParquetAttributes.RECORD_COUNT_ATTRIBUTE));
        assertEquals("0", out.getAttribute(DetectParquetAttributes.ROW_GROUP_COUNT_ATTRIBUTE));
        assertEquals(CREATED_BY, out.getAttribute(DetectParquetAttributes.CREATED_BY_ATTRIBUTE));
        assertNotNull(out.getAttribute(DetectParquetAttributes.SCHEMA_ATTRIBUTE));
    }

    /** Content that is not Parquet will never succeed, so it goes to failure unpenalized. */
    @Test
    public void testNonParquetContentIsFailureWithReason() {
        runner.enqueue("hello".getBytes(StandardCharsets.UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred(DetectParquetAttributes.REL_FAILURE, 1);
        final MockFlowFile out =
                runner.getFlowFilesForRelationship(DetectParquetAttributes.REL_FAILURE).get(0);
        out.assertContentEquals("hello");
        assertFalse(out.isPenalized());

        final String error = out.getAttribute(DetectParquetAttributes.ERROR_ATTRIBUTE);
        assertTrue(error.startsWith("not a readable Parquet file: "), error);
        assertNull(out.getAttribute(DetectParquetAttributes.RECORD_COUNT_ATTRIBUTE));
    }

    @Test
    public void testEmptyContentIsFailureWithReason() {
        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertAllFlowFilesTransferred(DetectParquetAttributes.REL_FAILURE, 1);
        assertNotNull(runner.getFlowFilesForRelationship(DetectParquetAttributes.REL_FAILURE).get(0)
                .getAttribute(DetectParquetAttributes.ERROR_ATTRIBUTE));
    }

    private static Set<String> keys(final String... keys) {
        return new TreeSet<>(Arrays.asList(keys));
    }

    /** The footer keys that became attributes, with the prefix taken back off. */
    private static Set<String> metadataKeys(final MockFlowFile flowFile) {
        final Set<String> keys = new TreeSet<>();
        for (final String attribute : flowFile.getAttributes().keySet()) {
            if (attribute.startsWith(DetectParquetAttributes.KEY_VALUE_ATTRIBUTE_PREFIX)) {
                keys.add(attribute.substring(DetectParquetAttributes.KEY_VALUE_ATTRIBUTE_PREFIX.length()));
            }
        }
        return keys;
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
        try (InputStream in = DetectParquetAttributesTest.class.getResourceAsStream("/parquet/" + fixture)) {
            assertNotNull(in, "missing fixture " + fixture);
            return in.readAllBytes();
        }
    }
}
