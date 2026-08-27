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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.jupiter.api.Test;

/**
 * Why file-level metadata cannot travel through NiFi's record API, pinned down rather than
 * asserted in a comment. Every claim FilterParquet's design rests on is checked here, so that if a
 * future NiFi version changes any of it these tests fail and the design can be revisited.
 *
 * <p>Verified against NiFi 1.28.1 and parquet-java 1.17.1.
 */
public class RecordApiMetadataLimitsTest {

    private static final Schema SCHEMA = new Schema.Parser().parse(
            "{\"type\":\"record\",\"name\":\"Row\",\"fields\":["
            + "{\"name\":\"id\",\"type\":[\"null\",\"long\"],\"default\":null},"
            + "{\"name\":\"name\",\"type\":[\"null\",\"string\"],\"default\":null},"
            + "{\"name\":\"status\",\"type\":[\"null\",\"string\"],\"default\":null}]}");

    /**
     * RecordReader is record oriented: a schema and a cursor over rows. There is no hook for
     * anything file-level, which is why the footer has to be read separately.
     */
    @Test
    public void testRecordReaderExposesNoFileMetadata() {
        for (final Method method : RecordReader.class.getMethods()) {
            final String name = method.getName().toLowerCase();
            assertFalse(name.contains("metadata") || name.contains("keyvalue") || name.contains("footer"),
                    "RecordReader unexpectedly exposes " + method.getName()
                            + "; the separate footer read in FilterParquet may no longer be needed");
        }
    }

    /**
     * Neither does the writer side. createWriter takes a logger, a schema, a stream and a variables
     * map, and none of those reach the Parquet footer.
     */
    @Test
    public void testRecordSetWriterExposesNoFileMetadata() {
        for (final Method method : RecordSetWriter.class.getMethods()) {
            final String name = method.getName().toLowerCase();
            assertFalse(name.contains("metadata") || name.contains("keyvalue") || name.contains("footer"),
                    "RecordSetWriter unexpectedly exposes " + method.getName());
        }
        for (final Method method : RecordSetWriterFactory.class.getMethods()) {
            final String name = method.getName().toLowerCase();
            assertFalse(name.contains("metadata") || name.contains("keyvalue"),
                    "RecordSetWriterFactory unexpectedly exposes " + method.getName());
        }
    }

    /** parquet-java's own writer does support it, which is why the write side left the abstraction. */
    @Test
    public void testParquetWriterBuilderSupportsExtraMetadata() throws NoSuchMethodException {
        final Method withExtraMetaData =
                ParquetWriter.Builder.class.getMethod("withExtraMetaData", Map.class);
        assertNotNull(withExtraMetaData);
    }

    /**
     * The trap. Copying every key across, which is the obvious reading of "inherit all file-level
     * metadata", throws: the Avro write support writes parquet.avro.schema itself and parquet-java
     * refuses a duplicate. Any implementation has to exclude the writer generated keys.
     */
    @Test
    public void testInheritingWriterGeneratedKeysIsFatal() throws IOException {
        final Map<String, String> everything = new LinkedHashMap<>();
        everything.put("source.system", "billing");
        everything.put("parquet.avro.schema", SCHEMA.toString());

        final IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> writeWith(everything));
        assertTrue(thrown.getMessage().contains("Duplicate metadata key"), thrown.getMessage());
        assertTrue(thrown.getMessage().contains("parquet.avro.schema"), thrown.getMessage());
    }

    /** The two keys the Avro write support claims, which ParquetFileMetadata therefore excludes. */
    @Test
    public void testWriterGeneratedKeysAreExactlyTheOnesWeExclude() throws IOException {
        final byte[] plain = writeWith(Collections_emptyMap());
        final Map<String, String> written = metadataOf(plain);

        assertEquals(new java.util.TreeSet<>(Arrays.asList("parquet.avro.schema", "writer.model.name")),
                new java.util.TreeSet<>(written.keySet()),
                "the set of keys the writer generates on its own has changed");
        for (final String key : written.keySet()) {
            assertTrue(ParquetFileMetadata.isWriterGenerated(key),
                    key + " is writer generated but ParquetFileMetadata does not exclude it");
        }
    }

    /** Excluding those two works, and the writer puts them back, so nothing is actually lost. */
    @Test
    public void testExcludingThemPreservesEverythingAnyway() throws IOException {
        final Map<String, String> custom = new LinkedHashMap<>();
        custom.put("source.system", "billing");
        custom.put("ingest.date", "2026-08-26");

        final Map<String, String> written = metadataOf(writeWith(custom));

        assertEquals("billing", written.get("source.system"));
        assertEquals("2026-08-26", written.get("ingest.date"));
        // Regenerated rather than inherited, but present all the same.
        assertNotNull(written.get("parquet.avro.schema"));
        assertEquals("avro", written.get("writer.model.name"));
    }

    private static Map<String, String> Collections_emptyMap() {
        return new LinkedHashMap<>();
    }

    private static byte[] writeWith(final Map<String, String> extraMetadata) throws IOException {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter
                .<GenericRecord>builder(new FlowFileOutputFile(bytes))
                .withSchema(SCHEMA)
                .withDataModel(GenericData.get())
                .withConf(new PlainParquetConfiguration())
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withExtraMetaData(extraMetadata)
                .build()) {
            final GenericRecord row = new GenericData.Record(SCHEMA);
            row.put("id", 1L);
            row.put("name", "bob");
            row.put("status", "ACTIVE");
            writer.write(row);
        }
        return bytes.toByteArray();
    }

    private static Map<String, String> metadataOf(final byte[] content) throws IOException {
        return ParquetFileMetadata
                .read(new ByteArrayInputFile(content, "probe"), new PlainParquetConfiguration())
                .all();
    }
}
