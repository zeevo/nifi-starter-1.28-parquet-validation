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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeSet;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.jupiter.api.Test;

/**
 * Why {@link ExactMetadataParquetWriter} exists, pinned down rather than asserted in a comment.
 *
 * <p>Verified against parquet-java 1.17.1.
 */
public class ExactMetadataWriterTest {

    private static final Schema SCHEMA = new Schema.Parser().parse(
            "{\"type\":\"record\",\"name\":\"Row\",\"fields\":["
            + "{\"name\":\"id\",\"type\":[\"null\",\"long\"],\"default\":null},"
            + "{\"name\":\"name\",\"type\":[\"null\",\"string\"],\"default\":null},"
            + "{\"name\":\"status\",\"type\":[\"null\",\"string\"],\"default\":null}]}");

    /**
     * The trap. Handing the stock builder every key the input carried, which is the obvious reading
     * of "inherit all file-level metadata", throws: the Avro write support emits
     * parquet.avro.schema itself and parquet-java refuses a duplicate.
     */
    @Test
    public void testStockBuilderRejectsTheWriterGeneratedKeys() {
        final Map<String, String> everything = new LinkedHashMap<>();
        everything.put("source.system", "billing");
        everything.put("parquet.avro.schema", SCHEMA.toString());

        final IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> writeWithStockBuilder(everything));
        assertTrue(thrown.getMessage().contains("Duplicate metadata key"), thrown.getMessage());
        assertTrue(thrown.getMessage().contains("parquet.avro.schema"), thrown.getMessage());
    }

    /** And it always appends its own two keys, so a copy of a foreign file would gain them. */
    @Test
    public void testStockBuilderAlwaysAddsItsOwnKeys() throws IOException {
        final Map<String, String> sparkish = new LinkedHashMap<>();
        sparkish.put("org.apache.spark.version", "3.5.1");

        final Map<String, String> written = metadataOf(writeWithStockBuilder(sparkish));

        assertEquals(new TreeSet<>(Arrays.asList(
                        "org.apache.spark.version", "parquet.avro.schema", "writer.model.name")),
                new TreeSet<>(written.keySet()),
                "the stock builder's added keys have changed");
    }

    /**
     * The way out. Footer metadata is whatever WriteSupport.init returns in its WriteContext, plus
     * writer.model.name from WriteSupport.getName(). Override both and the footer holds exactly
     * what was asked for, which is what makes copying a foreign file faithfully possible.
     */
    @Test
    public void testExactWriterEmitsPreciselyWhatItIsGiven() throws IOException {
        final Map<String, String> exact = new LinkedHashMap<>();
        exact.put("org.apache.spark.version", "3.5.1");
        exact.put("source.system", "billing");

        assertEquals(exact, metadataOf(writeExactly(exact)));
    }

    /** Including the reserved keys, which the stock builder cannot accept at all. */
    @Test
    public void testExactWriterAcceptsTheReservedKeys() throws IOException {
        final Map<String, String> withReserved = new LinkedHashMap<>();
        withReserved.put("parquet.avro.schema", SCHEMA.toString());
        withReserved.put("writer.model.name", "avro");
        withReserved.put("source.system", "billing");

        assertEquals(withReserved, metadataOf(writeExactly(withReserved)));
    }

    /** An empty map means an empty footer, not a defaulted one. */
    @Test
    public void testExactWriterCanEmitNoMetadataAtAll() throws IOException {
        assertTrue(metadataOf(writeExactly(new LinkedHashMap<>())).isEmpty());
    }

    private static byte[] writeWithStockBuilder(final Map<String, String> extra) throws IOException {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter
                .<GenericRecord>builder(new FlowFileOutputFile(bytes))
                .withSchema(SCHEMA)
                .withDataModel(GenericData.get())
                .withConf(new PlainParquetConfiguration())
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withExtraMetaData(extra)
                .build()) {
            writer.write(row());
        }
        return bytes.toByteArray();
    }

    private static byte[] writeExactly(final Map<String, String> metadata) throws IOException {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ParquetWriter<GenericRecord> writer =
                new ExactMetadataParquetWriter(new FlowFileOutputFile(bytes), SCHEMA, metadata)
                        .withConf(new PlainParquetConfiguration())
                        .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                        .build()) {
            writer.write(row());
        }
        return bytes.toByteArray();
    }

    private static GenericRecord row() {
        final GenericRecord row = new GenericData.Record(SCHEMA);
        row.put("id", 1L);
        row.put("name", "bob");
        row.put("status", "ACTIVE");
        return row;
    }

    private static Map<String, String> metadataOf(final byte[] content) throws IOException {
        return ParquetFileMetadata
                .read(new ByteArrayInputFile(content, "probe"), new PlainParquetConfiguration())
                .all();
    }
}
