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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * The pushdown predicate has to mean the same thing as {@link Item}'s rules, and it has to actually
 * let parquet skip work. Both are checked here.
 */
public class PredicatePushdownTest {

    private static final Schema SCHEMA = new Schema.Parser().parse(
            "{\"type\":\"record\",\"name\":\"Row\",\"fields\":["
            + "{\"name\":\"id\",\"type\":[\"null\",\"long\"],\"default\":null},"
            + "{\"name\":\"name\",\"type\":[\"null\",\"string\"],\"default\":null},"
            + "{\"name\":\"status\",\"type\":[\"null\",\"string\"],\"default\":null}]}");

    /**
     * The drift hazard: two statements of the same rules, kept in step by hand. Every fixture is
     * read both ways and the surviving ids compared, so any divergence fails the build.
     */
    @ParameterizedTest
    @ValueSource(strings = {
            "valid.parquet", "valid-null-name.parquet", "valid-null-status.parquet",
            "invalid-null-id.parquet", "invalid-zero-id.parquet", "invalid-both-null.parquet",
            "invalid-blank-name.parquet", "invalid-bad-status.parquet", "invalid-many.parquet",
            "metadata-rich.parquet", "snappy-multi-row-group.parquet"})
    public void testPredicateAgreesWithTheJavaRules(final String fixture) throws IOException {
        final byte[] content = FilterParquetTest.fixtureBytes(fixture);

        final List<Object> byRules = new ArrayList<>();
        for (final GenericRecord record : read(content, false)) {
            if (Item.from(record).isValid()) {
                byRules.add(record.get("id"));
            }
        }

        final List<Object> byPredicate = new ArrayList<>();
        for (final GenericRecord record : read(content, true)) {
            byPredicate.add(record.get("id"));
        }

        assertEquals(byRules, byPredicate, "predicate and rules disagree on " + fixture);
    }

    /**
     * The payoff. A file whose bad rows are clustered has row groups that cannot contain a passing
     * row, and the statistics filter drops those without decoding them.
     */
    @Test
    public void testWholeRowGroupsAreSkippedOnStatistics() throws IOException {
        // First half all id=0, second half all valid, with small row groups so the halves land in
        // different ones.
        final byte[] content = generate(20_000, i -> i <= 10_000);

        final long total;
        final long afterFiltering;
        final PlainParquetConfiguration conf = new PlainParquetConfiguration();
        try (ParquetFileReader plain = ParquetFileReader.open(
                new ByteArrayInputFile(content, "x"), ParquetReadOptions.builder(conf).build())) {
            total = plain.getRecordCount();
        }
        try (ParquetFileReader filtered = ParquetFileReader.open(
                new ByteArrayInputFile(content, "x"),
                ParquetReadOptions.builder(conf)
                        .withRecordFilter(FilterCompat.get(ItemPredicate.keepValidRows()))
                        .useStatsFilter(true)
                        .build())) {
            afterFiltering = filtered.getFilteredRecordCount();
        }

        assertEquals(20_000, total);
        // Statistics alone cannot know exactly which rows pass, but it can rule out whole groups,
        // so the candidate count must be well below the total.
        assertTrue(afterFiltering < total,
                "statistics filtering skipped nothing: " + afterFiltering + " of " + total);
        assertTrue(afterFiltering <= 11_000,
                "expected roughly the clean half to survive statistics filtering, got " + afterFiltering);
    }

    /** Where the rules are not deducible from statistics, nothing may be skipped, and that is correct. */
    @Test
    public void testNothingIsSkippedWhenStatisticsCannotTell() throws IOException {
        // Every row group holds a mixture, so no group can be ruled out.
        final byte[] content = generate(20_000, i -> i % 10 == 0);

        final PlainParquetConfiguration conf = new PlainParquetConfiguration();
        try (ParquetFileReader filtered = ParquetFileReader.open(
                new ByteArrayInputFile(content, "x"),
                ParquetReadOptions.builder(conf)
                        .withRecordFilter(FilterCompat.get(ItemPredicate.keepValidRows()))
                        .useStatsFilter(true)
                        .build())) {
            assertEquals(20_000, filtered.getFilteredRecordCount());
        }
    }

    private static List<GenericRecord> read(final byte[] content, final boolean pushDown) throws IOException {
        final ParquetReader.Builder<GenericRecord> builder = AvroParquetReader
                .<GenericRecord>builder(new ByteArrayInputFile(content, "x"), new PlainParquetConfiguration())
                .withDataModel(GenericData.get());
        if (pushDown) {
            builder.withFilter(FilterCompat.get(ItemPredicate.keepValidRows()))
                    .useStatsFilter(true)
                    .useRecordFilter(true)
                    .useColumnIndexFilter(true);
        }
        final List<GenericRecord> rows = new ArrayList<>();
        try (ParquetReader<GenericRecord> reader = builder.build()) {
            GenericRecord record;
            while ((record = reader.read()) != null) {
                rows.add(record);
            }
        }
        return rows;
    }

    private static byte[] generate(final int rows, final java.util.function.LongPredicate bad) throws IOException {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter
                .<GenericRecord>builder(new FlowFileOutputFile(bytes))
                .withSchema(SCHEMA)
                .withDataModel(GenericData.get())
                .withConf(new PlainParquetConfiguration())
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withRowGroupSize(32L * 1024)
                .build()) {
            for (long i = 1; i <= rows; i++) {
                final GenericRecord row = new GenericData.Record(SCHEMA);
                row.put("id", bad.test(i) ? 0L : i);
                row.put("name", "user-" + i + "-padding-padding-padding");
                row.put("status", i % 3 == 0 ? "ACTIVE" : i % 3 == 1 ? "INACTIVE" : "PENDING");
                writer.write(row);
            }
        }
        return bytes.toByteArray();
    }
}
