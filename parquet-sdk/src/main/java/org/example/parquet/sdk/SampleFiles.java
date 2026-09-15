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
package org.example.parquet.sdk;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

/**
 * The sample files themselves. Each method writes one file and exists to show one thing.
 *
 * <p>Every sample is deliberately small enough to read in full with a tool like {@code parquet-cli}
 * or the summary this project prints, except where the point is that the file has to be big enough
 * to show the behaviour.
 */
public final class SampleFiles {

    /** Every sample, in the order the generator writes them. */
    public static final List<Sample> ALL = Arrays.asList(
            new Sample("01-primitives.parquet",
                    "every Parquet physical type, all columns required",
                    SampleFiles::primitives),
            new Sample("02-nullable.parquet",
                    "optional columns, with nulls actually present",
                    SampleFiles::nullable),
            new Sample("03-nested.parquet",
                    "records nested two deep, one of them optional and absent on a row",
                    SampleFiles::nested),
            new Sample("04-collections.parquet",
                    "an array, a map, and an array of records with a varying number per row",
                    SampleFiles::collections),
            new Sample("05-logical-types.parquet",
                    "date, timestamp, decimal, uuid and enum over primitives",
                    SampleFiles::logicalTypes),
            new Sample("06-uncompressed.parquet",
                    "the same rows with no compression, as a size baseline",
                    file -> events(file, CompressionCodecName.UNCOMPRESSED)),
            new Sample("07-snappy.parquet",
                    "the same rows with SNAPPY: fast, moderate ratio",
                    file -> events(file, CompressionCodecName.SNAPPY)),
            new Sample("08-gzip.parquet",
                    "the same rows with GZIP: slower, better ratio",
                    file -> events(file, CompressionCodecName.GZIP)),
            new Sample("09-zstd.parquet",
                    "the same rows with ZSTD: usually the best of both",
                    file -> events(file, CompressionCodecName.ZSTD)),
            new Sample("10-many-row-groups.parquet",
                    "a small row group target, so the rows split across many groups",
                    SampleFiles::manyRowGroups),
            new Sample("11-dictionary-disabled.parquet",
                    "dictionary encoding turned off, for comparison with 07",
                    SampleFiles::dictionaryDisabled),
            new Sample("12-file-metadata.parquet",
                    "custom key/value metadata stored in the footer",
                    SampleFiles::fileMetadata),
            new Sample("13-empty.parquet",
                    "a valid file with a schema and no rows at all",
                    SampleFiles::empty));

    private SampleFiles() {
    }

    /** One named sample: a filename, a one line explanation, and the code that writes it. */
    public static final class Sample {

        private final String fileName;
        private final String explanation;
        private final Writer writer;

        Sample(final String fileName, final String explanation, final Writer writer) {
            this.fileName = fileName;
            this.explanation = explanation;
            this.writer = writer;
        }

        public String fileName() {
            return fileName;
        }

        public String explanation() {
            return explanation;
        }

        public Path writeTo(final Path directory) throws IOException {
            final Path file = directory.resolve(fileName);
            writer.write(file);
            return file;
        }

        @FunctionalInterface
        interface Writer {
            void write(Path file) throws IOException;
        }
    }

    private static void primitives(final Path file) throws IOException {
        final Schema schema = Schemas.primitives();
        final List<GenericRecord> rows = new ArrayList<>();
        for (int i = 1; i <= 5; i++) {
            final GenericRecord row = new GenericData.Record(schema);
            row.put("a_boolean", i % 2 == 0);
            row.put("an_int", i);
            row.put("a_long", 1_000_000_000_000L + i);
            row.put("a_float", 1.5f * i);
            row.put("a_double", Math.PI * i);
            row.put("a_string", "row-" + i);
            // Avro bytes maps to a Parquet BINARY with no annotation, so it stays raw.
            row.put("some_bytes", ByteBuffer.wrap(("raw-" + i).getBytes(StandardCharsets.UTF_8)));
            rows.add(row);
        }
        SampleWriter.write(file, schema, rows, SampleWriter.options());
    }

    private static void nullable(final Path file) throws IOException {
        final Schema schema = Schemas.nullable();
        final List<GenericRecord> rows = new ArrayList<>();

        rows.add(row(schema, 1L, "bob", "ACTIVE", 9.5d));
        rows.add(row(schema, 2L, null, "PENDING", null));   // name and score absent
        rows.add(row(schema, 3L, "carol", null, 7.25d));    // status absent
        rows.add(row(schema, null, null, null, null));      // every column null

        SampleWriter.write(file, schema, rows, SampleWriter.options());
    }

    private static GenericRecord row(final Schema schema, final Long id, final String name,
            final String status, final Double score) {
        final GenericRecord row = new GenericData.Record(schema);
        row.put("id", id);
        row.put("name", name);
        row.put("status", status);
        row.put("score", score);
        return row;
    }

    private static void nested(final Path file) throws IOException {
        final Schema schema = Schemas.nested();
        // A union's record branch is at index 1, since index 0 is null.
        final Schema addressSchema = schema.getField("address").schema().getTypes().get(1);
        final Schema geoSchema = addressSchema.getField("geo").schema();

        final List<GenericRecord> rows = new ArrayList<>();
        for (int i = 1; i <= 3; i++) {
            final GenericRecord row = new GenericData.Record(schema);
            row.put("id", (long) i);
            row.put("name", "customer-" + i);

            if (i == 3) {
                // No address at all, which is not the same as an address full of nulls.
                row.put("address", null);
            } else {
                final GenericRecord geo = new GenericData.Record(geoSchema);
                geo.put("latitude", 51.45 + i);
                geo.put("longitude", -2.58 - i);

                final GenericRecord address = new GenericData.Record(addressSchema);
                address.put("street", i + " High Street");
                address.put("city", i % 2 == 0 ? "Leeds" : "Bristol");
                address.put("postcode", i == 2 ? null : "AB" + i + " 1CD");
                address.put("geo", geo);

                row.put("address", address);
            }
            rows.add(row);
        }
        SampleWriter.write(file, schema, rows, SampleWriter.options());
    }

    private static void collections(final Path file) throws IOException {
        final Schema schema = Schemas.collections();
        final Schema lineItemSchema = schema.getField("items").schema().getElementType();
        final List<GenericRecord> rows = new ArrayList<>();

        for (int i = 1; i <= 3; i++) {
            final Map<String, Integer> quantities = new LinkedHashMap<>();
            quantities.put("widget", i);
            quantities.put("sprocket", i * 2);

            // A different number of records per row, which is what repetition levels encode.
            final List<GenericRecord> items = new ArrayList<>();
            for (int line = 1; line <= i; line++) {
                final GenericRecord item = new GenericData.Record(lineItemSchema);
                item.put("sku", String.format("SKU-%03d", i * 10 + line));
                item.put("quantity", line * 2);
                item.put("discount", line == 1 ? null : 0.05 * line);
                items.add(item);
            }

            final GenericRecord row = new GenericData.Record(schema);
            row.put("id", (long) i);
            // An empty list is not the same as a null list; this shows the former.
            row.put("tags", i == 3 ? new ArrayList<String>() : Arrays.asList("alpha", "beta-" + i));
            row.put("quantities", quantities);
            row.put("items", items);
            rows.add(row);
        }
        SampleWriter.write(file, schema, rows, SampleWriter.options());
    }

    private static void logicalTypes(final Path file) throws IOException {
        final Schema schema = Schemas.logicalTypes();
        final Schema colourSchema = schema.getField("colour").schema();
        final List<GenericRecord> rows = new ArrayList<>();

        for (int i = 1; i <= 4; i++) {
            final GenericRecord row = new GenericData.Record(schema);
            // A date logical type is an int: days since the epoch, not millis.
            row.put("event_date", (int) LocalDate.of(2026, 8, i).toEpochDay());
            row.put("event_time", 1_780_000_000_000L + i * 3_600_000L);
            // A decimal on bytes is the unscaled value as two's complement big endian.
            row.put("amount", ByteBuffer.wrap(
                    BigDecimal.valueOf(1234.50 + i).setScale(2).unscaledValue().toByteArray()));
            row.put("correlation_id", String.format("0000000%d-0000-4000-8000-000000000000", i));
            row.put("colour", new GenericData.EnumSymbol(colourSchema,
                    colourSchema.getEnumSymbols().get(i % colourSchema.getEnumSymbols().size())));
            rows.add(row);
        }
        // Without parquetUuid the uuid annotation is dropped and the column is a plain STRING.
        SampleWriter.write(file, schema, rows, SampleWriter.options().parquetUuid(true));
    }

    /** The same 20,000 rows every time, so the compression samples differ only by codec. */
    private static void events(final Path file, final CompressionCodecName codec) throws IOException {
        SampleWriter.write(file, Schemas.events(), eventRows(20_000),
                SampleWriter.options().codec(codec));
    }

    private static void manyRowGroups(final Path file) throws IOException {
        SampleWriter.write(file, Schemas.events(), eventRows(20_000),
                SampleWriter.options()
                        .codec(CompressionCodecName.SNAPPY)
                        // 64 KiB against a 128 MiB default, so the rows split many ways.
                        .rowGroupSize(64L * 1024)
                        .pageSize(8 * 1024));
    }

    private static void dictionaryDisabled(final Path file) throws IOException {
        SampleWriter.write(file, Schemas.events(), eventRows(20_000),
                SampleWriter.options()
                        .codec(CompressionCodecName.SNAPPY)
                        .dictionaryEncoding(false));
    }

    private static void fileMetadata(final Path file) throws IOException {
        final Map<String, String> metadata = new LinkedHashMap<>();
        metadata.put("source.system", "billing");
        metadata.put("ingest.date", "2026-08-28");
        metadata.put("pipeline.version", "7");
        metadata.put("retention.policy", "90d");

        SampleWriter.write(file, Schemas.events(), eventRows(100),
                SampleWriter.options()
                        .codec(CompressionCodecName.SNAPPY)
                        .extraMetadata(metadata));
    }

    /**
     * Zero rows but a complete schema and footer. Worth having around: plenty of code assumes at
     * least one record exists, and NiFi 1.28.1's own ParquetReader cannot read this file at all.
     */
    private static void empty(final Path file) throws IOException {
        SampleWriter.write(file, Schemas.events(), new ArrayList<>(), SampleWriter.options());
    }

    /**
     * {@code category} repeats across rows and {@code payload} never does, which is what makes the
     * dictionary and compression samples show a difference worth looking at.
     */
    private static List<GenericRecord> eventRows(final int count) {
        final Schema schema = Schemas.events();
        final String[] categories = {"ORDER", "REFUND", "SHIPMENT", "RETURN"};
        final List<GenericRecord> rows = new ArrayList<>(count);

        for (int i = 1; i <= count; i++) {
            final GenericRecord row = new GenericData.Record(schema);
            row.put("id", (long) i);
            row.put("category", categories[i % categories.length]);
            row.put("payload", "event-" + i + "-" + Integer.toHexString(i * 2_654_435_761L != 0
                    ? (int) (i * 2_654_435_761L) : i));
            row.put("measurement", (i % 1000) / 7.0d);
            rows.add(row);
        }
        return rows;
    }
}
