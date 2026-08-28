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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.LocalInputFile;
import org.example.parquet.sdk.SampleFiles.Sample;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The samples are teaching material, so what matters is that they are readable and that they
 * actually demonstrate what they claim to.
 */
public class SampleFilesTest {

    @TempDir
    Path directory;

    /** Every sample must produce a file parquet-java can read back. */
    @Test
    public void testEverySampleWritesAReadableFile() throws IOException {
        for (final Sample sample : SampleFiles.ALL) {
            final Path file = sample.writeTo(directory);

            assertTrue(Files.size(file) > 0, sample.fileName() + " is empty");
            assertNotNull(footerOf(file).getFileMetaData().getSchema(), sample.fileName());
            // Reading the rows proves the data is decodable, not just the footer.
            rowsOf(file);
        }
    }

    @Test
    public void testFileNamesAreUniqueAndOrdered() {
        final Set<String> seen = new HashSet<>();
        String previous = "";
        for (final Sample sample : SampleFiles.ALL) {
            assertTrue(seen.add(sample.fileName()), "duplicate " + sample.fileName());
            assertTrue(sample.fileName().compareTo(previous) > 0,
                    sample.fileName() + " is out of order");
            assertTrue(sample.explanation().length() > 10, "explain " + sample.fileName());
            previous = sample.fileName();
        }
    }

    /** The nullable sample has to contain actual nulls, or it demonstrates nothing. */
    @Test
    public void testNullableSampleContainsNulls() throws IOException {
        final List<GenericRecord> rows = rowsOf(write("02-nullable.parquet"));

        assertEquals(4, rows.size());
        assertTrue(rows.stream().anyMatch(r -> r.get("name") == null), "expected a null name");
        assertTrue(rows.stream().anyMatch(r -> r.get("id") == null), "expected a null id");
    }

    /** Nested records must survive as a group with dotted leaf paths. */
    @Test
    public void testNestedSampleHasAGroupColumn() throws IOException {
        final String schema = footerOf(write("03-nested.parquet")).getFileMetaData().getSchema().toString();

        assertTrue(schema.contains("group address"), schema);
        assertTrue(schema.contains("postcode"), schema);
    }

    /** Logical types must be annotated, not silently flattened to their physical type. */
    @Test
    public void testLogicalTypesAreAnnotated() throws IOException {
        final String schema = footerOf(write("05-logical-types.parquet"))
                .getFileMetaData().getSchema().toString();

        assertTrue(schema.contains("(DATE)"), schema);
        assertTrue(schema.contains("TIMESTAMP"), schema);
        assertTrue(schema.contains("DECIMAL(9,2)"), schema);
        // Only present because parquet.avro.write-parquet-uuid is enabled for this sample.
        assertTrue(schema.contains("(UUID)"), schema);
    }

    /** The codec samples are only worth having if they really differ. */
    @Test
    public void testCompressionSamplesGetSmaller() throws IOException {
        final long uncompressed = Files.size(write("06-uncompressed.parquet"));
        final long snappy = Files.size(write("07-snappy.parquet"));
        final long gzip = Files.size(write("08-gzip.parquet"));
        final long zstd = Files.size(write("09-zstd.parquet"));

        assertTrue(snappy < uncompressed, snappy + " should beat " + uncompressed);
        assertTrue(gzip < snappy, gzip + " should beat " + snappy);
        assertTrue(zstd < snappy, zstd + " should beat " + snappy);
    }

    /** The row group sample exists to show a small target producing many groups. */
    @Test
    public void testRowGroupSampleHasManyGroups() throws IOException {
        final List<BlockMetaData> many = footerOf(write("10-many-row-groups.parquet")).getBlocks();
        final List<BlockMetaData> one = footerOf(write("07-snappy.parquet")).getBlocks();

        assertEquals(1, one.size(), "the default target should produce a single row group");
        assertTrue(many.size() > 5, "expected many row groups, got " + many.size());
    }

    @Test
    public void testFileMetadataSampleCarriesItsKeys() throws IOException {
        final ParquetMetadata footer = footerOf(write("12-file-metadata.parquet"));

        assertEquals("billing", footer.getFileMetaData().getKeyValueMetaData().get("source.system"));
        assertEquals("90d", footer.getFileMetaData().getKeyValueMetaData().get("retention.policy"));
    }

    @Test
    public void testEmptySampleHasASchemaAndNoRows() throws IOException {
        final Path file = write("13-empty.parquet");

        assertTrue(footerOf(file).getBlocks().isEmpty(), "expected no row groups");
        assertEquals(0, rowsOf(file).size());
        assertNotNull(footerOf(file).getFileMetaData().getSchema());
    }

    /** The output directory resolution is the part a user is most likely to trip over. */
    @Test
    public void testOutputDirectoryPrefersTheArgumentThenTheProperty() {
        assertEquals(Path.of("/tmp/from-arg"),
                GenerateSampleParquetFiles.outputDirectory(new String[] {"/tmp/from-arg"}));

        final String previous = System.getProperty(GenerateSampleParquetFiles.OUTPUT_PROPERTY);
        System.setProperty(GenerateSampleParquetFiles.OUTPUT_PROPERTY, "/tmp/from-property");
        try {
            assertEquals(Path.of("/tmp/from-property"),
                    GenerateSampleParquetFiles.outputDirectory(new String[0]));
        } finally {
            if (previous == null) {
                System.clearProperty(GenerateSampleParquetFiles.OUTPUT_PROPERTY);
            } else {
                System.setProperty(GenerateSampleParquetFiles.OUTPUT_PROPERTY, previous);
            }
        }
    }

    private Path write(final String fileName) throws IOException {
        for (final Sample sample : SampleFiles.ALL) {
            if (sample.fileName().equals(fileName)) {
                return sample.writeTo(directory);
            }
        }
        throw new IllegalArgumentException("no sample named " + fileName);
    }

    private static ParquetMetadata footerOf(final Path file) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(
                new LocalInputFile(file), ParquetReadOptions.builder().build())) {
            return reader.getFooter();
        }
    }

    private static List<GenericRecord> rowsOf(final Path file) throws IOException {
        final List<GenericRecord> rows = new ArrayList<>();
        try (ParquetReader<GenericRecord> reader = AvroParquetReader
                .<GenericRecord>builder(new LocalInputFile(file))
                .withDataModel(GenericData.get())
                .build()) {
            GenericRecord record;
            while ((record = reader.read()) != null) {
                rows.add(record);
            }
        }
        return rows;
    }
}
