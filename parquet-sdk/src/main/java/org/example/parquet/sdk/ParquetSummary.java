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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.LocalInputFile;

/**
 * Prints what a Parquet file actually turned out to be.
 *
 * <p>This is where most of the learning is. A writer takes a schema and some options, but what
 * lands on disk is the result of decisions parquet made along the way: which encoding each column
 * got, whether a dictionary was used, how the rows were divided into row groups, what ended up in
 * the footer. Reading that back is the only way to see them.
 *
 * <p>Everything here comes from the footer, so none of it decompresses a single page.
 */
public final class ParquetSummary {

    private ParquetSummary() {
    }

    /** Reads {@code file}'s footer and prints a human readable description of it. */
    public static void print(final Path file) throws IOException {
        final ParquetMetadata footer;
        try (ParquetFileReader reader = ParquetFileReader.open(
                new LocalInputFile(file), ParquetReadOptions.builder().build())) {
            footer = reader.getFooter();
        }

        long rows = 0;
        long compressed = 0;
        long uncompressed = 0;
        for (final BlockMetaData block : footer.getBlocks()) {
            rows += block.getRowCount();
            compressed += block.getCompressedSize();
            uncompressed += block.getTotalByteSize();
        }

        System.out.printf("  file          %s (%,d bytes on disk)%n", file.getFileName(), Files.size(file));
        System.out.printf("  rows          %,d in %d row group(s)%n", rows, footer.getBlocks().size());
        if (uncompressed > 0) {
            System.out.printf("  column data   %,d bytes compressed from %,d (%.0f%%)%n",
                    compressed, uncompressed, 100.0 * compressed / uncompressed);
        }
        System.out.printf("  created by    %s%n", footer.getFileMetaData().getCreatedBy());

        System.out.println("  schema");
        for (final String line : footer.getFileMetaData().getSchema().toString().split("\n")) {
            System.out.println("      " + line);
        }

        if (!footer.getBlocks().isEmpty()) {
            System.out.println("  columns");
            for (final ColumnChunkMetaData column : footer.getBlocks().get(0).getColumns()) {
                System.out.printf("      %-28s %-12s %-10s %s%n",
                        String.join(".", column.getPath().toArray()),
                        column.getPrimitiveType().getPrimitiveTypeName(),
                        column.getCodec(),
                        encodings(column));
            }
        }

        final Map<String, String> metadata =
                new TreeMap<>(footer.getFileMetaData().getKeyValueMetaData());
        System.out.println("  file metadata " + (metadata.isEmpty() ? "(none)" : ""));
        for (final Map.Entry<String, String> entry : metadata.entrySet()) {
            System.out.printf("      %-22s %s%n", entry.getKey(), abbreviate(entry.getValue()));
        }
        System.out.println();
    }

    /**
     * Which encodings a column chunk used. Seeing {@code RLE_DICTIONARY} here is how you know a
     * dictionary was built for that column; {@code PLAIN} alone means it was not.
     */
    private static String encodings(final ColumnChunkMetaData column) {
        final Set<String> names = new LinkedHashSet<>();
        for (final Encoding encoding : column.getEncodings()) {
            names.add(encoding.name());
        }
        return String.join("+", names);
    }

    /** The Avro schema entry is long enough to drown out everything else. */
    private static String abbreviate(final String value) {
        final String flattened = value.replaceAll("\\s+", " ");
        return flattened.length() <= 90 ? flattened : flattened.substring(0, 87) + "...";
    }
}
