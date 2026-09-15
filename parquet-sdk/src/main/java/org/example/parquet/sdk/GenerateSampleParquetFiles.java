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
import java.nio.file.Paths;

import org.example.parquet.sdk.SampleFiles.Sample;

/**
 * Writes every sample Parquet file and prints what each one turned out to be.
 *
 * <pre>
 *   mvn compile exec:java@generate-sample-parquet-files -pl parquet-sdk
 *   mvn compile exec:java@generate-sample-parquet-files -pl parquet-sdk -Dparquet.sdk.output=/tmp/out
 * </pre>
 *
 * <p>Output defaults to {@code parquet-sdk/target/samples}, which is wiped by {@code mvn clean}.
 * Point it somewhere else if the files should outlive a build.
 */
public final class GenerateSampleParquetFiles {

    /** Where the files go unless {@code -Dparquet.sdk.output} says otherwise. */
    static final String OUTPUT_PROPERTY = "parquet.sdk.output";

    private static final String DEFAULT_OUTPUT = "target/samples";

    private GenerateSampleParquetFiles() {
    }

    public static void main(final String[] args) throws IOException {
        final Path directory = outputDirectory(args);
        Files.createDirectories(directory);

        System.out.println("Writing " + SampleFiles.ALL.size()
                + " sample Parquet files to " + directory.toAbsolutePath());
        System.out.println();

        for (final Sample sample : SampleFiles.ALL) {
            System.out.println(sample.fileName() + "  -  " + sample.explanation());
            ParquetSummary.print(sample.writeTo(directory));
        }

        System.out.println("Done. Compare 06 through 09 for codecs, 07 against 11 for the");
        System.out.println("dictionary, and 10 for how a small row group target divides the rows.");
    }

    /**
     * The first argument wins, then the system property, then the default. The argument form is
     * what makes the class usable directly; the property is what survives {@code exec:java}, which
     * passes no arguments of its own.
     */
    static Path outputDirectory(final String[] args) {
        if (args != null && args.length > 0 && !args[0].trim().isEmpty()) {
            return Paths.get(args[0].trim());
        }
        return Paths.get(System.getProperty(OUTPUT_PROPERTY, DEFAULT_OUTPUT));
    }
}
