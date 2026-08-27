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

import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.MessageType;

/**
 * Copies a Parquet file and gives it a different set of file-level key/value metadata.
 *
 * <p>This is what lets NiFi's {@code RecordSetWriter} stay in the picture. That API has no way to
 * set file-level metadata (see {@code RecordApiMetadataLimitsTest}), so the writer produces the
 * filtered file first and this stamps the metadata on afterwards.
 *
 * <p>It is not a re-encode. {@code ParquetFileWriter.appendRowGroups} copies each row group's
 * compressed column chunks across as bytes, so encodings, dictionaries, statistics, page layout and
 * compression all survive exactly. Only the footer is rebuilt, and
 * {@code ParquetFileWriter.end(Map)} takes the complete metadata map, which means unlike
 * {@code ParquetWriter.Builder.withExtraMetaData} there is no reserved-key collision to work
 * around: whatever is passed here is what the file ends up with.
 */
final class ParquetFooterRewriter {

    private ParquetFooterRewriter() {
    }

    /**
     * Writes {@code source} to {@code out} unchanged apart from its footer metadata.
     *
     * @param metadata the complete key/value metadata the copy should carry
     * @return how many row groups were copied
     */
    static int copyWithMetadata(final InputFile source, final OutputStream out,
            final Map<String, String> metadata, final ParquetConfiguration conf) throws IOException {

        final ParquetMetadata footer;
        try (org.apache.parquet.hadoop.ParquetFileReader reader =
                org.apache.parquet.hadoop.ParquetFileReader.open(
                        source, ParquetReadOptions.builder(conf).build())) {
            footer = reader.getFooter();
        }
        final MessageType schema = footer.getFileMetaData().getSchema();

        final ParquetFileWriter writer = new ParquetFileWriter(
                new FlowFileOutputFile(out), schema, ParquetFileWriter.Mode.CREATE,
                ParquetWriter.DEFAULT_BLOCK_SIZE, ParquetWriter.MAX_PADDING_SIZE_DEFAULT);

        writer.start();
        try (SeekableInputStream in = source.newStream()) {
            // dropColumns=false: keep every column, including any the rules never look at.
            writer.appendRowGroups(in, footer.getBlocks(), false);
        }
        writer.end(metadata);
        return footer.getBlocks().size();
    }

    /**
     * The metadata a filtered copy should carry: exactly what the incoming file carried.
     *
     * <p>Because {@code end(Map)} writes the footer wholesale, the copy can be made byte-for-byte
     * identical in metadata to its input, including inheriting nothing at all. A file written by
     * Spark, which carries neither {@code parquet.avro.schema} nor {@code writer.model.name}, comes
     * out still carrying neither, rather than picking them up from the writer that produced the
     * copy.
     *
     * <p>The single exception is a key that would otherwise make the footer lie. If the incoming
     * file declared an Avro schema and the copy was written with a different one, the copy's is
     * used, because a footer describing a schema the data does not have is worse than a footer that
     * differs from the input's. That only happens when the Record Writer is configured with an
     * explicit schema rather than inheriting the reader's, and the caller is told about it.
     *
     * @param conflicts populated with any key whose value had to be taken from the copy
     */
    static Map<String, String> merge(final Map<String, String> inherited, final Map<String, String> written,
            final Map<String, String> conflicts) {
        final Map<String, String> merged = new LinkedHashMap<>(inherited);

        for (final Map.Entry<String, String> entry : written.entrySet()) {
            if (!ParquetFileMetadata.isWriterGenerated(entry.getKey())) {
                continue;
            }
            final String inheritedValue = inherited.get(entry.getKey());
            // Absent upstream means absent downstream: adding it would not be "the same metadata".
            if (inheritedValue != null && !inheritedValue.equals(entry.getValue())) {
                merged.put(entry.getKey(), entry.getValue());
                conflicts.put(entry.getKey(), entry.getValue());
            }
        }
        return merged;
    }
}
