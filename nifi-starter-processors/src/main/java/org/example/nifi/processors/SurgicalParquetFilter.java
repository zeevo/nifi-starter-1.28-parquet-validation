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
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

/**
 * Filters a Parquet file a row group at a time, re-encoding only the row groups that actually
 * contain a failing row.
 *
 * <p>The usual approach decodes every row and writes every row back, which re-encodes the whole
 * file even when a single row is bad. Most of a real file is usually clean, and a clean row group
 * needs nothing done to it: {@code ParquetFileWriter.appendRowGroup} copies its compressed column
 * chunks across as bytes, preserving encodings, dictionaries, statistics and page layout exactly.
 *
 * <p>So this makes two passes over the data. The first counts failures per row group, which is a
 * decode but no encode. The second copies the clean groups verbatim and rewrites only the dirty
 * ones. On a file where the bad rows are concentrated, most groups are untouched.
 *
 * <p>The tradeoff is that a dirty row group is still re-encoded, and that the copy's row groups no
 * longer line up with a single writer's idea of sizing: clean ones keep the input's exact
 * boundaries, rewritten ones get whatever the writer produces.
 */
final class SurgicalParquetFilter {

    /** What one run did, so the processor can report it. */
    static final class Result {
        long rowsRead;
        long rowsKept;
        int rowGroupsCopied;
        int rowGroupsRewritten;
    }

    private SurgicalParquetFilter() {
    }

    static Result filter(final InputFile source, final OutputStream out,
            final ParquetFileMetadata metadata, final ParquetConfiguration conf) throws IOException {

        final List<BlockMetaData> blocks;
        try (ParquetFileReader reader =
                ParquetFileReader.open(source, ParquetReadOptions.builder(conf).build())) {
            blocks = reader.getFooter().getBlocks();
        }

        final Result result = new Result();
        final List<Boolean> clean = scan(source, blocks, conf, result);

        final ParquetFileWriter writer = new ParquetFileWriter(
                new FlowFileOutputFile(out), metadata.parquetSchema(), ParquetFileWriter.Mode.CREATE,
                ParquetWriter.DEFAULT_BLOCK_SIZE, ParquetWriter.MAX_PADDING_SIZE_DEFAULT);
        writer.start();

        try (SeekableInputStream in = source.newStream()) {
            for (int i = 0; i < blocks.size(); i++) {
                if (clean.get(i)) {
                    // Byte copy. Nothing is decoded, so this group's encoding survives exactly.
                    writer.appendRowGroup(in, blocks.get(i), false);
                    result.rowGroupsCopied++;
                } else {
                    rewrite(source, blocks.get(i), metadata, conf, writer);
                    result.rowGroupsRewritten++;
                }
            }
        }

        writer.end(metadata.all());
        return result;
    }

    /** First pass: which row groups are entirely valid, and the overall counts. */
    private static List<Boolean> scan(final InputFile source, final List<BlockMetaData> blocks,
            final ParquetConfiguration conf, final Result result) throws IOException {

        final List<Boolean> clean = new ArrayList<>(blocks.size());
        for (final BlockMetaData block : blocks) {
            long kept = 0;
            for (final GenericRecord record : rowsOf(source, block, conf)) {
                result.rowsRead++;
                if (Item.from(record).isValid()) {
                    kept++;
                    result.rowsKept++;
                }
            }
            clean.add(kept == block.getRowCount());
        }
        return clean;
    }

    /** Second pass for one dirty row group: decode it, drop the bad rows, write the rest. */
    private static void rewrite(final InputFile source, final BlockMetaData block,
            final ParquetFileMetadata metadata, final ParquetConfiguration conf,
            final ParquetFileWriter destination) throws IOException {

        final java.io.ByteArrayOutputStream buffer = new java.io.ByteArrayOutputStream();
        try (ParquetWriter<GenericRecord> writer = new ExactMetadataParquetWriter(
                        new FlowFileOutputFile(buffer), metadata.avroSchema(), metadata.all())
                .withConf(conf)
                .withCompressionCodec(metadata.codec())
                // One row group out, so the rewritten rows stay a single block.
                .withRowGroupSize(Math.max(block.getTotalByteSize() * 2, 1L))
                .build()) {
            for (final GenericRecord record : rowsOf(source, block, conf)) {
                if (Item.from(record).isValid()) {
                    writer.write(record);
                }
            }
        }

        final byte[] rewritten = buffer.toByteArray();
        final ByteArrayInputFile rewrittenFile = new ByteArrayInputFile(rewritten, "rewritten-row-group");
        final List<BlockMetaData> produced;
        try (ParquetFileReader reader =
                ParquetFileReader.open(rewrittenFile, ParquetReadOptions.builder(conf).build())) {
            produced = reader.getFooter().getBlocks();
        }
        if (produced.isEmpty()) {
            // Every row in the group failed, so there is nothing to append.
            return;
        }
        try (SeekableInputStream in = rewrittenFile.newStream()) {
            destination.appendRowGroups(in, produced, false);
        }
    }

    /** The records of a single row group, read by restricting the reader to its byte range. */
    private static List<GenericRecord> rowsOf(final InputFile source, final BlockMetaData block,
            final ParquetConfiguration conf) throws IOException {

        final long start = block.getStartingPos();
        final List<GenericRecord> rows = new ArrayList<>();
        try (ParquetReader<GenericRecord> reader = AvroParquetReader
                .<GenericRecord>builder(source, conf)
                .withDataModel(GenericData.get())
                .withFileRange(start, start + block.getCompressedSize())
                .build()) {
            GenericRecord record;
            while ((record = reader.read()) != null) {
                rows.add(record);
            }
        }
        return rows;
    }
}
