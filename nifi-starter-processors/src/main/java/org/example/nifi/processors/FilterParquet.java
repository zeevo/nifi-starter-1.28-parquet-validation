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
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.schema.MessageType;

@Tags({"parquet", "filter", "validate", "avro", "record"})
@CapabilityDescription("Writes a copy of an incoming Parquet file with the rows that fail "
        + "validation left out. The rules are the same ones ValidateParquet applies, and are "
        + "compiled in rather than configurable. Every column is carried through; only rows are "
        + "removed. Note that the output is a freshly encoded file, not a copy of the input with "
        + "rows deleted, so its row group layout and encodings are chosen by this processor.")
@InputRequirement(Requirement.INPUT_REQUIRED)
@SideEffectFree
@SupportsBatching
@WritesAttributes({
        @WritesAttribute(attribute = FilterParquet.ROWS_READ_ATTRIBUTE,
                description = "Rows read from the incoming file"),
        @WritesAttribute(attribute = FilterParquet.ROWS_KEPT_ATTRIBUTE,
                description = "Rows written to the outgoing file"),
        @WritesAttribute(attribute = FilterParquet.ROWS_REMOVED_ATTRIBUTE,
                description = "Rows left out because they failed validation")
})
public class FilterParquet extends AbstractProcessor {

    static final String ROWS_READ_ATTRIBUTE = "parquet.filter.rows.read";
    static final String ROWS_KEPT_ATTRIBUTE = "parquet.filter.rows.kept";
    static final String ROWS_REMOVED_ATTRIBUTE = "parquet.filter.rows.removed";

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("The filtered copy, containing only the rows that passed validation")
            .build();

    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("The incoming file, unchanged")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles that could not be filtered: content that is not readable as "
                    + "Parquet, or whose schema is missing a field the rules need")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList(REL_SUCCESS, REL_ORIGINAL, REL_FAILURE)));

    private final ParquetConfiguration parquetConfiguration = new PlainParquetConfiguration();

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final FlowFile original = session.get();
        if (original == null) {
            return;
        }

        try {
            final byte[] content = readContent(session, original);
            final InputFile inputFile =
                    new ByteArrayInputFile(content, original.getAttribute(CoreAttributes.UUID.key()));

            final ParquetMetadata footer = readFooter(inputFile);
            final MessageType parquetSchema = footer.getFileMetaData().getSchema();

            for (final String field : Item.FIELDS) {
                if (!parquetSchema.containsField(field)) {
                    // Without every field the rules cannot be evaluated, so there is no defensible
                    // way to decide which rows to drop. Refusing beats silently keeping everything.
                    getLogger().error("Cannot filter {}, schema is missing the {} field",
                            new Object[] {original, field});
                    session.transfer(session.penalize(original), REL_FAILURE);
                    return;
                }
            }

            final AtomicLong rowsRead = new AtomicLong();
            final AtomicLong rowsKept = new AtomicLong();

            FlowFile filtered = session.create(original);
            filtered = session.write(filtered, out -> {
                try (ParquetWriter<GenericRecord> writer = AvroParquetWriter
                        .<GenericRecord>builder(new FlowFileOutputFile(out))
                        // Carrying the Parquet schema across rather than an Avro one of our own
                        // keeps every column, including ones the rules never look at.
                        .withSchema(new AvroSchemaConverter().convert(parquetSchema))
                        .withDataModel(GenericData.get())
                        .withConf(parquetConfiguration)
                        .withCompressionCodec(codecOf(footer))
                        .withRowGroupSize(rowGroupSizeOf(footer))
                        .build();
                     ParquetReader<GenericRecord> reader = AvroParquetReader
                             .<GenericRecord>builder(inputFile, parquetConfiguration)
                             .withDataModel(GenericData.get())
                             .build()) {

                    GenericRecord record;
                    while ((record = reader.read()) != null) {
                        rowsRead.incrementAndGet();
                        if (Item.from(record).isValid()) {
                            writer.write(record);
                            rowsKept.incrementAndGet();
                        }
                    }
                }
            });

            final long removed = rowsRead.get() - rowsKept.get();
            filtered = session.putAttribute(filtered, ROWS_READ_ATTRIBUTE, Long.toString(rowsRead.get()));
            filtered = session.putAttribute(filtered, ROWS_KEPT_ATTRIBUTE, Long.toString(rowsKept.get()));
            filtered = session.putAttribute(filtered, ROWS_REMOVED_ATTRIBUTE, Long.toString(removed));

            session.transfer(filtered, REL_SUCCESS);
            session.transfer(original, REL_ORIGINAL);
            getLogger().debug("Filtered {}: kept {} of {} rows",
                    new Object[] {original, rowsKept.get(), rowsRead.get()});
        } catch (final Exception e) {
            // As in ValidateParquet, content that is not Parquet arrives as a bare RuntimeException
            // from parquet-java, so this cannot be narrowed usefully.
            getLogger().error("Failed to filter {}", new Object[] {original}, e);
            session.transfer(session.penalize(original), REL_FAILURE);
        }
    }

    private byte[] readContent(final ProcessSession session, final FlowFile flowFile) throws IOException {
        final byte[] content = new byte[(int) flowFile.getSize()];
        try (InputStream in = session.read(flowFile)) {
            StreamUtils.fillBuffer(in, content, true);
        }
        return content;
    }

    private ParquetMetadata readFooter(final InputFile inputFile) throws IOException {
        final ParquetReadOptions options = ParquetReadOptions.builder(parquetConfiguration).build();
        try (ParquetFileReader reader = ParquetFileReader.open(inputFile, options)) {
            return reader.getFooter();
        }
    }

    /**
     * Reuses whatever codec the incoming file was written with. Without this the writer would fall
     * back to its own default and a SNAPPY or ZSTD file would come out uncompressed, which can
     * make the filtered copy larger than the original it was meant to shrink.
     */
    private static CompressionCodecName codecOf(final ParquetMetadata footer) {
        if (footer.getBlocks().isEmpty() || footer.getBlocks().get(0).getColumns().isEmpty()) {
            return CompressionCodecName.UNCOMPRESSED;
        }
        return footer.getBlocks().get(0).getColumns().get(0).getCodec();
    }

    /**
     * Matches the incoming file's row group sizing. Row groups are what a downstream reader splits
     * and skips on, and the writer holds one in memory before flushing it, so inheriting the
     * default here would turn a file of many small row groups into a single large one and quietly
     * change both the read characteristics and this processor's own memory ceiling.
     */
    private static long rowGroupSizeOf(final ParquetMetadata footer) {
        if (footer.getBlocks().isEmpty()) {
            return ParquetWriter.DEFAULT_BLOCK_SIZE;
        }
        long uncompressed = 0;
        for (final BlockMetaData block : footer.getBlocks()) {
            uncompressed += block.getTotalByteSize();
        }
        // No floor. Imposing a minimum here would quietly consolidate a file that was written
        // with small row groups, which is a re-tuning decision this processor has no business
        // making: it was asked to remove rows, not to reorganise the file.
        return Math.max(1L, uncompressed / footer.getBlocks().size());
    }
}
