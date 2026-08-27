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
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.conf.PlainParquetConfiguration;

@Tags({"parquet", "record", "filter", "validate", "metadata"})
@CapabilityDescription("Writes a copy of an incoming Parquet file with the rows that fail "
        + "validation left out, carrying across all of the file-level key/value metadata the "
        + "incoming file had. Rows are read and written through the configured Record Reader and "
        + "Record Writer, which should be a ParquetReader and a ParquetRecordSetWriter. Because "
        + "the RecordSetWriter API cannot set file-level metadata, the written file is then copied "
        + "once more to stamp the metadata onto its footer; row groups are copied verbatim, so "
        + "nothing is re-encoded. Every field is carried through: only rows are removed.")
@InputRequirement(Requirement.INPUT_REQUIRED)
@SideEffectFree
@SupportsBatching
@WritesAttributes({
        @WritesAttribute(attribute = FilterParquet.ROWS_READ_ATTRIBUTE,
                description = "Rows read from the incoming file"),
        @WritesAttribute(attribute = FilterParquet.ROWS_KEPT_ATTRIBUTE,
                description = "Rows written to the outgoing file"),
        @WritesAttribute(attribute = FilterParquet.ROWS_REMOVED_ATTRIBUTE,
                description = "Rows left out because they failed validation"),
        @WritesAttribute(attribute = FilterParquet.METADATA_KEYS_ATTRIBUTE,
                description = "File-level metadata keys copied from the incoming file, comma "
                        + "separated. Excludes the keys the Parquet writer regenerates itself."),
        @WritesAttribute(attribute = "mime.type", description = "application/parquet")
})
public class FilterParquet extends AbstractProcessor {

    static final String ROWS_READ_ATTRIBUTE = "parquet.filter.rows.read";
    static final String ROWS_KEPT_ATTRIBUTE = "parquet.filter.rows.kept";
    static final String ROWS_REMOVED_ATTRIBUTE = "parquet.filter.rows.removed";
    static final String METADATA_KEYS_ATTRIBUTE = "parquet.filter.metadata.keys.inherited";

    public static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("Record Reader")
            .displayName("Record Reader")
            .description("Service used to read rows from the incoming file. This should be a "
                    + "ParquetReader: the incoming content has to be Parquet regardless, because "
                    + "the file-level metadata this processor preserves is read from its footer.")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    public static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("Record Writer")
            .displayName("Record Writer")
            .description("Service used to write the filtered rows. This should be a "
                    + "ParquetRecordSetWriter; it decides the output's schema, codec and row group "
                    + "sizing, none of which are inherited from the incoming file.")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(true)
            .build();

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

    private static final List<PropertyDescriptor> PROPERTIES =
            Collections.unmodifiableList(Arrays.asList(RECORD_READER, RECORD_WRITER));

    private static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList(REL_SUCCESS, REL_ORIGINAL, REL_FAILURE)));

    private static final ParquetConfiguration PARQUET_CONFIGURATION = new PlainParquetConfiguration();

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

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

        final RecordReaderFactory readerFactory =
                context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        final RecordSetWriterFactory writerFactory =
                context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);

        final Map<String, String> originalAttributes = original.getAttributes();
        final AtomicLong rowsRead = new AtomicLong();
        final AtomicLong rowsKept = new AtomicLong();
        final Map<String, String> writeAttributes = new HashMap<>();

        FlowFile written = null;
        FlowFile filtered = null;
        try {
            // The incoming file's own metadata. Read straight from its footer, because no part of
            // the record API exposes it.
            final Map<String, String> inherited = ParquetFileMetadata.read(
                    new ByteArrayInputFile(readContent(session, original),
                            original.getAttribute(CoreAttributes.UUID.key())),
                    PARQUET_CONFIGURATION).all();

            // Pass one: filter, entirely through the record API.
            written = session.create(original);
            written = session.write(written, out -> {
                try (InputStream in = session.read(original);
                     RecordReader reader = readerFactory.createRecordReader(
                             originalAttributes, in, original.getSize(), getLogger())) {

                    // The reader's schema, not the writer's: the rules run on what the reader emits.
                    requireRuleFields(reader.getSchema());

                    final RecordSchema schema =
                            writerFactory.getSchema(originalAttributes, reader.getSchema());
                    try (RecordSetWriter writer =
                            writerFactory.createWriter(getLogger(), schema, out, original)) {
                        writer.beginRecordSet();

                        Record record;
                        while ((record = reader.nextRecord()) != null) {
                            rowsRead.incrementAndGet();
                            if (Item.from(record).isValid()) {
                                writer.write(record);
                                rowsKept.incrementAndGet();
                            }
                        }

                        writeAttributes.putAll(writer.finishRecordSet().getAttributes());
                        writeAttributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
                    }
                } catch (final MalformedRecordException | SchemaNotFoundException e) {
                    throw new ProcessException("Could not filter " + original, e);
                }
            });

            // Pass two: copy that file, stamping the inherited metadata onto its footer. Row groups
            // are copied as bytes, so this costs a copy but changes nothing about the data.
            final byte[] writtenContent = readContent(session, written);
            final ByteArrayInputFile writtenFile = new ByteArrayInputFile(
                    writtenContent, written.getAttribute(CoreAttributes.UUID.key()));
            final Map<String, String> conflicts = new LinkedHashMap<>();
            final Map<String, String> merged = ParquetFooterRewriter.merge(inherited,
                    ParquetFileMetadata.read(writtenFile, PARQUET_CONFIGURATION).all(), conflicts);
            if (!conflicts.isEmpty()) {
                getLogger().warn("{} was written with a different schema from the incoming file, so "
                        + "{} could not be inherited verbatim. Configure the Record Writer to "
                        + "inherit the record schema if the copy should match exactly.",
                        new Object[] {original, conflicts.keySet()});
            }

            filtered = session.create(original);
            filtered = session.write(filtered, out ->
                    ParquetFooterRewriter.copyWithMetadata(writtenFile, out, merged, PARQUET_CONFIGURATION));

            session.remove(written);
            written = null;

            final long removed = rowsRead.get() - rowsKept.get();
            writeAttributes.put(ROWS_READ_ATTRIBUTE, Long.toString(rowsRead.get()));
            writeAttributes.put(ROWS_KEPT_ATTRIBUTE, Long.toString(rowsKept.get()));
            writeAttributes.put(ROWS_REMOVED_ATTRIBUTE, Long.toString(removed));
            writeAttributes.put(METADATA_KEYS_ATTRIBUTE, String.join(",", merged.keySet()));

            filtered = session.putAllAttributes(filtered, writeAttributes);
            session.transfer(filtered, REL_SUCCESS);
            session.transfer(original, REL_ORIGINAL);
            session.adjustCounter("Rows Removed", removed, false);
            getLogger().debug("Filtered {}: kept {} of {} rows, footer carries {} metadata keys",
                    new Object[] {original, rowsKept.get(), rowsRead.get(), merged.size()});
        } catch (final Exception e) {
            getLogger().error("Failed to filter {}", new Object[] {original}, e);
            if (written != null) {
                session.remove(written);
            }
            if (filtered != null) {
                session.remove(filtered);
            }
            session.transfer(session.penalize(original), REL_FAILURE);
        }
    }

    private static byte[] readContent(final ProcessSession session, final FlowFile flowFile) throws IOException {
        final byte[] content = new byte[(int) flowFile.getSize()];
        try (InputStream in = session.read(flowFile)) {
            StreamUtils.fillBuffer(in, content, true);
        }
        return content;
    }

    /**
     * Without every field the rules cannot be evaluated, so there is no defensible way to decide
     * which rows to drop. Refusing beats silently keeping everything.
     */
    private static void requireRuleFields(final RecordSchema schema) throws SchemaNotFoundException {
        for (final String field : Item.FIELDS) {
            if (!schema.getField(field).isPresent()) {
                throw new SchemaNotFoundException("Schema is missing the " + field + " field");
            }
        }
    }
}
