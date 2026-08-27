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
import java.util.List;
import java.util.Map;
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
import org.apache.nifi.avro.AvroTypeUtil;
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
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.hadoop.ParquetWriter;

@Tags({"parquet", "record", "filter", "validate", "metadata"})
@CapabilityDescription("Writes a copy of an incoming Parquet file with the rows that fail "
        + "validation left out, preserving the file's own encoding and all of its file-level "
        + "key/value metadata. Rows are read through the configured Record Reader, which should be "
        + "a ParquetReader. The copy is written with parquet-java directly rather than through a "
        + "Record Writer, because NiFi's RecordSetWriter API has no way to set file-level "
        + "metadata. Every field is carried through: only rows are removed.")
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
            Collections.singletonList(RECORD_READER);

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

        final Map<String, String> originalAttributes = original.getAttributes();
        final AtomicLong rowsRead = new AtomicLong();
        final AtomicLong rowsKept = new AtomicLong();

        FlowFile filtered = session.create(original);
        try {
            // The footer holds what a filtered copy should inherit: the key/value metadata, the
            // codec, the row group sizing and the file's own Avro schema. None of that is
            // reachable through the record API, so it is read with parquet-java directly.
            final byte[] content = readContent(session, original);
            final ParquetFileMetadata metadata = ParquetFileMetadata.read(
                    new ByteArrayInputFile(content, original.getAttribute(CoreAttributes.UUID.key())),
                    PARQUET_CONFIGURATION);

            try (InputStream in = session.read(original);
                 RecordReader reader = readerFactory.createRecordReader(
                         originalAttributes, in, original.getSize(), getLogger())) {

                // Check the reader's schema, not the writer's: the rules run against the records
                // the reader produces.
                requireRuleFields(reader.getSchema());

                filtered = session.write(filtered, out -> {
                    try (ParquetWriter<GenericRecord> writer = buildWriter(out, metadata)) {
                        Record record;
                        while ((record = reader.nextRecord()) != null) {
                            rowsRead.incrementAndGet();
                            if (Item.from(record).isValid()) {
                                writer.write(AvroTypeUtil.createAvroRecord(record, metadata.avroSchema()));
                                rowsKept.incrementAndGet();
                            }
                        }
                    } catch (final MalformedRecordException e) {
                        throw new ProcessException("Could not filter " + original, e);
                    }
                });
            }

            final long removed = rowsRead.get() - rowsKept.get();
            final Map<String, String> attributes = new HashMap<>();
            attributes.put(ROWS_READ_ATTRIBUTE, Long.toString(rowsRead.get()));
            attributes.put(ROWS_KEPT_ATTRIBUTE, Long.toString(rowsKept.get()));
            attributes.put(ROWS_REMOVED_ATTRIBUTE, Long.toString(removed));
            attributes.put(METADATA_KEYS_ATTRIBUTE, String.join(",", metadata.inheritable().keySet()));
            attributes.put(CoreAttributes.MIME_TYPE.key(), "application/parquet");

            filtered = session.putAllAttributes(filtered, attributes);
            session.transfer(filtered, REL_SUCCESS);
            session.transfer(original, REL_ORIGINAL);
            session.adjustCounter("Rows Removed", removed, false);
            getLogger().debug("Filtered {}: kept {} of {} rows, inherited {} metadata keys",
                    new Object[] {original, rowsKept.get(), rowsRead.get(), metadata.inheritable().size()});
        } catch (final Exception e) {
            getLogger().error("Failed to filter {}", new Object[] {original}, e);
            session.remove(filtered);
            session.transfer(session.penalize(original), REL_FAILURE);
        }
    }

    /**
     * Reproduces the incoming file's encoding as closely as the writer allows and carries its
     * file-level metadata across. The writer generated keys are dropped by
     * {@link ParquetFileMetadata#inheritable()} and put back here by the Avro write support, so
     * the copy still ends up with the full set.
     */
    private ParquetWriter<GenericRecord> buildWriter(final OutputStream out,
            final ParquetFileMetadata metadata) throws IOException {
        final AvroParquetWriter.Builder<GenericRecord> builder = AvroParquetWriter
                .<GenericRecord>builder(new FlowFileOutputFile(out))
                .withSchema(metadata.avroSchema())
                .withDataModel(GenericData.get())
                .withConf(PARQUET_CONFIGURATION)
                .withCompressionCodec(metadata.codec())
                .withExtraMetaData(metadata.inheritable());

        if (metadata.rowGroupSize() > 0) {
            builder.withRowGroupSize(metadata.rowGroupSize());
        }
        return builder.build();
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
