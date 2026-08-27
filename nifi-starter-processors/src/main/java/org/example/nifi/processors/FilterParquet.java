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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import org.apache.avro.Schema;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.stream.io.StreamUtils;
import org.example.nifi.api.ParquetSource;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

@Tags({"parquet", "filter", "validate", "metadata"})
@CapabilityDescription("Writes a copy of an incoming Parquet file with the rows that fail "
        + "validation left out, preserving the file's file-level key/value metadata exactly along "
        + "with its compression codec and row group sizing. Reads and writes Parquet with "
        + "the configured ParquetSource service, so there is nothing to "
        + "wire up beyond a ParquetSource service, which supplies both the rows and the "
        + "file-level metadata in one contract. Every column is carried through: only rows are "
        + "removed.")
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
                description = "File-level metadata keys on the copy, comma separated. Identical to "
                        + "the incoming file's."),
        @WritesAttribute(attribute = "mime.type", description = "application/parquet")
})
public class FilterParquet extends AbstractProcessor {

    static final String ROWS_READ_ATTRIBUTE = "parquet.filter.rows.read";
    static final String ROWS_KEPT_ATTRIBUTE = "parquet.filter.rows.kept";
    static final String ROWS_REMOVED_ATTRIBUTE = "parquet.filter.rows.removed";
    static final String METADATA_KEYS_ATTRIBUTE = "parquet.filter.metadata.keys.inherited";

    public static final PropertyDescriptor PARQUET_SOURCE = new PropertyDescriptor.Builder()
            .name("Parquet Source")
            .displayName("Parquet Source")
            .description("Service that opens the incoming Parquet content and exposes both its "
                    + "rows and its file-level metadata. NiFi's own ParquetReader cannot do the "
                    + "second, which is why this is a bundle-local service rather than a "
                    + "RecordReaderFactory.")
            .identifiesControllerService(ParquetSource.class)
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

    private static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList(REL_SUCCESS, REL_ORIGINAL, REL_FAILURE)));

    private static final List<PropertyDescriptor> PROPERTIES = Collections.singletonList(PARQUET_SOURCE);

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

        FlowFile filtered = null;
        String metadataKeys = "";
        try {
            final ParquetSource source =
                    context.getProperty(PARQUET_SOURCE).asControllerService(ParquetSource.class);
            final byte[] content = readContent(session, original);

            final long[] counts = new long[2];
            filtered = session.create(original);
            try (ParquetSource.Handle handle =
                    source.open(content, original.getAttribute(CoreAttributes.UUID.key()))) {

                final Schema avroSchema = new Schema.Parser().parse(handle.avroSchema());
                for (final String field : Item.FIELDS) {
                    if (avroSchema.getField(field) == null) {
                        throw new IOException("Schema is missing the " + field + " field");
                    }
                }
                // One contract, both halves: the rows below and the metadata handed to the writer.
                final Map<String, String> fileMetadata = handle.fileMetadata();

                filtered = session.write(filtered, out -> {
                    try (ParquetWriter<GenericRecord> writer = new ExactMetadataParquetWriter(
                                    new FlowFileOutputFile(out), avroSchema, fileMetadata)
                            .withConf(PARQUET_CONFIGURATION)
                            .withCompressionCodec(CompressionCodecName.valueOf(handle.compressionCodec()))
                            .withRowGroupSize(Math.max(handle.rowGroupSize(), 1L))
                            .build()) {

                        Object row;
                        while ((row = handle.nextRecord()) != null) {
                            counts[0]++;
                            final GenericRecord record = (GenericRecord) row;
                            if (Item.from(record).isValid()) {
                                writer.write(record);
                                counts[1]++;
                            }
                        }
                    }
                });

                metadataKeys = String.join(",", fileMetadata.keySet());
            }

            final long removed = counts[0] - counts[1];
            final Map<String, String> attributes = new HashMap<>();
            attributes.put(ROWS_READ_ATTRIBUTE, Long.toString(counts[0]));
            attributes.put(ROWS_KEPT_ATTRIBUTE, Long.toString(counts[1]));
            attributes.put(ROWS_REMOVED_ATTRIBUTE, Long.toString(removed));
            attributes.put(METADATA_KEYS_ATTRIBUTE, metadataKeys);
            attributes.put(CoreAttributes.MIME_TYPE.key(), "application/parquet");

            filtered = session.putAllAttributes(filtered, attributes);
            session.transfer(filtered, REL_SUCCESS);
            session.transfer(original, REL_ORIGINAL);
            session.adjustCounter("Rows Removed", removed, false);
        } catch (final Exception e) {
            getLogger().error("Failed to filter {}", new Object[] {original}, e);
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
}
