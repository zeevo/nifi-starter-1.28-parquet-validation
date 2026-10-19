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

import java.io.BufferedInputStream;
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

import org.apache.hadoop.conf.Configuration;
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
import org.apache.nifi.parquet.record.ParquetRecordReader;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

@Tags({"parquet", "record", "row", "get"})
@CapabilityDescription("Reads a Parquet file carried as FlowFile content and writes the single row "
        + "at the configured index out with the configured Record Writer. The index is zero-based. "
        + "Rows before the index are read but not converted to records, and reading stops at "
        + "the requested row. A file with no row at that index is routed to failure.")
@InputRequirement(Requirement.INPUT_REQUIRED)
@SideEffectFree
@SupportsBatching
@WritesAttributes({
        @WritesAttribute(attribute = "record.count", description = "Always 1"),
        @WritesAttribute(attribute = "mime.type", description = "The MIME type of the Record Writer")
})
public class GetParquetRow extends AbstractProcessor {

    /** The keys ParquetRecordReader reads its offset and limit from. */
    private static final String RECORD_OFFSET = "record.offset";
    private static final String RECORD_COUNT = "record.count";

    public static final PropertyDescriptor INDEX = new PropertyDescriptor.Builder()
            .name("index")
            .displayName("Index")
            .description("Zero-based index of the row to get")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("record-writer")
            .displayName("Record Writer")
            .description("Specifies the Controller Service to use for writing out the row")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(true)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("The requested row, written with the Record Writer")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles that are not readable as Parquet, have no row at the index, "
                    + "or whose row could not be written")
            .build();

    private static final List<PropertyDescriptor> PROPERTIES =
            Collections.unmodifiableList(Arrays.asList(INDEX, RECORD_WRITER));

    private static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList(REL_SUCCESS, REL_FAILURE)));

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final long index = context.getProperty(INDEX).asLong();
        final RecordSetWriterFactory writerFactory =
                context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);

        final Map<String, String> readerVariables = new HashMap<>();
        readerVariables.put(RECORD_OFFSET, Long.toString(index));
        readerVariables.put(RECORD_COUNT, "1");

        final FlowFile original = flowFile;
        final Map<String, String> attributes = new HashMap<>();
        try {
            flowFile = session.write(flowFile, (final InputStream in, final OutputStream out) -> {
                // ParquetRecordReader seeks with mark/reset, which a content stream may not offer.
                final InputStream markable = in.markSupported() ? in : new BufferedInputStream(in);
                try (ParquetRecordReader reader = new ParquetRecordReader(
                        markable, original.getSize(), new Configuration(), readerVariables)) {

                    final Record row = reader.nextRecord();
                    if (row == null) {
                        throw new IOException("No row at index " + index);
                    }

                    final RecordSchema writeSchema =
                            writerFactory.getSchema(original.getAttributes(), row.getSchema());
                    try (RecordSetWriter writer =
                                 writerFactory.createWriter(getLogger(), writeSchema, out, original)) {
                        writer.beginRecordSet();
                        writer.write(row);
                        final WriteResult result = writer.finishRecordSet();

                        attributes.put("record.count", String.valueOf(result.getRecordCount()));
                        attributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
                        attributes.putAll(result.getAttributes());
                    }
                } catch (final MalformedRecordException | SchemaNotFoundException e) {
                    throw new ProcessException(e);
                }
            });
        } catch (final Exception e) {
            getLogger().error("Failed to get row {} of {}", index, flowFile, e);
            session.transfer(session.penalize(flowFile), REL_FAILURE);
            return;
        }

        flowFile = session.putAllAttributes(flowFile, attributes);
        session.transfer(flowFile, REL_SUCCESS);
    }
}
