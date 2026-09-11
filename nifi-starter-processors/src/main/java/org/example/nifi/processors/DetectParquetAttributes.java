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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.InputFile;

@Tags({"parquet", "metadata", "attributes", "footer", "schema"})
@CapabilityDescription("Writes the file-level metadata of a Parquet file carried as FlowFile content "
        + "to FlowFile attributes: the application that wrote it, its record and row group counts, "
        + "its Parquet schema, and every key/value pair in its footer. Only the footer is read, so "
        + "no column data is decompressed, and it is read straight from the content repository "
        + "rather than buffered. What is extracted is fixed, so this processor has no properties. "
        + "Content is never modified.")
@InputRequirement(Requirement.INPUT_REQUIRED)
@SideEffectFree
@SupportsBatching
@WritesAttributes({
        @WritesAttribute(attribute = DetectParquetAttributes.CREATED_BY_ATTRIBUTE,
                description = "The application that wrote the file, for instance 'parquet-mr version "
                        + "1.17.1 (build ...)'. Not written when the footer does not record one."),
        @WritesAttribute(attribute = DetectParquetAttributes.RECORD_COUNT_ATTRIBUTE,
                description = "Number of records in the file, summed from the row group metadata "
                        + "rather than counted by reading them."),
        @WritesAttribute(attribute = DetectParquetAttributes.ROW_GROUP_COUNT_ATTRIBUTE,
                description = "Number of row groups. Zero for a file with no records."),
        @WritesAttribute(attribute = DetectParquetAttributes.SCHEMA_ATTRIBUTE,
                description = "The Parquet schema, in the text form parquet-java prints and parses, "
                        + "for instance 'message Row { optional int64 id; }'."),
        @WritesAttribute(attribute = DetectParquetAttributes.KEY_VALUE_ATTRIBUTE_PREFIX + "*",
                description = "One attribute per key/value pair in the footer, named by the key after "
                        + "this prefix. That includes the keys a writer adds for itself, such as "
                        + "parquet-avro's parquet.avro.schema, which can be large. A key stored "
                        + "without a value is not written."),
        @WritesAttribute(attribute = DetectParquetAttributes.ERROR_ATTRIBUTE,
                description = "Why the content could not be read as Parquet. Written on the failure "
                        + "relationship only.")
})
public class DetectParquetAttributes extends AbstractProcessor {

    static final String CREATED_BY_ATTRIBUTE = "parquet.created.by";
    static final String RECORD_COUNT_ATTRIBUTE = "parquet.record.count";
    static final String ROW_GROUP_COUNT_ATTRIBUTE = "parquet.row.group.count";
    static final String SCHEMA_ATTRIBUTE = "parquet.schema";
    static final String KEY_VALUE_ATTRIBUTE_PREFIX = "parquet.metadata.";
    static final String ERROR_ATTRIBUTE = "parquet.detection.error";

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Parquet files whose metadata was written to attributes")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles whose content is not a readable Parquet file, or that could not "
                    + "be processed because of an unexpected error")
            .build();

    private static final Set<Relationship> RELATIONSHIPS =
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList(REL_SUCCESS, REL_FAILURE)));

    /**
     * Reused across invocations. ParquetReadOptions.builder() with no argument constructs a Hadoop
     * Configuration instead, which XML-parses core-default.xml on every call.
     */
    private final ParquetConfiguration parquetConfiguration = new PlainParquetConfiguration();

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

        try {
            final InputFile inputFile = new FlowFileInputFile(session, flowFile);

            final ParquetMetadata footer;
            try {
                footer = readFooter(inputFile);
            } catch (final IOException | RuntimeException e) {
                // parquet-java reports "this is not a Parquet file" with a bare RuntimeException
                // rather than a typed one, so this catch has to stay broad. Unreadable content is a
                // data problem that retrying cannot fix, so unlike the catch below it is not
                // penalized.
                getLogger().debug("{} could not be read as Parquet", new Object[] {flowFile}, e);
                flowFile = session.putAttribute(flowFile, ERROR_ATTRIBUTE,
                        "not a readable Parquet file: " + describe(e));
                session.transfer(flowFile, REL_FAILURE);
                return;
            }

            flowFile = session.putAllAttributes(flowFile, attributesOf(footer));
            session.transfer(flowFile, REL_SUCCESS);
        } catch (final Exception e) {
            getLogger().error("Failed to read Parquet metadata from {}", new Object[] {flowFile}, e);
            session.transfer(session.penalize(flowFile), REL_FAILURE);
        }
    }

    /** Reads the footer only. No column data is decompressed. */
    private ParquetMetadata readFooter(final InputFile inputFile) throws IOException {
        final ParquetReadOptions options = ParquetReadOptions.builder(parquetConfiguration).build();
        try (ParquetFileReader reader = ParquetFileReader.open(inputFile, options)) {
            return reader.getFooter();
        }
    }

    private static Map<String, String> attributesOf(final ParquetMetadata footer) {
        final FileMetaData fileMetaData = footer.getFileMetaData();

        long recordCount = 0;
        for (final BlockMetaData block : footer.getBlocks()) {
            recordCount += block.getRowCount();
        }

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(RECORD_COUNT_ATTRIBUTE, Long.toString(recordCount));
        attributes.put(ROW_GROUP_COUNT_ATTRIBUTE, Integer.toString(footer.getBlocks().size()));
        attributes.put(SCHEMA_ATTRIBUTE, fileMetaData.getSchema().toString());

        // The footer makes created_by and every key/value value optional. NiFi silently drops a null
        // attribute value but nifi-mock keeps it, so nulls are skipped here to keep the two in step.
        if (fileMetaData.getCreatedBy() != null) {
            attributes.put(CREATED_BY_ATTRIBUTE, fileMetaData.getCreatedBy());
        }
        for (final Map.Entry<String, String> entry : fileMetaData.getKeyValueMetaData().entrySet()) {
            if (entry.getValue() != null) {
                attributes.put(KEY_VALUE_ATTRIBUTE_PREFIX + entry.getKey(), entry.getValue());
            }
        }
        return attributes;
    }

    /** Some parquet failures carry no message, in which case the type name is all we have. */
    private static String describe(final Exception e) {
        return e.getMessage() == null ? e.toString() : e.getMessage();
    }
}
