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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.InputFile;

@Tags({"parquet", "metadata", "attributes", "footer", "schema"})
@CapabilityDescription("Reads the file-level metadata of a Parquet file carried as FlowFile content: "
        + "the application that wrote it, its record and row group counts, its Parquet schema, and "
        + "every key/value pair in its footer. Destination decides whether that lands in FlowFile "
        + "attributes or replaces the content with a JSON object, and Attribute Subset narrows it "
        + "to the names worth carrying. Only the footer is read, so no column data is decompressed, "
        + "and it is read straight from the content repository rather than buffered.")
@InputRequirement(Requirement.INPUT_REQUIRED)
@SideEffectFree
@SupportsBatching
@WritesAttributes({
        // Everything but the error attribute is written only when Destination is attributes, and
        // only when Attribute Subset does not filter it out.
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

    static final AllowableValue TO_ATTRIBUTES = new AllowableValue("attributes", "attributes",
            "Write the metadata to FlowFile attributes and leave the content alone");

    static final AllowableValue TO_CONTENT = new AllowableValue("content", "content",
            "Replace the FlowFile content with a JSON object of the metadata");

    public static final PropertyDescriptor DESTINATION = new PropertyDescriptor.Builder()
            .name("Destination")
            .displayName("Destination")
            .description("Where the metadata is written. Writing to content replaces the Parquet "
                    + "file with a JSON object of name and value pairs, so the file itself is gone "
                    + "from the FlowFile: route a copy if it is still needed downstream.")
            .required(true)
            .allowableValues(TO_ATTRIBUTES, TO_CONTENT)
            .defaultValue(TO_ATTRIBUTES.getValue())
            .build();

    public static final PropertyDescriptor ATTRIBUTE_SUBSET = new PropertyDescriptor.Builder()
            .name("Attribute Subset")
            .displayName("Attribute Subset")
            .description("Comma separated list of names to keep, for instance "
                    + "'parquet.record.count,parquet.metadata.source.system'. Names are matched "
                    + "exactly against the full attribute name, so a footer key includes the "
                    + "parquet.metadata. prefix. A name the file has nothing for is simply absent. "
                    + "Left unset, everything found is written, which for a footer carrying a large "
                    + "key such as parquet.avro.schema can be a lot of data to make every "
                    + "downstream FlowFile carry. Applies to both destinations.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> PROPERTIES =
            Collections.unmodifiableList(Arrays.asList(DESTINATION, ATTRIBUTE_SUBSET));

    /** JsonFactory is thread safe and meant to be shared, so one serves every invocation. */
    private static final JsonFactory JSON = new JsonFactory();

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
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
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

            final Map<String, String> metadata = selected(attributesOf(footer),
                    subset(context.getProperty(ATTRIBUTE_SUBSET).getValue()));

            if (TO_CONTENT.getValue().equals(context.getProperty(DESTINATION).getValue())) {
                // Deliberately after the footer has been read: this replaces the Parquet file.
                flowFile = session.write(flowFile, out -> writeJson(metadata, out));
            } else {
                flowFile = session.putAllAttributes(flowFile, metadata);
            }
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

        // Sorted, so the JSON destination writes the same bytes for the same footer every time.
        final Map<String, String> attributes = new TreeMap<>();
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

    /** The names from the Attribute Subset property, or empty when it is not set. */
    private static Set<String> subset(final String property) {
        if (property == null) {
            return Collections.emptySet();
        }
        return Arrays.stream(property.split(","))
                .map(String::trim)
                .filter(name -> !name.isEmpty())
                .collect(Collectors.toSet());
    }

    /** Everything found, or just the named part of it. An empty subset means no filtering. */
    private static Map<String, String> selected(final Map<String, String> metadata,
            final Set<String> subset) {
        if (subset.isEmpty()) {
            return metadata;
        }
        return metadata.entrySet().stream()
                .filter(entry -> subset.contains(entry.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                        (first, second) -> first, TreeMap::new));
    }

    /**
     * Writes the metadata as a flat JSON object of strings.
     *
     * <p>A loop rather than a stream: the generator throws IOException on every call, and a lambda
     * cannot let that out.
     */
    private static void writeJson(final Map<String, String> metadata, final OutputStream out)
            throws IOException {
        try (JsonGenerator json = JSON.createGenerator(out)) {
            json.writeStartObject();
            for (final Map.Entry<String, String> entry : metadata.entrySet()) {
                json.writeStringField(entry.getKey(), entry.getValue());
            }
            json.writeEndObject();
        }
    }

    /** Some parquet failures carry no message, in which case the type name is all we have. */
    private static String describe(final Exception e) {
        return e.getMessage() == null ? e.toString() : e.getMessage();
    }
}
