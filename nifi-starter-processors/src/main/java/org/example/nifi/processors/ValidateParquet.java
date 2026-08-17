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
import java.util.ArrayList;
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
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.schema.MessageType;

@Tags({"parquet", "validate", "avro", "record"})
@CapabilityDescription("Validates a Parquet file carried as FlowFile content. The schema must "
        + "contain the fields id, name and status, and every record must satisfy a fixed set of "
        + "rules: id present and positive, at least one of name and status present, name not "
        + "blank when present, and status one of ACTIVE, INACTIVE or PENDING when present. "
        + "Content is never modified. Note that the whole FlowFile is buffered in memory, because "
        + "Parquet keeps its footer at the end of the file while NiFi content streams are "
        + "forward-only.")
@InputRequirement(Requirement.INPUT_REQUIRED)
@SideEffectFree
@SupportsBatching
@WritesAttributes({
        @WritesAttribute(attribute = ValidateParquet.RECORD_COUNT_ATTRIBUTE,
                description = "Number of records read from the file"),
        @WritesAttribute(attribute = ValidateParquet.INVALID_COUNT_ATTRIBUTE,
                description = "Number of records that failed validation"),
        @WritesAttribute(attribute = ValidateParquet.VIOLATIONS_ATTRIBUTE,
                description = "Why the file was rejected. Record-level violations are listed as "
                        + "'row N: reason', capped at the first 10 with a count of the remainder")
})
public class ValidateParquet extends AbstractProcessor {

    static final String RECORD_COUNT_ATTRIBUTE = "parquet.validation.record.count";
    static final String INVALID_COUNT_ATTRIBUTE = "parquet.validation.invalid.count";
    static final String VIOLATIONS_ATTRIBUTE = "parquet.validation.violations";

    private static final String ID_FIELD = "id";
    private static final String NAME_FIELD = "name";
    private static final String STATUS_FIELD = "status";

    private static final List<String> REQUIRED_FIELDS =
            Collections.unmodifiableList(Arrays.asList(ID_FIELD, NAME_FIELD, STATUS_FIELD));

    private static final Set<String> ALLOWED_STATUSES = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList("ACTIVE", "INACTIVE", "PENDING")));

    /**
     * Ceiling on how much content will be buffered. Not configurable by design; raise it here if
     * larger files are expected. A guard is mandatory rather than merely prudent, since anything
     * at or above 2 GiB would also overflow the int cast on the array length.
     */
    private static final long MAX_IN_MEMORY_BYTES = 256L * 1024 * 1024;

    /** Keeps the violations attribute bounded on a file where every row is bad. */
    private static final int MAX_REPORTED_VIOLATIONS = 10;

    public static final Relationship REL_VALID = new Relationship.Builder()
            .name("valid")
            .description("Parquet files whose schema and records all passed validation")
            .build();

    public static final Relationship REL_INVALID = new Relationship.Builder()
            .name("invalid")
            .description("FlowFiles that failed validation: a required field is missing, at least "
                    + "one record broke a rule, or the content is not a readable Parquet file")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles that could not be validated because of an unexpected error, "
                    + "such as content that could not be read or is too large to buffer")
            .build();

    private static final Set<Relationship> RELATIONSHIPS =
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList(REL_VALID, REL_INVALID, REL_FAILURE)));

    /**
     * Reused across invocations. The single-argument parquet builders construct a Hadoop
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
            final byte[] content = readContent(session, flowFile);
            final InputFile inputFile =
                    new ByteArrayInputFile(content, flowFile.getAttribute(CoreAttributes.UUID.key()));

            final ValidationResult result;
            try {
                result = validate(inputFile);
            } catch (final IOException | RuntimeException e) {
                // parquet-java reports "this is not a Parquet file" with a bare RuntimeException
                // rather than a typed one, so this catch has to stay broad. Unreadable content is
                // a data problem, which the user asked to route to invalid rather than failure.
                getLogger().debug("{} could not be read as Parquet", new Object[] {flowFile}, e);
                flowFile = session.putAttribute(flowFile, VIOLATIONS_ATTRIBUTE,
                        "not a readable Parquet file: " + e.getMessage());
                session.transfer(flowFile, REL_INVALID);
                return;
            }

            flowFile = session.putAllAttributes(flowFile, result.attributes);
            session.transfer(flowFile, result.valid ? REL_VALID : REL_INVALID);
        } catch (final Exception e) {
            getLogger().error("Failed to validate {}", new Object[] {flowFile}, e);
            session.transfer(session.penalize(flowFile), REL_FAILURE);
        }
    }

    private byte[] readContent(final ProcessSession session, final FlowFile flowFile) throws IOException {
        final long size = flowFile.getSize();
        if (size > MAX_IN_MEMORY_BYTES) {
            throw new IOException("Content is " + size + " bytes, above the " + MAX_IN_MEMORY_BYTES
                    + " byte ceiling for in-memory Parquet validation");
        }

        // getSize is exact, so this is a single right-sized allocation.
        final byte[] content = new byte[(int) size];
        try (InputStream in = session.read(flowFile)) {
            StreamUtils.fillBuffer(in, content, true);
        }
        return content;
    }

    private ValidationResult validate(final InputFile inputFile) throws IOException {
        final MessageType schema = readSchema(inputFile);

        final List<String> missing = new ArrayList<>();
        for (final String field : REQUIRED_FIELDS) {
            // containsField is case sensitive and matches top-level fields only, which is what we
            // want. It also has to come before any getType call, which throws on unknown names.
            if (!schema.containsField(field)) {
                missing.add(field);
            }
        }
        if (!missing.isEmpty()) {
            return ValidationResult.missingFields(missing);
        }

        long recordCount = 0;
        long invalidCount = 0;
        final List<String> violations = new ArrayList<>();

        try (ParquetReader<GenericRecord> reader = AvroParquetReader
                .<GenericRecord>builder(inputFile, parquetConfiguration)
                // Short-circuits AvroReadSupport, which would otherwise build another Hadoop
                // Configuration just to resolve the data model.
                .withDataModel(GenericData.get())
                .build()) {

            GenericRecord record;
            while ((record = reader.read()) != null) {
                recordCount++;
                final String violation = validateRecord(record);
                if (violation != null) {
                    invalidCount++;
                    // Keep counting past the cap so invalid.count stays accurate.
                    if (violations.size() < MAX_REPORTED_VIOLATIONS) {
                        violations.add("row " + recordCount + ": " + violation);
                    }
                }
            }
        }

        return ValidationResult.of(recordCount, invalidCount, violations);
    }

    /** Reads the footer only. No column data is decompressed. */
    private MessageType readSchema(final InputFile inputFile) throws IOException {
        final ParquetReadOptions options = ParquetReadOptions.builder(parquetConfiguration).build();
        try (ParquetFileReader reader = ParquetFileReader.open(inputFile, options)) {
            return reader.getFileMetaData().getSchema();
        }
    }

    /** @return the first rule the record breaks, or null if it passes */
    private static String validateRecord(final GenericRecord record) {
        final Object rawId = record.get(ID_FIELD);
        final String name = text(record, NAME_FIELD);
        final String status = text(record, STATUS_FIELD);

        if (rawId == null) {
            return "id is null";
        }
        if (!(rawId instanceof Number)) {
            return "id is not numeric";
        }
        if (((Number) rawId).longValue() <= 0) {
            return "id must be positive";
        }
        if (name == null && status == null) {
            return "name and status are both null";
        }
        if (name != null && name.trim().isEmpty()) {
            return "name is blank";
        }
        if (status != null && !ALLOWED_STATUSES.contains(status)) {
            return "status '" + status + "' is not an allowed value";
        }
        return null;
    }

    /**
     * Avro hands back Utf8 rather than String unless the writer set avro.java.string, which files
     * from Spark, pyarrow and DuckDB do not, so casting to String would fail on most real files.
     */
    private static String text(final GenericRecord record, final String field) {
        final Object value = record.get(field);
        return value == null ? null : value.toString();
    }

    private static final class ValidationResult {

        private final boolean valid;
        private final Map<String, String> attributes;

        private ValidationResult(final boolean valid, final Map<String, String> attributes) {
            this.valid = valid;
            this.attributes = attributes;
        }

        /** No records are read in this case, so no counts are reported. */
        static ValidationResult missingFields(final List<String> missing) {
            final Map<String, String> attributes = new HashMap<>();
            attributes.put(VIOLATIONS_ATTRIBUTE,
                    "schema is missing required field(s): " + String.join(", ", missing));
            return new ValidationResult(false, attributes);
        }

        static ValidationResult of(final long recordCount, final long invalidCount,
                final List<String> violations) {
            final Map<String, String> attributes = new HashMap<>();
            attributes.put(RECORD_COUNT_ATTRIBUTE, Long.toString(recordCount));
            attributes.put(INVALID_COUNT_ATTRIBUTE, Long.toString(invalidCount));

            if (invalidCount > 0) {
                final StringBuilder message = new StringBuilder(String.join("; ", violations));
                final long unreported = invalidCount - violations.size();
                if (unreported > 0) {
                    message.append("; ... and ").append(unreported).append(" more");
                }
                attributes.put(VIOLATIONS_ATTRIBUTE, message.toString());
            }

            return new ValidationResult(invalidCount == 0, attributes);
        }
    }

}
