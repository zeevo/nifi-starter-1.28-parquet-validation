package org.example.nifi.processors;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.InvalidRecordException;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

@Tags({"parquet", "validate", "schema", "starter"})
@CapabilityDescription("Validates FlowFile content against an expected Parquet schema. The content must be a "
        + "structurally valid Parquet file whose schema matches the configured expectation; optionally every row "
        + "is decompressed and decoded as well, which detects corruption in the data pages that a schema check "
        + "alone cannot see. Conforming FlowFiles are routed to 'valid', all others to 'invalid'. Content is "
        + "never modified.")
@InputRequirement(Requirement.INPUT_REQUIRED)
@SideEffectFree
@SupportsBatching
@WritesAttributes({
        @WritesAttribute(attribute = ValidateParquet.RECORD_COUNT_ATTRIBUTE,
                description = "Number of records in the file, set on FlowFiles routed to 'valid'"),
        @WritesAttribute(attribute = ValidateParquet.DETAIL_ATTRIBUTE,
                description = "Reason the file failed validation, set on FlowFiles routed to 'invalid'")
})
public class ValidateParquet extends AbstractProcessor {

    static final String RECORD_COUNT_ATTRIBUTE = "record.count";
    static final String DETAIL_ATTRIBUTE = "parquet.validation.detail";

    static final AllowableValue MODE_EXACT = new AllowableValue("exact", "Exact",
            "The file's schema must match the expected schema exactly: same columns, same order, same types and "
                    + "repetition (required/optional/repeated). The top-level message name is ignored.");
    static final AllowableValue MODE_CONTAINS = new AllowableValue("contains", "Contains",
            "The file's schema must contain every expected column with matching type and repetition; columns not "
                    + "listed in the expected schema are allowed. Column order is ignored.");

    static final AllowableValue DEPTH_SCHEMA_ONLY = new AllowableValue("schema-only", "Schema Only",
            "Only the file footer and schema are checked. Fast, but corruption inside data pages is not detected.");
    static final AllowableValue DEPTH_FULL = new AllowableValue("full", "Schema and Content",
            "After the schema check, every row is decompressed and decoded, verifying page checksums where "
                    + "present. Detects corrupt or truncated data pages at the cost of reading the whole file.");

    private static final Validator PARQUET_SCHEMA_VALIDATOR = (subject, input, context) -> {
        try {
            MessageTypeParser.parseMessageType(input);
            return new ValidationResult.Builder().subject(subject).input(input).valid(true).build();
        } catch (final RuntimeException e) {
            return new ValidationResult.Builder().subject(subject).input(input).valid(false)
                    .explanation("not a valid Parquet message type: " + e.getMessage()).build();
        }
    };

    public static final PropertyDescriptor SCHEMA_ATTRIBUTE = new PropertyDescriptor.Builder()
            .name("Schema Attribute Name")
            .displayName("Schema Attribute Name")
            .description("Optional name of a FlowFile attribute carrying the expected schema, in the same Parquet "
                    + "message type syntax as the 'Parquet Schema' property. When this is set and the attribute is "
                    + "present and non-blank, its schema is used for that FlowFile. When the attribute is missing "
                    + "or blank, the 'Parquet Schema' property is used as the fallback. When the attribute is "
                    + "present but is not a valid Parquet message type, the FlowFile is routed to 'invalid' rather "
                    + "than silently falling back, so that a broken schema is not mistaken for an absent one.")
            .required(false)
            .addValidator(StandardValidators.ATTRIBUTE_KEY_VALIDATOR)
            .build();

    public static final PropertyDescriptor SCHEMA = new PropertyDescriptor.Builder()
            .name("Parquet Schema")
            .displayName("Parquet Schema")
            .description("The expected schema in Parquet message type syntax, e.g. "
                    + "'message event { required int64 id; optional binary name (STRING); }'. "
                    + "Nested group types are supported. This is the fallback schema: it is used whenever a "
                    + "per-FlowFile schema is not found via 'Schema Attribute Name'.")
            .required(true)
            .addValidator(PARQUET_SCHEMA_VALIDATOR)
            .build();

    public static final PropertyDescriptor MATCH_MODE = new PropertyDescriptor.Builder()
            .name("Schema Match Mode")
            .displayName("Schema Match Mode")
            .description("How the file's schema is compared to the expected schema")
            .required(true)
            .allowableValues(MODE_EXACT, MODE_CONTAINS)
            .defaultValue(MODE_EXACT.getValue())
            .build();

    public static final PropertyDescriptor VALIDATION_DEPTH = new PropertyDescriptor.Builder()
            .name("Validation Depth")
            .displayName("Validation Depth")
            .description("How much of the file is read during validation")
            .required(true)
            .allowableValues(DEPTH_FULL, DEPTH_SCHEMA_ONLY)
            .defaultValue(DEPTH_FULL.getValue())
            .build();

    public static final Relationship REL_VALID = new Relationship.Builder()
            .name("valid")
            .description("FlowFiles that are valid Parquet and conform to the expected schema")
            .build();

    public static final Relationship REL_INVALID = new Relationship.Builder()
            .name("invalid")
            .description("FlowFiles that are not valid Parquet or do not conform to the expected schema")
            .build();

    private static final List<PropertyDescriptor> PROPERTIES =
            Collections.unmodifiableList(Arrays.asList(SCHEMA_ATTRIBUTE, SCHEMA, MATCH_MODE, VALIDATION_DEPTH));

    private static final Set<Relationship> RELATIONSHIPS =
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList(REL_VALID, REL_INVALID)));

    /** Parsed form of the SCHEMA property, used whenever a per-FlowFile schema is not found. */
    private volatile MessageType fallbackSchema;

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        fallbackSchema = MessageTypeParser.parseMessageType(context.getProperty(SCHEMA).getValue());
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final boolean exact = MODE_EXACT.getValue().equals(context.getProperty(MATCH_MODE).getValue());
        final boolean fullRead = DEPTH_FULL.getValue().equals(context.getProperty(VALIDATION_DEPTH).getValue());

        final MessageType expected;
        try {
            expected = resolveExpectedSchema(context, flowFile);
        } catch (final RuntimeException e) {
            getLogger().info("{} has an unusable expected schema: {}", flowFile, e.getMessage());
            session.transfer(session.putAttribute(flowFile, DETAIL_ATTRIBUTE, e.getMessage()), REL_INVALID);
            return;
        }

        String detail;
        long recordCount = -1;
        try (InputStream in = session.read(flowFile)) {
            final ParquetReadOptions options = ParquetReadOptions.builder()
                    .usePageChecksumVerification(true)
                    .build();
            try (ParquetFileReader reader = ParquetFileReader.open(
                    new FlowFileInputFile(in, flowFile.getSize()), options)) {
                final MessageType fileSchema = reader.getFooter().getFileMetaData().getSchema();
                detail = compareSchema(fileSchema, expected, exact);
                if (detail == null) {
                    if (fullRead) {
                        recordCount = readAllRows(reader, fileSchema);
                        final long declared = reader.getRecordCount();
                        if (recordCount != declared) {
                            detail = "footer declares " + declared + " records but " + recordCount
                                    + " were read";
                        }
                    } else {
                        recordCount = reader.getRecordCount();
                    }
                }
            } catch (final IOException | RuntimeException e) {
                detail = "not a valid Parquet file: " + e.getMessage();
            }
        } catch (final IOException e) {
            detail = "not a valid Parquet file: " + e.getMessage();
        }

        if (detail == null) {
            flowFile = session.putAttribute(flowFile, RECORD_COUNT_ATTRIBUTE, String.valueOf(recordCount));
            session.transfer(flowFile, REL_VALID);
        } else {
            getLogger().info("{} failed Parquet validation: {}", flowFile, detail);
            flowFile = session.putAttribute(flowFile, DETAIL_ATTRIBUTE, detail);
            session.transfer(flowFile, REL_INVALID);
        }
    }

    /**
     * Resolves the schema to validate against. A FlowFile carrying its own schema in the
     * configured attribute wins; otherwise the SCHEMA property is the fallback. An
     * attribute that is present but unparseable is an error rather than a reason to fall
     * back, so that a malformed schema is not mistaken for an absent one.
     */
    private MessageType resolveExpectedSchema(final ProcessContext context, final FlowFile flowFile) {
        final String attributeName = context.getProperty(SCHEMA_ATTRIBUTE).getValue();
        if (attributeName == null) {
            return fallbackSchema;
        }

        final String schemaText = flowFile.getAttribute(attributeName);
        if (schemaText == null || schemaText.trim().isEmpty()) {
            getLogger().debug("{} has no schema in attribute '{}', using the fallback schema",
                    flowFile, attributeName);
            return fallbackSchema;
        }

        try {
            return MessageTypeParser.parseMessageType(schemaText);
        } catch (final RuntimeException e) {
            throw new IllegalArgumentException("attribute '" + attributeName
                    + "' is not a valid Parquet message type: " + e.getMessage(), e);
        }
    }

    /**
     * Returns null when the file schema conforms, otherwise a human-readable reason.
     * The top-level message name is ignored in both modes: producers name it
     * inconsistently (e.g. Avro record name, "spark_schema", "root").
     */
    private String compareSchema(final MessageType fileSchema, final MessageType expectedSchema,
            final boolean exact) {
        final MessageType expected = new MessageType(fileSchema.getName(), expectedSchema.getFields());
        if (exact) {
            if (expected.equals(fileSchema)) {
                return null;
            }
            return "schema does not exactly match the expected schema. Expected: " + expected
                    + " Actual: " + fileSchema;
        }
        try {
            fileSchema.checkContains(expected);
            return null;
        } catch (final InvalidRecordException e) {
            return "schema does not contain the expected columns: " + e.getMessage();
        }
    }

    /**
     * Decodes every row of every row group, forcing all pages to be decompressed and
     * read. Throws if any page is corrupt, undecodable, or fails its checksum.
     */
    private long readAllRows(final ParquetFileReader reader, final MessageType fileSchema) throws IOException {
        final MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(fileSchema);
        long count = 0;
        PageReadStore rowGroup;
        while ((rowGroup = reader.readNextRowGroup()) != null) {
            final RecordReader<Group> recordReader =
                    columnIO.getRecordReader(rowGroup, new GroupRecordConverter(fileSchema));
            for (long i = 0, rows = rowGroup.getRowCount(); i < rows; i++) {
                recordReader.read();
                count++;
            }
        }
        return count;
    }

}
