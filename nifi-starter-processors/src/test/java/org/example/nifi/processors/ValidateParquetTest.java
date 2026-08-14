package org.example.nifi.processors;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ValidateParquetTest {

    private static final String AVRO_SCHEMA = "{\"type\":\"record\",\"name\":\"Event\",\"fields\":["
            + "{\"name\":\"id\",\"type\":\"long\"},"
            + "{\"name\":\"name\",\"type\":[\"null\",\"string\"],\"default\":null}]}";

    private static final String AVRO_SCHEMA_EXTRA_COLUMN = "{\"type\":\"record\",\"name\":\"Event\",\"fields\":["
            + "{\"name\":\"id\",\"type\":\"long\"},"
            + "{\"name\":\"name\",\"type\":[\"null\",\"string\"],\"default\":null},"
            + "{\"name\":\"extra\",\"type\":\"int\"}]}";

    private static final String EXPECTED_SCHEMA =
            "message event { required int64 id; optional binary name (STRING); }";

    private TestRunner runner;

    @BeforeEach
    public void init() {
        runner = TestRunners.newTestRunner(ValidateParquet.class);
        runner.setProperty(ValidateParquet.SCHEMA, EXPECTED_SCHEMA);
    }

    @Test
    public void testInvalidSchemaPropertyFailsValidation() {
        runner.setProperty(ValidateParquet.SCHEMA, "message broken { not a schema }");
        runner.assertNotValid();
    }

    @Test
    public void testConformingFileIsValid() throws IOException {
        runner.enqueue(writeParquet(AVRO_SCHEMA, CompressionCodecName.SNAPPY, 100));
        runner.run();

        runner.assertAllFlowFilesTransferred(ValidateParquet.REL_VALID, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ValidateParquet.REL_VALID).get(0);
        out.assertAttributeEquals(ValidateParquet.RECORD_COUNT_ATTRIBUTE, "100");
    }

    @Test
    public void testMessageNameIsIgnored() throws IOException {
        // file's message name is "Event" (the Avro record name); expected says "event"
        runner.setProperty(ValidateParquet.SCHEMA,
                "message somethingelse { required int64 id; optional binary name (STRING); }");
        runner.enqueue(writeParquet(AVRO_SCHEMA, CompressionCodecName.SNAPPY, 5));
        runner.run();

        runner.assertAllFlowFilesTransferred(ValidateParquet.REL_VALID, 1);
    }

    @Test
    public void testWrongColumnTypeIsInvalid() throws IOException {
        runner.setProperty(ValidateParquet.SCHEMA,
                "message event { required int32 id; optional binary name (STRING); }");
        runner.enqueue(writeParquet(AVRO_SCHEMA, CompressionCodecName.SNAPPY, 5));
        runner.run();

        runner.assertAllFlowFilesTransferred(ValidateParquet.REL_INVALID, 1);
        assertDetailContains("int32");
    }

    @Test
    public void testMissingColumnIsInvalid() throws IOException {
        runner.setProperty(ValidateParquet.SCHEMA,
                "message event { required int64 id; required binary missing_col (STRING); }");
        runner.setProperty(ValidateParquet.MATCH_MODE, ValidateParquet.MODE_CONTAINS.getValue());
        runner.enqueue(writeParquet(AVRO_SCHEMA, CompressionCodecName.SNAPPY, 5));
        runner.run();

        runner.assertAllFlowFilesTransferred(ValidateParquet.REL_INVALID, 1);
        assertDetailContains("missing_col");
    }

    @Test
    public void testRequiredVsOptionalIsInvalid() throws IOException {
        // file has optional name; expecting required must fail even in contains mode
        runner.setProperty(ValidateParquet.SCHEMA,
                "message event { required int64 id; required binary name (STRING); }");
        runner.setProperty(ValidateParquet.MATCH_MODE, ValidateParquet.MODE_CONTAINS.getValue());
        runner.enqueue(writeParquet(AVRO_SCHEMA, CompressionCodecName.SNAPPY, 5));
        runner.run();

        runner.assertAllFlowFilesTransferred(ValidateParquet.REL_INVALID, 1);
    }

    @Test
    public void testExtraColumnIsInvalidInExactMode() throws IOException {
        runner.enqueue(writeParquet(AVRO_SCHEMA_EXTRA_COLUMN, CompressionCodecName.SNAPPY, 5));
        runner.run();

        runner.assertAllFlowFilesTransferred(ValidateParquet.REL_INVALID, 1);
    }

    @Test
    public void testExtraColumnIsValidInContainsMode() throws IOException {
        runner.setProperty(ValidateParquet.MATCH_MODE, ValidateParquet.MODE_CONTAINS.getValue());
        runner.enqueue(writeParquet(AVRO_SCHEMA_EXTRA_COLUMN, CompressionCodecName.SNAPPY, 5));
        runner.run();

        runner.assertAllFlowFilesTransferred(ValidateParquet.REL_VALID, 1);
    }

    @Test
    public void testAttributeSchemaIsUsedWhenPresent() throws IOException {
        // property would reject this file; the attribute's schema accepts it
        runner.setProperty(ValidateParquet.SCHEMA, "message event { required int64 totally_different; }");
        runner.setProperty(ValidateParquet.SCHEMA_ATTRIBUTE, "parquet.schema");
        runner.enqueue(writeParquet(AVRO_SCHEMA, CompressionCodecName.SNAPPY, 5),
                Collections.singletonMap("parquet.schema", EXPECTED_SCHEMA));
        runner.run();

        runner.assertAllFlowFilesTransferred(ValidateParquet.REL_VALID, 1);
    }

    @Test
    public void testFallsBackToPropertyWhenAttributeMissing() throws IOException {
        runner.setProperty(ValidateParquet.SCHEMA_ATTRIBUTE, "parquet.schema");
        runner.enqueue(writeParquet(AVRO_SCHEMA, CompressionCodecName.SNAPPY, 5));
        runner.run();

        runner.assertAllFlowFilesTransferred(ValidateParquet.REL_VALID, 1);
    }

    @Test
    public void testFallsBackToPropertyWhenAttributeIsBlank() throws IOException {
        runner.setProperty(ValidateParquet.SCHEMA_ATTRIBUTE, "parquet.schema");
        runner.enqueue(writeParquet(AVRO_SCHEMA, CompressionCodecName.SNAPPY, 5),
                Collections.singletonMap("parquet.schema", "   "));
        runner.run();

        runner.assertAllFlowFilesTransferred(ValidateParquet.REL_VALID, 1);
    }

    @Test
    public void testFallbackStillRejectsNonConformingFile() throws IOException {
        // the fallback must actually be applied, not just accepted blindly
        runner.setProperty(ValidateParquet.SCHEMA, "message event { required int64 totally_different; }");
        runner.setProperty(ValidateParquet.SCHEMA_ATTRIBUTE, "parquet.schema");
        runner.enqueue(writeParquet(AVRO_SCHEMA, CompressionCodecName.SNAPPY, 5));
        runner.run();

        runner.assertAllFlowFilesTransferred(ValidateParquet.REL_INVALID, 1);
        assertDetailContains("totally_different");
    }

    @Test
    public void testMalformedAttributeSchemaIsInvalidRatherThanFallingBack() throws IOException {
        runner.setProperty(ValidateParquet.SCHEMA_ATTRIBUTE, "parquet.schema");
        runner.enqueue(writeParquet(AVRO_SCHEMA, CompressionCodecName.SNAPPY, 5),
                Collections.singletonMap("parquet.schema", "message broken { not a schema }"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ValidateParquet.REL_INVALID, 1);
        assertDetailContains("not a valid Parquet message type");
    }

    @Test
    public void testAttributeIgnoredWhenAttributeNameNotConfigured() throws IOException {
        // no Schema Attribute Name set, so a stray attribute must not be picked up
        runner.enqueue(writeParquet(AVRO_SCHEMA, CompressionCodecName.SNAPPY, 5),
                Collections.singletonMap("parquet.schema", "message event { required int64 nope; }"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ValidateParquet.REL_VALID, 1);
    }

    @Test
    public void testNonParquetContentIsInvalid() {
        runner.enqueue("this is definitely not a parquet file");
        runner.run();

        runner.assertAllFlowFilesTransferred(ValidateParquet.REL_INVALID, 1);
        assertDetailContains("not a valid Parquet file");
    }

    @Test
    public void testTruncatedFileIsInvalid() throws IOException {
        final byte[] good = writeParquet(AVRO_SCHEMA, CompressionCodecName.SNAPPY, 100);
        runner.enqueue(Arrays.copyOf(good, good.length - 40));
        runner.run();

        runner.assertAllFlowFilesTransferred(ValidateParquet.REL_INVALID, 1);
    }

    @Test
    public void testCorruptDataPageIsInvalidWithFullDepth() throws IOException {
        runner.enqueue(corruptDataPage(writeParquet(AVRO_SCHEMA, CompressionCodecName.SNAPPY, 100)));
        runner.run();

        runner.assertAllFlowFilesTransferred(ValidateParquet.REL_INVALID, 1);
    }

    @Test
    public void testCorruptDataPagePassesSchemaOnlyDepth() throws IOException {
        // documents the trade-off: schema-only cannot see data page corruption
        runner.setProperty(ValidateParquet.VALIDATION_DEPTH, ValidateParquet.DEPTH_SCHEMA_ONLY.getValue());
        runner.enqueue(corruptDataPage(writeParquet(AVRO_SCHEMA, CompressionCodecName.SNAPPY, 100)));
        runner.run();

        runner.assertAllFlowFilesTransferred(ValidateParquet.REL_VALID, 1);
    }

    @Test
    public void testAllCodecs() throws IOException {
        for (final CompressionCodecName codec : new CompressionCodecName[] {
                CompressionCodecName.UNCOMPRESSED, CompressionCodecName.SNAPPY,
                CompressionCodecName.GZIP, CompressionCodecName.ZSTD}) {
            runner.clearTransferState();
            runner.enqueue(writeParquet(AVRO_SCHEMA, codec, 50));
            runner.run();
            runner.assertAllFlowFilesTransferred(ValidateParquet.REL_VALID, 1);
        }
    }

    private void assertDetailContains(final String fragment) {
        final MockFlowFile out = runner.getFlowFilesForRelationship(ValidateParquet.REL_INVALID).get(0);
        final String detail = out.getAttribute(ValidateParquet.DETAIL_ATTRIBUTE);
        assertTrue(detail != null && detail.contains(fragment),
                "expected detail containing '" + fragment + "' but was: " + detail);
    }

    private static byte[] writeParquet(final String avroSchemaJson, final CompressionCodecName codec,
            final int rows) throws IOException {
        final Schema schema = new Schema.Parser().parse(avroSchemaJson);
        final InMemoryOutputFile out = new InMemoryOutputFile();
        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(out)
                .withSchema(schema)
                .withConf(new Configuration())
                .withCompressionCodec(codec)
                .build()) {
            for (int i = 0; i < rows; i++) {
                final GenericRecord record = new GenericData.Record(schema);
                record.put("id", (long) i);
                record.put("name", "row number " + i);
                if (schema.getField("extra") != null) {
                    record.put("extra", i);
                }
                writer.write(record);
            }
        }
        return out.toByteArray();
    }

    /** Flips a byte in the data page region (after the 4-byte magic, well before the footer). */
    private static byte[] corruptDataPage(final byte[] file) {
        final byte[] corrupted = file.clone();
        corrupted[100] ^= (byte) 0xFF;
        return corrupted;
    }

    private static class InMemoryOutputFile implements OutputFile {
        private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();

        byte[] toByteArray() {
            return buffer.toByteArray();
        }

        @Override
        public PositionOutputStream create(final long blockSizeHint) {
            return createOrOverwrite(blockSizeHint);
        }

        @Override
        public PositionOutputStream createOrOverwrite(final long blockSizeHint) {
            buffer.reset();
            return new PositionOutputStream() {
                private long pos = 0;

                @Override
                public long getPos() {
                    return pos;
                }

                @Override
                public void write(final int b) {
                    buffer.write(b);
                    pos++;
                }

                @Override
                public void write(final byte[] b, final int off, final int len) {
                    buffer.write(b, off, len);
                    pos += len;
                }
            };
        }

        @Override
        public boolean supportsBlockSize() {
            return false;
        }

        @Override
        public long defaultBlockSize() {
            return 0;
        }
    }

}
