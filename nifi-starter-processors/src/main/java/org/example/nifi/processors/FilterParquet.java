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
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

@Tags({"parquet", "filter", "validate"})
@CapabilityDescription("Writes a copy of an incoming Parquet file with the rows that fail "
        + "ValidateParquet's rules left out. The copy has exactly the same Parquet schema and "
        + "carries every file-level key/value metadata entry of the input, except "
        + "writer.model.name, which parquet-java always sets itself. Compression and row group "
        + "sizing are the writer's defaults rather than the input's. Every column is carried "
        + "through: only rows are removed. Content is streamed in both directions, so memory use "
        + "does not grow with the size of the file.")
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
        @WritesAttribute(attribute = "mime.type", description = "application/parquet")
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

    /** Same reason as ValidateParquet: avoids a Hadoop Configuration per builder. */
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

        FlowFile filtered = null;
        try {
            final InputFile inputFile = new FlowFileInputFile(session, original);
            final FileMetaData footer = readFooter(inputFile);

            for (final String field : Item.FIELDS) {
                if (!footer.getSchema().containsField(field)) {
                    throw new IOException("Schema is missing the " + field + " field");
                }
            }

            // parquet-java refuses writer.model.name as extra metadata and writes its own, so it is
            // the one key that cannot be carried over. Everything else passes through verbatim.
            final Map<String, String> keyValues = new HashMap<>(footer.getKeyValueMetaData());
            keyValues.remove(ParquetWriter.OBJECT_MODEL_NAME_PROP);

            final long[] counts = new long[2];
            filtered = session.create(original);
            filtered = session.write(filtered, out -> {
                // Groups rather than Avro records on both sides, so the copy is written with the
                // input's own MessageType instead of one converted there and back through Avro.
                try (ParquetReader<Group> reader = new ParquetReader.Builder<Group>(inputFile, parquetConfiguration) {
                            @Override
                            protected ReadSupport<Group> getReadSupport() {
                                return new GroupReadSupport();
                            }
                        }.build();
                     ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile(out))
                             .withConf(parquetConfiguration)
                             .withType(footer.getSchema())
                             .withExtraMetaData(keyValues)
                             .build()) {

                    Group row;
                    while ((row = reader.read()) != null) {
                        counts[0]++;
                        if (ValidateParquet.validateItem(Item.from(row)) == null) {
                            writer.write(row);
                            counts[1]++;
                        }
                    }
                }
            });

            final long removed = counts[0] - counts[1];
            final Map<String, String> attributes = new HashMap<>();
            attributes.put(ROWS_READ_ATTRIBUTE, Long.toString(counts[0]));
            attributes.put(ROWS_KEPT_ATTRIBUTE, Long.toString(counts[1]));
            attributes.put(ROWS_REMOVED_ATTRIBUTE, Long.toString(removed));
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

    /** Reads the footer only. No column data is decompressed. */
    private FileMetaData readFooter(final InputFile inputFile) throws IOException {
        final ParquetReadOptions options = ParquetReadOptions.builder(parquetConfiguration).build();
        try (ParquetFileReader reader = ParquetFileReader.open(inputFile, options)) {
            return reader.getFileMetaData();
        }
    }

    /**
     * Writing Parquet never seeks, so an OutputFile over the session's stream only has to count
     * bytes. The session owns that stream, so close flushes rather than closes it.
     */
    private static OutputFile outputFile(final OutputStream out) {
        final PositionOutputStream stream = new PositionOutputStream() {
            private long position;

            @Override
            public long getPos() {
                return position;
            }

            @Override
            public void write(final int b) throws IOException {
                out.write(b);
                position++;
            }

            @Override
            public void write(final byte[] b, final int off, final int len) throws IOException {
                out.write(b, off, len);
                position += len;
            }

            @Override
            public void close() throws IOException {
                out.flush();
            }
        };

        return new OutputFile() {
            @Override
            public PositionOutputStream create(final long blockSizeHint) {
                return stream;
            }

            @Override
            public PositionOutputStream createOrOverwrite(final long blockSizeHint) {
                return stream;
            }

            @Override
            public boolean supportsBlockSize() {
                return false;
            }

            @Override
            public long defaultBlockSize() {
                return 0;
            }
        };
    }
}
