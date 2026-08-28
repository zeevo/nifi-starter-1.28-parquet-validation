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

import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.avro.AvroWriteSupport;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.api.RecordConsumer;

/**
 * A Parquet writer whose footer carries exactly the file-level metadata it is given, and nothing
 * else.
 *
 * <p>{@code ParquetWriter.Builder.withExtraMetaData} cannot do this. It merges what you pass with
 * what the write support emits, throws {@code IllegalArgumentException} if the two collide on
 * {@code parquet.avro.schema}, and always appends {@code writer.model.name}. Copying a file that
 * never had those keys therefore gains them, which makes "the same metadata as the input"
 * impossible to express.
 *
 * <p>The metadata a Parquet file ends up with is whatever {@link WriteSupport#init} returns in its
 * {@code WriteContext}, plus {@code writer.model.name} taken from {@link WriteSupport#getName()}.
 * So this delegates every part of writing to {@link AvroWriteSupport} and overrides just those two:
 * the context carries the caller's map verbatim, and the name is null so no model key is added.
 */
final class ExactMetadataParquetWriter extends ParquetWriter.Builder<GenericRecord, ExactMetadataParquetWriter> {

    private final Schema avroSchema;
    private final Map<String, String> metadata;

    ExactMetadataParquetWriter(final OutputFile file, final Schema avroSchema,
            final Map<String, String> metadata) {
        super(file);
        this.avroSchema = avroSchema;
        this.metadata = metadata;
    }

    @Override
    protected ExactMetadataParquetWriter self() {
        return this;
    }

    @Override
    protected WriteSupport<GenericRecord> getWriteSupport(final Configuration conf) {
        return new ExactMetadata(new AvroWriteSupport<>(
                new AvroSchemaConverter(conf).convert(avroSchema), avroSchema, GenericData.get()));
    }

    @Override
    protected WriteSupport<GenericRecord> getWriteSupport(final ParquetConfiguration conf) {
        return new ExactMetadata(new AvroWriteSupport<>(
                new AvroSchemaConverter(conf).convert(avroSchema), avroSchema, GenericData.get()));
    }

    /** Avro's write support in every respect except which key/value pairs reach the footer. */
    private final class ExactMetadata extends WriteSupport<GenericRecord> {

        private final WriteSupport<GenericRecord> delegate;

        ExactMetadata(final WriteSupport<GenericRecord> delegate) {
            this.delegate = delegate;
        }

        @Override
        public WriteContext init(final Configuration conf) {
            return new WriteContext(delegate.init(conf).getSchema(), metadata);
        }

        @Override
        public WriteContext init(final ParquetConfiguration conf) {
            return new WriteContext(delegate.init(conf).getSchema(), metadata);
        }

        /** Null suppresses the writer.model.name entry parquet would otherwise add. */
        @Override
        public String getName() {
            return null;
        }

        @Override
        public void prepareForWrite(final RecordConsumer recordConsumer) {
            delegate.prepareForWrite(recordConsumer);
        }

        @Override
        public void write(final GenericRecord record) {
            delegate.write(record);
        }

        @Override
        public FinalizedWriteContext finalizeWrite() {
            return delegate.finalizeWrite();
        }
    }
}
