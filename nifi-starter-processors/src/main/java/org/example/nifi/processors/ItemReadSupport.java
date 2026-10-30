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

import java.util.Collections;
import java.util.Map;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

/**
 * Reads only the columns an {@link Item} needs, leaving every other column chunk undecoded.
 *
 * <p>parquet-avro's own projection setting is not used because it takes an Avro schema, and the
 * round trip through Avro is lossy: a 3-level Spark list comes back as a 2-level one and no longer
 * matches the file. Handing parquet the file's own types has no such problem, and works whether a
 * column is optional, required, INT32 or INT64.
 *
 * <p>Only the projection is converted to Avro, never the whole file schema. That conversion throws
 * on the INT96 timestamps Spark writes by default, and on column names Avro rejects such as
 * "unit price", so a file would otherwise be unreadable because of columns we ignore.
 */
final class ItemReadSupport extends AvroReadSupport<GenericRecord> {

    ItemReadSupport() {
        // Short-circuits AvroReadSupport, which would otherwise build another Hadoop Configuration
        // just to resolve the data model.
        super(GenericData.get());
    }

    @Override
    public ReadContext init(final ParquetConfiguration configuration, final Map<String, String> keyValueMetaData,
            final MessageType fileSchema) {
        final ReadContext context = super.init(configuration, keyValueMetaData, fileSchema);
        return new ReadContext(Item.projection(fileSchema), context.getReadSupportMetadata());
    }

    /**
     * Drops the footer metadata so parquet-avro derives its Avro schema from the projection rather
     * than parsing one the file embeds, whose extra fields would only come back as nulls.
     */
    @Override
    public RecordMaterializer<GenericRecord> prepareForRead(final ParquetConfiguration configuration,
            final Map<String, String> keyValueMetaData, final MessageType fileSchema, final ReadContext readContext) {
        return super.prepareForRead(configuration, Collections.emptyMap(), fileSchema, readContext);
    }
}
