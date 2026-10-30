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

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;

/**
 * Reads only the columns an {@link Item} needs, straight into an Item.
 *
 * <p>Every other column chunk stays undecoded, and nothing passes through Avro. That matters beyond
 * the allocation it saves: converting a schema to Avro throws on the INT96 timestamps Spark writes
 * by default and on column names such as "unit price", so a file would otherwise be unreadable
 * because of columns we ignore. The projection uses the file's own types, so it also holds whether
 * a column is optional, required, INT32 or INT64.
 *
 * <p>The converters decide what a column means, which keeps the type trivia out of the rules: any
 * numeric id is a number and anything else is non-numeric, while a name or status is its value as
 * text. The rules then read as rules.
 */
final class ItemReadSupport extends ReadSupport<Item> {

    /** Stands in for a struct, list or map where text was expected. It is present, not blank. */
    private static final String GROUP_TEXT = "<group>";

    @Override
    public ReadContext init(final InitContext context) {
        return new ReadContext(Item.projection(context.getFileSchema()));
    }

    @Override
    public RecordMaterializer<Item> prepareForRead(final ParquetConfiguration configuration,
            final Map<String, String> keyValueMetaData, final MessageType fileSchema, final ReadContext readContext) {
        final ItemConverter converter = new ItemConverter(readContext.getRequestedSchema());
        return new RecordMaterializer<>() {
            @Override
            public Item getCurrentRecord() {
                return converter.item;
            }

            @Override
            public GroupConverter getRootConverter() {
                return converter;
            }
        };
    }

    /** Never called, since {@link ValidateParquet} always builds with a ParquetConfiguration. */
    @Override
    public RecordMaterializer<Item> prepareForRead(final Configuration configuration,
            final Map<String, String> keyValueMetaData, final MessageType fileSchema, final ReadContext readContext) {
        throw new UnsupportedOperationException();
    }

    /** Fields arrive in projection order, which is the order of {@link Item#FIELDS}. */
    private static final class ItemConverter extends GroupConverter {

        private final Converter id;
        private final Converter name;
        private final Converter status;

        private Long idValue;
        private boolean idNonNumeric;
        private String nameValue;
        private String statusValue;
        private Item item;

        ItemConverter(final MessageType projection) {
            id = idConverter(projection.getType(Item.ID));
            name = textConverter(projection.getType(Item.NAME), value -> nameValue = value);
            status = textConverter(projection.getType(Item.STATUS), value -> statusValue = value);
        }

        private Converter idConverter(final Type type) {
            if (!type.isPrimitive()) {
                return new PresenceConverter(type.asGroupType(), () -> idNonNumeric = true);
            }
            return new PrimitiveConverter() {
                @Override
                public void addInt(final int value) {
                    idValue = (long) value;
                }

                @Override
                public void addLong(final long value) {
                    idValue = value;
                }

                @Override
                public void addFloat(final float value) {
                    idValue = (long) value;
                }

                @Override
                public void addDouble(final double value) {
                    idValue = (long) value;
                }

                @Override
                public void addBoolean(final boolean value) {
                    idNonNumeric = true;
                }

                @Override
                public void addBinary(final Binary value) {
                    idNonNumeric = true;
                }
            };
        }

        private static Converter textConverter(final Type type, final TextSink sink) {
            if (!type.isPrimitive()) {
                return new PresenceConverter(type.asGroupType(), () -> sink.accept(GROUP_TEXT));
            }
            return new TextConverter(type.asPrimitiveType().getPrimitiveTypeName(), sink);
        }

        @Override
        public Converter getConverter(final int fieldIndex) {
            switch (fieldIndex) {
                case 0:
                    return id;
                case 1:
                    return name;
                default:
                    return status;
            }
        }

        @Override
        public void start() {
            idValue = null;
            idNonNumeric = false;
            nameValue = null;
            statusValue = null;
        }

        @Override
        public void end() {
            item = new Item(idValue, nameValue, statusValue, idNonNumeric);
        }
    }

    private interface TextSink {
        void accept(String value);
    }

    /**
     * Turns a column into text, whatever its type. Strings are usually dictionary encoded, and a
     * status column may hold three values across millions of rows, so each dictionary entry is
     * decoded at most once and shared.
     */
    private static final class TextConverter extends PrimitiveConverter {

        private final PrimitiveTypeName type;
        private final TextSink sink;

        private Dictionary dictionary;
        private String[] decoded;

        TextConverter(final PrimitiveTypeName type, final TextSink sink) {
            this.type = type;
            this.sink = sink;
        }

        @Override
        public boolean hasDictionarySupport() {
            return type == PrimitiveTypeName.BINARY || type == PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
        }

        @Override
        public void setDictionary(final Dictionary dictionary) {
            this.dictionary = dictionary;
            decoded = new String[dictionary.getMaxId() + 1];
        }

        @Override
        public void addValueFromDictionary(final int dictionaryId) {
            String value = decoded[dictionaryId];
            if (value == null) {
                value = dictionary.decodeToBinary(dictionaryId).toStringUsingUTF8();
                decoded[dictionaryId] = value;
            }
            sink.accept(value);
        }

        @Override
        public void addBinary(final Binary value) {
            sink.accept(value.toStringUsingUTF8());
        }

        @Override
        public void addInt(final int value) {
            sink.accept(Integer.toString(value));
        }

        @Override
        public void addLong(final long value) {
            sink.accept(Long.toString(value));
        }

        @Override
        public void addFloat(final float value) {
            sink.accept(Float.toString(value));
        }

        @Override
        public void addDouble(final double value) {
            sink.accept(Double.toString(value));
        }

        @Override
        public void addBoolean(final boolean value) {
            sink.accept(Boolean.toString(value));
        }
    }

    /** Reports that a nested column is not null, and ignores what is inside it. */
    private static final class PresenceConverter extends GroupConverter {

        private final Runnable onPresent;
        private final Converter[] fields;

        PresenceConverter(final GroupType type, final Runnable onPresent) {
            this.onPresent = onPresent;
            fields = new Converter[type.getFieldCount()];
            for (int i = 0; i < fields.length; i++) {
                final Type field = type.getType(i);
                fields[i] = field.isPrimitive()
                        ? new IgnoringConverter()
                        : new PresenceConverter(field.asGroupType(), () -> { });
            }
        }

        @Override
        public Converter getConverter(final int fieldIndex) {
            return fields[fieldIndex];
        }

        @Override
        public void start() {
            onPresent.run();
        }

        @Override
        public void end() {
        }
    }

    /** The default PrimitiveConverter throws on every value. */
    private static final class IgnoringConverter extends PrimitiveConverter {

        @Override
        public void addBinary(final Binary value) {
        }

        @Override
        public void addBoolean(final boolean value) {
        }

        @Override
        public void addDouble(final double value) {
        }

        @Override
        public void addFloat(final float value) {
        }

        @Override
        public void addInt(final int value) {
        }

        @Override
        public void addLong(final long value) {
        }
    }
}
