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

import java.util.function.ToLongFunction;

import org.apache.parquet.VersionParser;
import org.apache.parquet.VersionParser.ParsedVersion;
import org.apache.parquet.VersionParser.VersionParseException;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.impl.ColumnReaderImpl;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;

/**
 * Walks the id, name and status columns of one row group side by side, yielding an {@link Item}
 * per row.
 *
 * <p>This is parquet's record assembly done by hand, and only the part an Item needs. A row is the
 * same position in three columns, so there is no record to build and no state machine to run, which
 * is the idea behind Spark's vectorized reader. Nothing passes through Avro either, whose schema
 * conversion throws on INT96 timestamps and on column names such as "unit price".
 *
 * <p>The column readers decide what a value means, which keeps the type trivia out of the rules:
 * any numeric id is a number and anything else is non-numeric, while a name or status is its value
 * as text. The rules then read as rules.
 */
final class ItemColumns {

    /** Stands in for a struct, list or map where text was expected. It is present, not blank. */
    private static final String GROUP_TEXT = "<group>";

    private final Column id;
    private final Column name;
    private final Column status;

    private final ToLongFunction<ColumnReader> idReader;
    private final TextConverter nameText;
    private final TextConverter statusText;

    ItemColumns(final PageReadStore rowGroup, final MessageType projection, final String createdBy) {
        final ParsedVersion writerVersion = writerVersion(createdBy);
        nameText = new TextConverter(projection.getType(Item.NAME));
        statusText = new TextConverter(projection.getType(Item.STATUS));
        id = new Column(Item.ID, rowGroup, projection, writerVersion, new PrimitiveConverter() { });
        name = new Column(Item.NAME, rowGroup, projection, writerVersion, nameText);
        status = new Column(Item.STATUS, rowGroup, projection, writerVersion, statusText);
        idReader = id.scalar ? idReader(id.reader.getDescriptor().getPrimitiveType().getPrimitiveTypeName()) : null;
    }

    /** Must be called exactly once per row in the row group. */
    Item next() {
        Long idValue = null;
        boolean idNonNumeric = false;
        if (id.present()) {
            if (idReader == null) {
                idNonNumeric = true;
            } else {
                idValue = idReader.applyAsLong(id.reader);
            }
        }
        final Item item = new Item(idValue, text(name, nameText), text(status, statusText), idNonNumeric);
        id.advance();
        name.advance();
        status.advance();
        return item;
    }

    private static String text(final Column column, final TextConverter converter) {
        if (!column.present()) {
            return null;
        }
        if (!column.scalar) {
            return GROUP_TEXT;
        }
        column.reader.writeCurrentValueToConverter();
        return converter.value;
    }

    /** Null for a type that is not a number. */
    private static ToLongFunction<ColumnReader> idReader(final PrimitiveTypeName type) {
        switch (type) {
            case INT32:
                return ColumnReader::getInteger;
            case INT64:
                return ColumnReader::getLong;
            case FLOAT:
                return reader -> (long) reader.getFloat();
            case DOUBLE:
                return reader -> (long) reader.getDouble();
            default:
                return null;
        }
    }

    /** Null is what ColumnReadStoreImpl falls back to as well, and it only makes reads stricter. */
    private static ParsedVersion writerVersion(final String createdBy) {
        try {
            return VersionParser.parse(createdBy);
        } catch (final RuntimeException | VersionParseException e) {
            return null;
        }
    }

    /**
     * One top-level field. A nested field has several leaf columns, but any of them shows whether
     * the field is null in a row, so only the first is read and its values are never looked at.
     */
    private static final class Column {

        private final ColumnReader reader;
        private final boolean scalar;
        private final int presentLevel;

        Column(final String field, final PageReadStore rowGroup, final MessageType projection,
                final ParsedVersion writerVersion, final PrimitiveConverter converter) {
            final Type type = projection.getType(field);
            final ColumnDescriptor leaf = firstLeaf(field, projection);
            scalar = type.isPrimitive() && !type.isRepetition(Repetition.REPEATED);
            // The field is top-level, so one definition level is all an optional field can add.
            presentLevel = type.isRepetition(Repetition.REQUIRED) ? 0 : 1;
            reader = new ColumnReaderImpl(leaf, rowGroup.getPageReader(leaf), converter, writerVersion);
        }

        private static ColumnDescriptor firstLeaf(final String field, final MessageType projection) {
            for (final ColumnDescriptor column : projection.getColumns()) {
                if (column.getPath()[0].equals(field)) {
                    return column;
                }
            }
            throw new IllegalArgumentException("no column for " + field);
        }

        boolean present() {
            return reader.getCurrentDefinitionLevel() >= presentLevel;
        }

        /** A repeated field holds many values in one row, and level 0 starts the next row. */
        void advance() {
            do {
                reader.consume();
            } while (reader.getCurrentRepetitionLevel() > 0);
        }
    }

    /**
     * Turns a value into text, whatever its type. Strings are usually dictionary encoded, and a
     * status column may hold three values across millions of rows, so each dictionary entry is
     * decoded at most once and shared. Taking dictionary ids needs a converter rather than the
     * column reader's getters, which would decode a String per row.
     */
    private static final class TextConverter extends PrimitiveConverter {

        private final boolean binary;

        private Dictionary dictionary;
        private String[] decoded;
        private String value;

        TextConverter(final Type type) {
            binary = type.isPrimitive()
                    && (type.asPrimitiveType().getPrimitiveTypeName() == PrimitiveTypeName.BINARY
                    || type.asPrimitiveType().getPrimitiveTypeName() == PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY);
        }

        @Override
        public boolean hasDictionarySupport() {
            return binary;
        }

        @Override
        public void setDictionary(final Dictionary dictionary) {
            this.dictionary = dictionary;
            decoded = new String[dictionary.getMaxId() + 1];
        }

        @Override
        public void addValueFromDictionary(final int dictionaryId) {
            value = decoded[dictionaryId];
            if (value == null) {
                value = dictionary.decodeToBinary(dictionaryId).toStringUsingUTF8();
                decoded[dictionaryId] = value;
            }
        }

        @Override
        public void addBinary(final Binary binary) {
            value = binary.toStringUsingUTF8();
        }

        @Override
        public void addInt(final int number) {
            value = Integer.toString(number);
        }

        @Override
        public void addLong(final long number) {
            value = Long.toString(number);
        }

        @Override
        public void addFloat(final float number) {
            value = Float.toString(number);
        }

        @Override
        public void addDouble(final double number) {
            value = Double.toString(number);
        }

        @Override
        public void addBoolean(final boolean bool) {
            value = Boolean.toString(bool);
        }
    }
}
