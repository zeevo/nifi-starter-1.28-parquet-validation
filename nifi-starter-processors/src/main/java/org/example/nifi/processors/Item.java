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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.avro.generic.GenericRecord;

/**
 * One item out of a Parquet file: the id, name and status that ValidateParquet's rules work on,
 * plus the optional key column.
 *
 * <p>This is deliberately not a mapping of the whole file. A Parquet file may carry any number of
 * other columns and they are ignored, so adding a column upstream does not affect validation.
 *
 * <p>Projecting here keeps two pieces of Avro trivia out of the rules: string columns arrive as
 * Utf8 rather than String, and an id column may be INT32 or INT64. The rules then read as rules.
 *
 * <p>Java 11 has no record keyword, hence the hand-written immutable class.
 */
final class Item {

    static final String ID = "id";
    static final String NAME = "name";
    static final String STATUS = "status";
    static final String KEY = "key";

    /** The fields the schema must declare before any item can be read out of it. */
    static final List<String> FIELDS = Collections.unmodifiableList(Arrays.asList(ID, NAME, STATUS));

    private final Long id;
    private final String name;
    private final String status;
    private final boolean idNonNumeric;
    private final List<Key> keys;

    private Item(final Long id, final String name, final String status, final boolean idNonNumeric,
            final List<Key> keys) {
        this.id = id;
        this.name = name;
        this.status = status;
        this.idNonNumeric = idNonNumeric;
        this.keys = keys;
    }

    /**
     * Reads one item out of a record. Only safe once the schema is known to declare every field in
     * {@link #FIELDS}, because GenericRecord.get throws AvroRuntimeException rather than returning
     * null for a field the schema does not declare.
     */
    static Item from(final GenericRecord record) {
        final Object rawId = record.get(ID);
        return new Item(
                rawId instanceof Number ? ((Number) rawId).longValue() : null,
                text(record, NAME),
                text(record, STATUS),
                rawId != null && !(rawId instanceof Number),
                keys(record));
    }

    /**
     * The values of the key column, in file order. Empty when the schema does not declare the
     * column at all and when it declares it but the row holds null, since neither gives the rules
     * anything to check.
     *
     * <p>A single value and an array of them both arrive here as a list, so a rule does not have to
     * ask which shape the column had. That is the only thing flattened: each value keeps the form
     * the file used, text or binary.
     */
    private static List<Key> keys(final GenericRecord record) {
        // The record's own schema, not the file's, decides what get() may be asked for: it throws
        // AvroRuntimeException for a field the schema does not declare. Unlike id, name and status
        // the key column is optional, so a file without one reads exactly as it did before.
        if (record.getSchema().getField(KEY) == null) {
            return Collections.emptyList();
        }

        final Object value = record.get(KEY);
        if (value == null) {
            return Collections.emptyList();
        }
        if (value instanceof Collection) {
            // GenericData.Array for an array column, whichever list encoding the file used.
            final List<Key> keys = new ArrayList<>();
            for (final Object element : (Collection<?>) value) {
                keys.add(Key.of(element));
            }
            return Collections.unmodifiableList(keys);
        }
        return Collections.singletonList(Key.of(value));
    }

    /** Null when the column was null, and also when it held something that is not a number. */
    Long id() {
        return id;
    }

    String name() {
        return name;
    }

    String status() {
        return status;
    }

    /** True when the id column held a non-numeric value, for instance a string typed column. */
    boolean idNonNumeric() {
        return idNonNumeric;
    }

    /** Every value the key column held, empty when it held none. Never null. */
    List<Key> keys() {
        return keys;
    }

    /**
     * Avro hands back Utf8 rather than String unless the writer set avro.java.string, which files
     * from Spark, pyarrow and DuckDB do not, so casting to String would fail on most real files.
     */
    private static String text(final GenericRecord record, final String field) {
        final Object value = record.get(field);
        return value == null ? null : value.toString();
    }

    @Override
    public String toString() {
        return "Item[id=" + id + ", name=" + name + ", status=" + status + ", keys=" + keys + "]";
    }
}
