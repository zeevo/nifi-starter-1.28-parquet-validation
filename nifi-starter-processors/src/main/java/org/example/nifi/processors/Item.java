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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.avro.generic.GenericRecord;

/**
 * One item out of a Parquet file: the id, name and status that ValidateParquet's rules work on.
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

    /** The fields the schema must declare before any item can be read out of it. */
    static final List<String> FIELDS = Collections.unmodifiableList(Arrays.asList(ID, NAME, STATUS));

    private static final Set<String> ALLOWED_STATUSES = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList("ACTIVE", "INACTIVE", "PENDING")));

    private final Long id;
    private final String name;
    private final String status;
    private final boolean idNonNumeric;

    private Item(final Long id, final String name, final String status, final boolean idNonNumeric) {
        this.id = id;
        this.name = name;
        this.status = status;
        this.idNonNumeric = idNonNumeric;
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
                rawId != null && !(rawId instanceof Number));
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

    /**
     * The business rules, hardcoded on purpose. They live here rather than on a processor so that
     * validating a file and filtering one cannot drift apart: both ask the item the same question.
     *
     * @return the first rule this item breaks, or null if it passes
     */
    String violation() {
        if (idNonNumeric) {
            // A string or binary id column would otherwise look like a null id. Calling it out
            // separately keeps a schema problem from being reported as a missing value.
            return "id is not numeric";
        }
        if (id == null) {
            return "id is null";
        }
        if (id <= 0) {
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

    /** Convenience for the filtering path, which does not care which rule was broken. */
    boolean isValid() {
        return violation() == null;
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
        return "Item[id=" + id + ", name=" + name + ", status=" + status + "]";
    }
}
