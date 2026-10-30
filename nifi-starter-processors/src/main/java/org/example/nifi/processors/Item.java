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
import java.util.Collections;
import java.util.List;

import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

/**
 * One item out of a Parquet file: the id, name and status that ValidateParquet's rules work on.
 *
 * <p>This is deliberately not a mapping of the whole file. A Parquet file may carry any number of
 * other columns and they are ignored, so adding a column upstream does not affect validation.
 *
 * <p>Java 11 has no record keyword, hence the hand-written immutable class.
 */
final class Item {

    static final String ID = "id";
    static final String NAME = "name";
    static final String STATUS = "status";

    /** The fields the schema must declare before any item can be read out of it. */
    static final List<String> FIELDS = Collections.unmodifiableList(Arrays.asList(ID, NAME, STATUS));

    private final Long id;
    private final String name;
    private final String status;
    private final boolean idNonNumeric;

    Item(final Long id, final String name, final String status, final boolean idNonNumeric) {
        this.id = id;
        this.name = name;
        this.status = status;
        this.idNonNumeric = idNonNumeric;
    }

    /**
     * The file's schema cut down to {@link #FIELDS}. Only safe once the schema is known to declare
     * every one of them, because MessageType.getType throws for a field it does not have.
     */
    static MessageType projection(final MessageType fileSchema) {
        final List<Type> fields = new ArrayList<>();
        for (final String field : FIELDS) {
            fields.add(fileSchema.getType(field));
        }
        return new MessageType(fileSchema.getName(), fields);
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

    @Override
    public String toString() {
        return "Item[id=" + id + ", name=" + name + ", status=" + status + "]";
    }
}
