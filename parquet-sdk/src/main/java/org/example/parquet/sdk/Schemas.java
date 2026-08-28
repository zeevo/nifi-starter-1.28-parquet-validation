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
package org.example.parquet.sdk;

import java.util.Arrays;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

/**
 * The schemas the samples are written with.
 *
 * <p>These are Avro schemas, not Parquet ones. {@code parquet-avro} converts an Avro schema into a
 * Parquet {@code MessageType} when it writes, and stores the original Avro JSON in the footer under
 * {@code parquet.avro.schema} so a reader can reproduce the exact Avro types. Running the generator
 * prints both, which is the clearest way to see how one maps onto the other.
 *
 * <p>Two things trip people up and are worth watching for in the output:
 *
 * <ul>
 *   <li>A nullable Avro field is a <em>union</em> of null and the type. That is what makes the
 *       Parquet column {@code optional} rather than {@code required}.</li>
 *   <li>Avro logical types (decimal, date, timestamp, uuid) are annotations on top of a primitive.
 *       In Parquet they come out as a physical type plus a logical type annotation, which is why a
 *       date is an {@code INT32} and a decimal can be a {@code FIXED_LEN_BYTE_ARRAY}.</li>
 * </ul>
 */
public final class Schemas {

    private Schemas() {
    }

    /** Every Parquet physical type, each one required so none of them is a union. */
    public static Schema primitives() {
        return SchemaBuilder.record("Primitives").namespace("org.example.parquet.sdk").fields()
                .requiredBoolean("a_boolean")
                .requiredInt("an_int")        // INT32
                .requiredLong("a_long")       // INT64
                .requiredFloat("a_float")     // FLOAT
                .requiredDouble("a_double")   // DOUBLE
                .requiredString("a_string")   // BINARY annotated as UTF8
                .requiredBytes("some_bytes")  // BINARY with no annotation
                .endRecord();
    }

    /** The same idea, but every field optional, so the columns carry definition levels. */
    public static Schema nullable() {
        return SchemaBuilder.record("Nullable").namespace("org.example.parquet.sdk").fields()
                .optionalLong("id")
                .optionalString("name")
                .optionalString("status")
                .optionalDouble("score")
                .endRecord();
    }

    /**
     * A record inside a record. Parquet stores this as a group, and the leaf columns are named by
     * their full path, which is why the summary prints things like {@code address.city}.
     */
    public static Schema nested() {
        return SchemaBuilder.record("Customer").namespace("org.example.parquet.sdk").fields()
                .requiredLong("id")
                .requiredString("name")
                .name("address").type(
                        SchemaBuilder.record("Address").namespace("org.example.parquet.sdk").fields()
                                .requiredString("street")
                                .requiredString("city")
                                .optionalString("postcode")
                                .endRecord())
                .noDefault()
                .endRecord();
    }

    /**
     * Repeated and keyed data. Both become three-level groups in Parquet, which is where repetition
     * levels come from; the summary shows the leaf paths those expand into.
     */
    public static Schema collections() {
        return SchemaBuilder.record("Order").namespace("org.example.parquet.sdk").fields()
                .requiredLong("id")
                .name("tags").type().array().items().stringType().noDefault()
                .name("quantities").type().map().values().intType().noDefault()
                .endRecord();
    }

    /**
     * Logical types: a physical type plus an annotation telling readers what it means.
     *
     * <p>SchemaBuilder has no fluent form for these, so each is built by applying a
     * {@link LogicalTypes} annotation to the underlying primitive schema.
     */
    public static Schema logicalTypes() {
        final Schema date = LogicalTypes.date()
                .addToSchema(Schema.create(Schema.Type.INT));
        final Schema timestampMillis = LogicalTypes.timestampMillis()
                .addToSchema(Schema.create(Schema.Type.LONG));
        // A decimal on bytes: precision 9, scale 2, so values like 1234567.89.
        final Schema decimal = LogicalTypes.decimal(9, 2)
                .addToSchema(Schema.create(Schema.Type.BYTES));
        final Schema uuid = LogicalTypes.uuid()
                .addToSchema(Schema.create(Schema.Type.STRING));
        final Schema colour = Schema.createEnum("Colour", null, "org.example.parquet.sdk",
                Arrays.asList("RED", "GREEN", "BLUE"));

        return Schema.createRecord("LogicalTypes", null, "org.example.parquet.sdk", false,
                Arrays.asList(
                        new Schema.Field("event_date", date, "days since 1970-01-01", null),
                        new Schema.Field("event_time", timestampMillis, "millis since the epoch", null),
                        new Schema.Field("amount", decimal, "fixed point, scale 2", null),
                        new Schema.Field("correlation_id", uuid, null, null),
                        new Schema.Field("colour", colour, null, null)));
    }

    /** A wide-ish row used for the encoding and row group samples. */
    public static Schema events() {
        return SchemaBuilder.record("Event").namespace("org.example.parquet.sdk").fields()
                .requiredLong("id")
                .requiredString("category")   // few distinct values, so a dictionary pays off
                .requiredString("payload")    // every value distinct, so it does not
                .requiredDouble("measurement")
                .endRecord();
    }
}
