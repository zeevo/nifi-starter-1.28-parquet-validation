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

import java.nio.ByteBuffer;

import org.apache.avro.generic.GenericFixed;

/**
 * One value of an item's key column, held in whichever form the file wrote it.
 *
 * <p>A key column may be text or raw binary, and a file is free to use either. The two forms are
 * kept apart rather than folded into one: turning 16 bytes into text, or text into bytes, would
 * invent a representation the file never contained. Sixteen bytes are not always UUID text, and
 * UUID text is not 16 bytes of UTF-8.
 *
 * <p>Which form arrives is decided by the schema, not by the physical type. A
 * {@code fixed_len_byte_array(16)} column annotated {@code UUID} comes back as canonical UUID
 * <em>text</em>, because parquet-avro renders it before this code sees it, while the same 16 bytes
 * without the annotation come back as binary. Both are keys; they are simply not the same form.
 *
 * <p>A value that is neither, such as a numeric column or a null inside an array, produces a Key
 * with neither field set. That is a state the rules report rather than quietly skip, for the same
 * reason Item keeps a non-numeric id apart from a null one.
 *
 * <p>Java 11 has no record keyword, hence the hand-written immutable class.
 */
final class Key {

    private final String text;
    private final byte[] bytes;

    private Key(final String text, final byte[] bytes) {
        this.text = text;
        this.bytes = bytes;
    }

    /**
     * Wraps one value read out of a key column.
     *
     * <p>The types come from parquet-avro's generic data model: {@code Utf8} for a string column,
     * {@code String} for a UUID annotated one, {@code GenericFixed} for a fixed width binary column
     * and {@code ByteBuffer} for a variable width one.
     */
    static Key of(final Object value) {
        if (value instanceof CharSequence) {
            return new Key(value.toString(), null);
        }
        if (value instanceof GenericFixed) {
            return new Key(null, ((GenericFixed) value).bytes());
        }
        if (value instanceof ByteBuffer) {
            // Read through a duplicate: consuming the original would leave the buffer empty for
            // anything else that looks at the same record.
            final ByteBuffer buffer = ((ByteBuffer) value).duplicate();
            final byte[] copy = new byte[buffer.remaining()];
            buffer.get(copy);
            return new Key(null, copy);
        }
        return new Key(null, null);
    }

    /** The value as text, or null when it did not arrive as text. */
    String text() {
        return text;
    }

    /**
     * The value as bytes, or null when it did not arrive as bytes. Handed back as held rather than
     * copied: the record it came from is discarded as soon as the item has been validated.
     */
    byte[] bytes() {
        return bytes;
    }

    boolean isText() {
        return text != null;
    }

    boolean isBinary() {
        return bytes != null;
    }

    @Override
    public String toString() {
        if (isText()) {
            return "Key[text=" + text + "]";
        }
        if (isBinary()) {
            return "Key[bytes=" + bytes.length + "]";
        }
        return "Key[neither]";
    }
}
