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

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

/**
 * A Parquet {@link InputFile} backed entirely by a byte array.
 *
 * <p>Parquet stores its footer at the end of the file and seeks backwards to it as soon as a
 * reader is opened, so reading requires random access. parquet-java ships no in-memory
 * implementation: {@code LocalInputFile} opens a {@code RandomAccessFile}, which would mean
 * writing the FlowFile out to disk first.
 */
final class ByteArrayInputFile implements InputFile {

    private final byte[] data;
    private final String name;

    /**
     * @param name identifies the file in parquet's own error messages, which are surfaced to
     *             users as NiFi bulletins, so a FlowFile UUID belongs here
     */
    ByteArrayInputFile(final byte[] data, final String name) {
        this.data = data;
        this.name = name;
    }

    @Override
    public long getLength() {
        return data.length;
    }

    @Override
    public SeekableInputStream newStream() {
        // DelegatingSeekableInputStream already implements the ByteBuffer and readFully methods
        // correctly, leaving only getPos and seek.
        final CursorStream cursor = new CursorStream(data);
        return new DelegatingSeekableInputStream(cursor) {
            @Override
            public long getPos() {
                return cursor.position();
            }

            @Override
            public void seek(final long newPos) throws IOException {
                if (newPos < 0 || newPos > data.length) {
                    throw new IOException("Invalid seek position " + newPos + " for " + name);
                }
                cursor.position((int) newPos);
            }
        };
    }

    @Override
    public String toString() {
        return name;
    }

    /**
     * ByteArrayInputStream already tracks the cursor we need; this exposes its protected field so
     * seek is a plain assignment rather than a reset-and-skip.
     */
    private static final class CursorStream extends ByteArrayInputStream {

        CursorStream(final byte[] buf) {
            super(buf);
        }

        int position() {
            return this.pos;
        }

        void position(final int newPos) {
            this.pos = newPos;
        }
    }

}
