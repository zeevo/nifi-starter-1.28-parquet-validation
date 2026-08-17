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
 * A parquet-java {@link InputFile} served from a byte array.
 *
 * <p>Parquet keeps its footer at the end of the file, so a reader seeks backwards before it reads
 * anything else. A NiFi content stream is forward-only and staging to a temporary file is ruled
 * out here, so the FlowFile content is buffered and served from memory instead.
 *
 * <p>parquet-java has no in-memory InputFile of its own. {@code LocalInputFile} is
 * {@link java.nio.file.Path} based and opens a {@code RandomAccessFile}, which is exactly the
 * temporary file we are avoiding.
 */
final class ByteArrayInputFile implements InputFile {

    private final byte[] data;
    private final String name;

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
        // over the wrapped stream, so only getPos and seek are left to supply.
        final CursorStream cursor = new CursorStream(data);
        return new DelegatingSeekableInputStream(cursor) {
            @Override
            public long getPos() {
                return cursor.position();
            }

            @Override
            public void seek(final long newPos) throws IOException {
                if (newPos < 0 || newPos > data.length) {
                    throw new IOException("Cannot seek to " + newPos + " in " + name
                            + ", which is " + data.length + " bytes");
                }
                cursor.position((int) newPos);
            }
        };
    }

    /**
     * parquet-java builds its "is not a Parquet file" message out of the InputFile, so returning
     * the FlowFile UUID is what makes that message traceable in a NiFi bulletin.
     */
    @Override
    public String toString() {
        return name;
    }

    /** Exposes ByteArrayInputStream's protected cursor so that seeking is a field assignment. */
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
