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

import java.io.IOException;
import java.io.OutputStream;

import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

/**
 * A parquet-java {@link OutputFile} that writes straight into a FlowFile.
 *
 * <p>Writing Parquet, unlike reading it, never seeks. The writer emits the magic bytes, then each
 * row group, then the footer, then the footer length, in that order, and only needs to be told how
 * many bytes have gone past so it can record offsets in the footer. So a plain counter over the
 * OutputStream that {@code session.write} hands us is enough: nothing is buffered, and memory does
 * not grow with the size of the output.
 *
 * <p>The asymmetry with reading is worth noting. {@link FlowFileInputFile} has to reopen the
 * content to seek backwards, because the footer is at the end of the file and a reader must go
 * looking for it. A writer puts the footer there in the first place, so it only ever moves
 * forwards and a counter is enough.
 */
final class FlowFileOutputFile implements OutputFile {

    private final OutputStream out;

    FlowFileOutputFile(final OutputStream out) {
        this.out = out;
    }

    @Override
    public PositionOutputStream create(final long blockSizeHint) {
        return new CountingOutputStream(out);
    }

    @Override
    public PositionOutputStream createOrOverwrite(final long blockSizeHint) {
        return new CountingOutputStream(out);
    }

    /** The FlowFile is created by the session, so the writer does not get to choose a block size. */
    @Override
    public boolean supportsBlockSize() {
        return false;
    }

    @Override
    public long defaultBlockSize() {
        return 0;
    }

    /** All parquet needs of the output is the number of bytes written so far. */
    private static final class CountingOutputStream extends PositionOutputStream {

        private final OutputStream out;
        private long position;

        CountingOutputStream(final OutputStream out) {
            this.out = out;
        }

        @Override
        public long getPos() {
            return position;
        }

        @Override
        public void write(final int b) throws IOException {
            out.write(b);
            position++;
        }

        @Override
        public void write(final byte[] buffer, final int offset, final int length) throws IOException {
            out.write(buffer, offset, length);
            position += length;
        }

        @Override
        public void flush() throws IOException {
            out.flush();
        }

        /**
         * Deliberately does not close the underlying stream. The session owns it, and closing a
         * FlowFile output stream early makes the session throw when it tries to finish the write.
         */
        @Override
        public void close() throws IOException {
            out.flush();
        }
    }
}
