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
import java.io.InputStream;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

/**
 * A parquet-java {@link InputFile} that reads straight out of the NiFi content repository.
 *
 * <p>Parquet keeps its footer at the end of the file, so a reader has to seek backwards. A content
 * repository stream only runs forwards, so the usual workaround is to buffer the whole FlowFile
 * first. This avoids that: a backward seek closes the current handle and asks the session for a
 * new one, which starts again at offset zero, then skips forward.
 *
 * <p>Reopening is cheaper than it sounds. {@code session.read} resolves to a fresh handle on the
 * same content claim, and the skip that follows becomes a file seek rather than a copy, so memory
 * use stays flat no matter how large the FlowFile is.
 */
final class FlowFileInputFile implements InputFile {

    private final ProcessSession session;
    private final FlowFile flowFile;

    FlowFileInputFile(final ProcessSession session, final FlowFile flowFile) {
        this.session = session;
        this.flowFile = flowFile;
    }

    @Override
    public long getLength() {
        return flowFile.getSize();
    }

    @Override
    public SeekableInputStream newStream() {
        // Each stream gets its own handle, so closing one parquet reader cannot pull the content
        // out from under another. DelegatingSeekableInputStream supplies the ByteBuffer and
        // readFully methods on top, leaving only getPos and seek.
        final ContentStream content = new ContentStream(session, flowFile);
        return new DelegatingSeekableInputStream(content) {
            @Override
            public long getPos() {
                return content.position();
            }

            @Override
            public void seek(final long newPos) throws IOException {
                content.seek(newPos);
            }
        };
    }

    /**
     * parquet-java builds its "is not a Parquet file" message out of the InputFile, so returning
     * the FlowFile UUID is what makes that message traceable in a NiFi bulletin.
     */
    @Override
    public String toString() {
        return flowFile.getAttribute(CoreAttributes.UUID.key());
    }

    /** A content repository handle that knows its own offset and can start over to rewind. */
    private static final class ContentStream extends InputStream {

        private final ProcessSession session;
        private final FlowFile flowFile;

        private InputStream delegate;
        private long position;

        ContentStream(final ProcessSession session, final FlowFile flowFile) {
            this.session = session;
            this.flowFile = flowFile;
            this.delegate = session.read(flowFile);
        }

        long position() {
            return position;
        }

        void seek(final long newPos) throws IOException {
            if (newPos == position) {
                return;
            }
            if (newPos < position) {
                delegate.close();
                delegate = session.read(flowFile);
                position = 0;
            }
            StreamUtils.skip(delegate, newPos - position);
            position = newPos;
        }

        @Override
        public int read() throws IOException {
            final int value = delegate.read();
            if (value >= 0) {
                position++;
            }
            return value;
        }

        @Override
        public int read(final byte[] buffer, final int offset, final int length) throws IOException {
            final int count = delegate.read(buffer, offset, length);
            if (count > 0) {
                position += count;
            }
            return count;
        }

        @Override
        public long skip(final long n) throws IOException {
            final long skipped = delegate.skip(n);
            position += skipped;
            return skipped;
        }

        @Override
        public int available() throws IOException {
            return delegate.available();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }
}
