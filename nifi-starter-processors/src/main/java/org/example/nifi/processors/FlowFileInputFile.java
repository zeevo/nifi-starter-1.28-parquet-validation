package org.example.nifi.processors;

import java.io.IOException;
import java.io.InputStream;

import org.apache.nifi.stream.io.ByteCountingInputStream;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

/**
 * Adapts a FlowFile content stream to Parquet's {@link InputFile}, which requires
 * random access. Follows the same approach as NiFi's own parquet bundle
 * (NifiParquetInputFile): the stream must support mark/reset, and a backward seek
 * is implemented as reset-to-start followed by a forward skip. NiFi's
 * ContentClaimInputStream (returned by session.read) supports this efficiently:
 * the mark read limit is only a buffering hint, and a reset beyond it reopens the
 * underlying content claim rather than failing. The small limit keeps that buffer
 * small; a large one would buffer file content in heap.
 */
class FlowFileInputFile implements InputFile {

    private static final int MARK_READ_LIMIT = 8192;

    private final ByteCountingInputStream input;
    private final long length;

    FlowFileInputFile(final InputStream input, final long length) {
        if (!input.markSupported()) {
            throw new IllegalArgumentException("InputStream must support mark/reset");
        }
        this.input = new ByteCountingInputStream(input);
        this.input.mark(MARK_READ_LIMIT);
        this.length = length;
    }

    @Override
    public long getLength() {
        return length;
    }

    @Override
    public SeekableInputStream newStream() {
        return new DelegatingSeekableInputStream(input) {
            @Override
            public long getPos() {
                return input.getBytesConsumed();
            }

            @Override
            public void seek(final long newPos) throws IOException {
                final long currentPos = getPos();
                if (newPos == currentPos) {
                    return;
                }
                if (newPos < currentPos) {
                    input.reset();
                    input.mark(MARK_READ_LIMIT);
                }
                StreamUtils.skip(input, newPos - getPos());
            }
        };
    }
}
