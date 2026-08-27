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
package org.example.nifi.api;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

import org.apache.nifi.controller.ControllerService;

/**
 * Opens a Parquet file and exposes both its rows and its file-level metadata.
 *
 * <p>NiFi's own {@code ParquetReader} cannot do the second half. It returns a {@code RecordReader},
 * which is a schema and a cursor over rows and has no notion of anything file-level, and NiFi
 * 1.28.1's {@code ParquetRecordReader} never opens the footer's key/value metadata at all. Nor can
 * that gap be closed by narrowing a property to {@code ParquetReader}: that class lives in
 * {@code nifi-parquet-nar}, a sibling of this bundle rather than an ancestor, so a processor here
 * cannot reference it and a bundled copy would be a different class from the service an operator
 * configured.
 *
 * <p>So the way to have a Parquet-specific controller service is to declare one. This interface
 * lives in the API module, which is bundled into the same NAR as the processors that use it and the
 * implementation that satisfies it, which is exactly the arrangement
 * {@code StarterService} already uses.
 */
public interface ParquetSource extends ControllerService {

    /** One open Parquet file: its file-level metadata, and a cursor over its rows. */
    interface Handle extends Closeable {

        /**
         * The file-level key/value metadata from the footer, exactly as it appears there,
         * including any keys a writer generated for itself.
         */
        Map<String, String> fileMetadata();

        /** The file's Avro schema as JSON, either the one it declares or one derived from its types. */
        String avroSchema();

        /** The compression codec its first column chunk uses, so a copy can match it. */
        String compressionCodec();

        /** Average uncompressed row group size, or 0 when the file has no row groups. */
        long rowGroupSize();

        /** Rows the footer says the file holds. */
        long rowCount();

        /**
         * The next row as an Avro {@code GenericRecord}, or null at the end.
         *
         * @return declared as Object so the API module needs no Avro dependency of its own
         */
        Object nextRecord() throws IOException;
    }

    /**
     * Opens a Parquet file.
     *
     * @param content the file's bytes, which must be seekable in full because the Parquet footer
     *                lives at the end of the file
     */
    Handle open(byte[] content, String name) throws IOException;
}
