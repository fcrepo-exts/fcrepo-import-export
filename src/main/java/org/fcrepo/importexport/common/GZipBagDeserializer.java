/*
 * Licensed to DuraSpace under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.
 *
 * DuraSpace licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.fcrepo.importexport.common;

import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Deflate a gzipped bag so that the underlying bag can continue to be deserialized.
 *
 * @author mikejritter
 * @since 2020-02-11
 */
public class GZipBagDeserializer implements BagDeserializer {

    private final Logger logger = LoggerFactory.getLogger(GZipBagDeserializer.class);

    private final BagProfile profile;

    protected GZipBagDeserializer(final BagProfile profile) {
        this.profile = profile;
    }

    @Override
    public Path deserialize(final Path root) throws IOException {
        final Path parent = root.getParent();
        final String nameWithExtension = root.getFileName().toString();
        final int dotIdx = nameWithExtension.lastIndexOf(".");
        final String filename = (dotIdx == -1) ? nameWithExtension : nameWithExtension.substring(0, dotIdx);
        final Path serializedBag = parent.resolve(filename);

        // Deflate the gzip to get the base file
        logger.info("Deflating gzipped bag: {}", filename);
        try (InputStream is = Files.newInputStream(root);
            final InputStream bis = new BufferedInputStream(is);
            final GzipCompressorInputStream gzipIS = new GzipCompressorInputStream(bis)) {

            Files.copy(gzipIS, serializedBag);
        } catch (FileAlreadyExistsException ex) {
            logger.warn("{} already decompressed! Continuing with deserialization.", root);
        }

        // Get a deserializer for the deflated content
        final BagDeserializer deserializer = SerializationSupport.deserializerFor(serializedBag, profile);
        return deserializer.deserialize(serializedBag);
    }
}
