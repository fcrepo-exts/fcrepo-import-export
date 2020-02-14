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

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Interface for common deserialization operations for {@link gov.loc.repository.bagit.domain.Bag}s. Each deserializer
 * is instantiated independently of what it is working on so that only {@link BagDeserializer#deserialize(Path)}
 * needs to be called.
 *
 * @author mikejritter
 * @since 2020-02-11
 */
public interface BagDeserializer {

    Logger logger = LoggerFactory.getLogger(BagDeserializer.class);

    /**
     * Deserialize a {@link gov.loc.repository.bagit.domain.Bag} located at the give {@code path}. This will create a
     * version of the bag in the parent directory of the given {@code path}.
     *
     * @param path the {@link Path} to the serialized version of a {@link gov.loc.repository.bagit.domain.Bag}
     * @return the {@link Path} to the deserialized bag
     * @throws IOException if there are any errors deserializing the bag
     */
    Path deserialize(final Path path) throws IOException;

    /**
     * Common {@link ArchiveEntry} extraction handling for Bags which are serialized with a format handled by the
     * commons-compress library.
     *
     * @param is the {@link ArchiveInputStream}
     * @param parent the parent directory of the serialized bag
     * @throws IOException if there is an error creating a directory or file when extracting the archive
     */
    default void extract(final ArchiveInputStream is, final Path parent) throws IOException {
        ArchiveEntry entry;
        while ((entry = is.getNextEntry()) != null) {
            final String name = entry.getName();

            logger.debug("Handling entry {}", entry.getName());
            final Path archiveFile = parent.resolve(name);

            if (entry.isDirectory()) {
                Files.createDirectories(archiveFile);
            } else {
                if (Files.exists(parent.resolve(name))) {
                    logger.warn("File {} already exists!", name);
                } else{
                    Files.copy(is, archiveFile);
                }
            }
        }
    }
}
