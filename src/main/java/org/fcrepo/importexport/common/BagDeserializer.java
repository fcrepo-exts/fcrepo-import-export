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
 * sup
 *
 */
public interface BagDeserializer {

    Logger logger = LoggerFactory.getLogger(BagDeserializer.class);

    /**
     *
     * @param path
     * @throws IOException
     */
    void deserialize(final Path path) throws IOException;

    /**
     *
     * @param is
     * @param parent
     * @throws IOException
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
                Files.copy(is, archiveFile);
            }
        }
    }
}
