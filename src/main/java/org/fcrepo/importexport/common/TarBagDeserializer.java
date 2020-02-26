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
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

/**
 * Deserializer for {@link gov.loc.repository.bagit.domain.Bag}s serialized using tar
 *
 * @author mikejritter
 * @since 2020-02-11
 */
public class TarBagDeserializer implements BagDeserializer {

    private final Logger logger = LoggerFactory.getLogger(TarBagDeserializer.class);

    protected TarBagDeserializer() {
    }

    @Override
    public Path deserialize(final Path root) throws IOException {
        logger.info("Extracting serialized bag: {}", root.getFileName());

        final Path parent = root.getParent();
        final int rootPathCount = root.getNameCount();
        Optional<String> filename = Optional.empty();
        try (TarArchiveInputStream tais = new TarArchiveInputStream(Files.newInputStream(root))) {
            ArchiveEntry entry;
            while ((entry = tais.getNextEntry()) != null) {
                final String name = entry.getName();

                logger.debug("Handling entry {}", entry.getName());
                final Path archiveFile = parent.resolve(name);

                if (entry.isDirectory()) {
                    Files.createDirectories(archiveFile);
                    if (archiveFile.getNameCount() == rootPathCount) {
                        logger.debug("Archive name is {}", archiveFile.getFileName());
                        filename = Optional.of(archiveFile.getFileName().toString());
                    }
                } else {
                    if (Files.exists(parent.resolve(name))) {
                        logger.warn("File {} already exists!", name);
                    } else {
                        Files.copy(tais, archiveFile);
                    }
                }
            }
        }

        final String extracted = filename.orElseGet(() -> {
            // get the name from the tarball minus the extension
            final String rootName = root.getFileName().toString();
            final int dotIdx = rootName.lastIndexOf(".");
            return rootName.substring(0, dotIdx);
        });
        return parent.resolve(extracted);
    }
}
