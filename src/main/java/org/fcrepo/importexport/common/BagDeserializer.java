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

    void deserialize(final Path path) throws IOException;

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
