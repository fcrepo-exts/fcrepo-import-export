package org.fcrepo.importexport.common;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * sup
 *
 */
public interface BagDeserializer {

    void deserialize(final Path path) throws IOException;

    /*
    public static void deserialize(final Path root, final BagProfile profile) throws IOException {
        final Set<String> acceptedTypes = profile.getAcceptedSerializations();
        final String contentType = Files.probeContentType(root);

        if (acceptedTypes.contains(contentType) && supportedSerializations.contains(contentType)) {
            if ("application/zip".equals(contentType)) {
                extractZip(root);
            }
        } else {
            logger.error("Unacceptable type {}", contentType);
        }

    }

    private static void extractZip(final Path root) throws IOException {
        final String regex = "\\.zip";
        final Pattern pattern = Pattern.compile(regex);
        final Path parent = root.getParent();
        final Path fileName = root.getFileName();

        final String trimmedName = pattern.matcher(fileName.toString()).replaceFirst("");
        logger.info("Extracting serialized bag {}", trimmedName);

        ZipEntry entry;
        try (ZipInputStream inputStream = new ZipInputStream(Files.newInputStream(root))) {
            while ((entry = inputStream.getNextEntry()) != null) {
                final String name = entry.getName();

                logger.debug("Handling entry {}", entry.getName());
                final Path inBagFile = parent.resolve(name);

                if (entry.isDirectory()) {
                    Files.createDirectories(inBagFile);
                } else {
                    Files.copy(inputStream, inBagFile);
                }

                inputStream.closeEntry();
            }
        }
    }
     */


}
