package org.fcrepo.importexport.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class ZipBagDeserializer implements BagDeserializer {

    private final Logger logger = LoggerFactory.getLogger(ZipBagDeserializer.class);

    @Override
    public void deserialize(Path root) throws IOException {
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
}
