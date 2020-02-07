package org.fcrepo.importexport.common;

import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

public class GZipBagDeserializer implements BagDeserializer {

    private final Logger logger = LoggerFactory.getLogger(GZipBagDeserializer.class);

    @Override
    public void deserialize(final Path root) throws IOException {
        final String regex = "\\.tar.gz";
        final Pattern pattern = Pattern.compile(regex);
        final Path parent = root.getParent();
        final Path fileName = root.getFileName();

        final String trimmedName = pattern.matcher(fileName.toString()).replaceFirst("");
        logger.info("Extracting serialized bag {}", trimmedName);

        try (InputStream is = Files.newInputStream(root)) {
            InputStream buffedIs = new BufferedInputStream(is);
            GZIPInputStream gzipIs = new GZIPInputStream(buffedIs);
            ArchiveInputStream archiveIs = new TarArchiveInputStream(gzipIs);
            extract(archiveIs, parent);
        }
    }
}
