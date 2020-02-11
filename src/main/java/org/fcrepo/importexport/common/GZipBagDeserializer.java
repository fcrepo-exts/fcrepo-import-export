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

/**
 * Deserializer for bags which are serialized using tar+gzip
 *
 * @author mikejritter
 * @since 2020-02-11
 */
public class GZipBagDeserializer implements BagDeserializer {

    private final Logger logger = LoggerFactory.getLogger(GZipBagDeserializer.class);

    protected GZipBagDeserializer() {
    }

    @Override
    public Path deserialize(final Path root) throws IOException {
        final String regex = "\\.tar.gz";
        final Pattern pattern = Pattern.compile(regex);
        final Path parent = root.getParent();
        final Path fileName = root.getFileName();

        final String trimmedName = pattern.matcher(fileName.toString()).replaceFirst("");
        logger.info("Extracting serialized bag {}", trimmedName);

        try (InputStream is = Files.newInputStream(root)) {
            final InputStream buffedIs = new BufferedInputStream(is);
            final GZIPInputStream gzipIs = new GZIPInputStream(buffedIs);
            final ArchiveInputStream archiveIs = new TarArchiveInputStream(gzipIs);
            extract(archiveIs, parent);
        }

        return parent.resolve(trimmedName);
    }
}
