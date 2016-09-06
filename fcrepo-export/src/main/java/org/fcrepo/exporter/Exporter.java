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
package org.fcrepo.exporter;

import static org.apache.commons.io.IOUtils.copy;
import static org.apache.jena.rdf.model.ModelFactory.createDefaultModel;
import static org.fcrepo.importexport.Constants.BINARY_EXTENSION;
import static org.fcrepo.importexport.FcrepoConstants.CONTAINER;
import static org.fcrepo.importexport.FcrepoConstants.CONTAINS;
import static org.fcrepo.importexport.FcrepoConstants.NON_RDF_SOURCE;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.List;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.NodeIterator;
import org.fcrepo.client.FcrepoClient;
import org.fcrepo.client.FcrepoOperationFailedException;
import org.fcrepo.client.FcrepoResponse;
import org.fcrepo.importexport.TransferProcess;
import org.slf4j.Logger;

/**
 * Fedora Export Utility
 *
 * @author awoods
 * @author escowles
 * @since 2016-08-29
 */
public class Exporter implements TransferProcess {
    private static final Logger logger = getLogger(Exporter.class);
    private Config config;
    protected FcrepoClient client;
    private URI binaryURI;
    private URI containerURI;

    /**
     * Constructor that takes the Import/Export configuration
     *
     * @param config for export
     */
    public Exporter(final Config config) {
        this.config = config;
        this.client = FcrepoClient.client().build();
        try {
            binaryURI = new URI(NON_RDF_SOURCE.getURI());
            containerURI = new URI(CONTAINER.getURI());
        } catch (URISyntaxException ex) {
            // no-op
        }
    }

    /**
     * This method does the export
     */
    public void run() {
        logger.info("Running exporter...");
        export(config.getResource());
    }
    private void export(final URI uri) {
        try (FcrepoResponse response = client.head(uri).perform()) {
            final List<URI> linkHeaders = response.getLinkHeaders("type");
            if (linkHeaders.contains(binaryURI)) {
                exportBinary(uri);
            } else if (linkHeaders.contains(containerURI)) {
                exportDescription(uri);
            } else {
                logger.error("Resource is neither an LDP Container nor an LDP NonRDFSource: {}", uri);
            }
        } catch (FcrepoOperationFailedException ex) {
            logger.warn("Error retrieving content: {}", ex.toString());
        } catch (IOException ex) {
            logger.warn("Error writing content: {}", ex.toString());
        }
    }
    private void exportBinary(final URI uri)
            throws FcrepoOperationFailedException, IOException {
        final File file = fileForBinary(uri);
        if (file == null) {
            logger.info("Skipping {}", uri);
            return;
        }

        try (FcrepoResponse response = client.get(uri).perform()) {
            logger.info("Exporting binary: {}", uri);
            writeResponse(response, file);
        }
    }

    private void exportDescription(final URI uri) throws FcrepoOperationFailedException, IOException {
        final File file = fileForContainer(uri);
        if (file == null) {
            logger.info("Skipping {}", uri);
            return;
        }

        try (FcrepoResponse response = client.get(uri).accept(config.getRdfLanguage()).perform()) {
            logger.info("Exporting description: {}", uri);
            writeResponse(response, file);
        } catch ( Exception ex ) {
            ex.printStackTrace();
        }

        exportMembers(file);
    }
    private void exportMembers(final File file) {
        try {
            final Model model = createDefaultModel().read(new FileInputStream(file), null, config.getRdfLanguage());
            for (final NodeIterator it = model.listObjectsOfProperty(CONTAINS); it.hasNext();) {
                export(new URI(it.nextNode().toString()));
            }
        } catch (FileNotFoundException ex) {
            logger.warn("Unable to parse file: {}", ex.toString());
        } catch (URISyntaxException ex) {
            logger.warn("Unable to parse URI: {}", ex.toString());
        }
    }
    void writeResponse(final FcrepoResponse response, final File file)
            throws IOException, FcrepoOperationFailedException {
        if (!file.getParentFile().exists()) {
            file.getParentFile().mkdirs();
        }
        try (Writer w = new FileWriter(file)) {
            copy(response.getBody(), w, "UTF-8");
            logger.info("Exported {} to {}", response.getUrl(), file.getAbsolutePath());
        }

        final List<URI> describedby = response.getLinkHeaders("describedby");
        for (final Iterator<URI> it = describedby.iterator(); describedby != null && it.hasNext(); ) {
            exportDescription(it.next());
        }
    }
    private File fileForBinary(final URI uri) {
        if (config.getBinaryDirectory() == null) {
            return null;
        }
        return new File(config.getBinaryDirectory(), uri.getPath().replaceAll(":", "_") + BINARY_EXTENSION);
    }
    private File fileForContainer(final URI uri) {
        return new File(config.getDescriptionDirectory(), uri.getPath().replaceAll(":", "_")
            + config.getRdfExtension());
    }
}
