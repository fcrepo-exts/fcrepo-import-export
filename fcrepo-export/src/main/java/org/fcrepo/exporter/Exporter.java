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
import static org.fcrepo.importexport.FcrepoConstants.BINARY_EXTENSION;
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
import java.util.Iterator;
import java.util.List;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.NodeIterator;
import org.fcrepo.client.FcrepoClient;
import org.fcrepo.client.FcrepoOperationFailedException;
import org.fcrepo.client.FcrepoResponse;
import org.fcrepo.importexport.AuthenticationRequiredRuntimeException;
import org.fcrepo.importexport.AuthorizationDeniedRuntimeException;
import org.fcrepo.importexport.Config;
import org.fcrepo.importexport.ResourceNotFoundRuntimeException;
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
    protected FcrepoClient.FcrepoClientBuilder clientBuilder;
    private URI binaryURI;
    private URI containerURI;

    /**
     * Constructor that takes the Import/Export configuration
     *
     * @param config for export
     * @param clientBuilder for retrieving resources from Fedora
     */
    public Exporter(final Config config, final FcrepoClient.FcrepoClientBuilder clientBuilder) {
        this.config = config;
        this.clientBuilder = clientBuilder;
        this.binaryURI = URI.create(NON_RDF_SOURCE.getURI());
        this.containerURI = URI.create(CONTAINER.getURI());
    }

    private FcrepoClient client() {
        if (config.getUsername() != null) {
            clientBuilder.credentials(config.getUsername(), config.getPassword());
        }
        return clientBuilder.build();
    }

    /**
     * This method does the export
     */
    public void run() {
        logger.info("Running exporter...");
        export(config.getResource());
    }
    private void export(final URI uri) {
        try (FcrepoResponse response = client().head(uri).perform()) {
            checkValidResponse(response, uri);
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

        try (FcrepoResponse response = client().get(uri).perform()) {
            checkValidResponse(response, uri);
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

        try (FcrepoResponse response = client().get(uri).accept(config.getRdfLanguage()).perform()) {
            checkValidResponse(response, uri);
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
                export(URI.create(it.nextNode().toString()));
            }
        } catch (FileNotFoundException ex) {
            logger.warn("Unable to parse file: {}", ex.toString());
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
        return new File(config.getBinaryDirectory(), TransferProcess.encodePath(uri.getPath()) + BINARY_EXTENSION);
    }
    private File fileForContainer(final URI uri) {
        return new File(config.getDescriptionDirectory(), TransferProcess.encodePath(uri.getPath())
            + config.getRdfExtension());
    }

    /**
     * Checks the response code and throws a RuntimeException with a helpful
     * message (when possible) for non 2xx codes.
     * @param response the response from a REST call to Fedora
     * @param uri the URI against which the request was made
     */
    private void checkValidResponse(final FcrepoResponse response, final URI uri) {
        switch (response.getStatusCode()) {
            case 401:
                throw new AuthenticationRequiredRuntimeException();
            case 403:
                throw new AuthorizationDeniedRuntimeException(config.getUsername(), uri);
            case 404:
                throw new ResourceNotFoundRuntimeException(uri);
            default:
                if (response.getStatusCode() < 200 || response.getStatusCode() >= 300) {
                    throw new RuntimeException("Export operation failed: unexpected status "
                            + response.getStatusCode() + " for " + uri);
                }
        }
    }
}
