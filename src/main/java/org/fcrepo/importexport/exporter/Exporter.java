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
package org.fcrepo.importexport.exporter;

import static org.apache.commons.io.IOUtils.copy;
import static org.apache.jena.rdf.model.ModelFactory.createDefaultModel;
import static org.apache.jena.rdf.model.ResourceFactory.createProperty;
import static org.fcrepo.importexport.common.FcrepoConstants.CONTAINER;
import static org.fcrepo.importexport.common.FcrepoConstants.NON_RDF_SOURCE;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.fcrepo.client.FcrepoClient;
import org.fcrepo.client.FcrepoOperationFailedException;
import org.fcrepo.client.FcrepoResponse;
import org.fcrepo.importexport.common.AuthenticationRequiredRuntimeException;
import org.fcrepo.importexport.common.AuthorizationDeniedRuntimeException;
import org.fcrepo.importexport.common.Config;
import org.fcrepo.importexport.common.ResourceNotFoundRuntimeException;
import org.fcrepo.importexport.common.TransferProcess;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.NodeIterator;
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

    private static final Logger exportLogger = getLogger(IMPORT_EXPORT_LOG_PREFIX);
    private AtomicLong successCount = new AtomicLong(); // set to zero at start
    private AtomicLong failureCount = new AtomicLong(); // set to zero at start

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
    @Override
    public void run() {
        logger.info("Running exporter...");
        exportLogger.info("Starting export...");
        export(config.getResource());
        exportLogger.info("Export finished... {} resources exported, {} failures", successCount.get(),
            failureCount.get());
    }

    private void export(final URI uri) {
        try (FcrepoResponse response = client().head(uri).disableRedirects().perform()) {
            checkValidResponse(response, uri);
            final List<URI> linkHeaders = response.getLinkHeaders("type");
            if (linkHeaders.contains(binaryURI)) {
                final String contentType = response.getContentType();
                final boolean external = contentType != null && contentType.contains("message/external-body");
                exportBinary(uri, external);
            } else if (linkHeaders.contains(containerURI)) {
                exportDescription(uri);
            } else {
                logger.error("Resource is neither an LDP Container nor an LDP NonRDFSource: {}", uri);
                exportLogger.error("Resource is neither an LDP Container nor an LDP NonRDFSource: {}", uri);
                failureCount.incrementAndGet();
            }
        } catch (FcrepoOperationFailedException ex) {
            logger.warn("Error retrieving content: {}", ex.toString());
            exportLogger.error("Error reading context: {}", ex.toString());
            failureCount.incrementAndGet();
        } catch (IOException ex) {
            logger.warn("Error writing content: {}", ex.toString());
            exportLogger.error("Error writing content: {}", ex.toString());
            failureCount.incrementAndGet();
        }
    }

    private void exportBinary(final URI uri, final boolean external)
            throws FcrepoOperationFailedException, IOException {

        if (!config.isIncludeBinaries()) {
            logger.info("Skipping {}", uri);
            return;
        }

        try (FcrepoResponse response = client().get(uri).disableRedirects().perform()) {
            checkValidResponse(response, uri);

            final File file = external ?
                                TransferProcess.fileForExternalResources(uri, config.getBaseDirectory()) :
                                    TransferProcess.fileForBinary(uri, config.getBaseDirectory());

            logger.info("Exporting binary: {}", uri);
            writeResponse(response, file);
            exportLogger.info("Exporting binary: {} to {}", uri, file.getAbsolutePath());
            successCount.incrementAndGet();
        }
    }

    private void exportDescription(final URI uri) throws FcrepoOperationFailedException, IOException {
        final File file = TransferProcess.fileForURI(uri, config.getBaseDirectory(),
                config.getRdfExtension());
        if (file == null) {
            logger.info("Skipping {}", uri);
            return;
        }

        try (FcrepoResponse response = client().get(uri).accept(config.getRdfLanguage()).perform()) {
            checkValidResponse(response, uri);
            logger.info("Exporting description: {}", uri);
            writeResponse(response, file);
            exportLogger.info("Exporting description: {} to {}", uri, file.getAbsolutePath());
            successCount.incrementAndGet();
        } catch ( Exception ex ) {
            ex.printStackTrace();
            exportLogger.error("Error exporting description: {}, Cause: {}", uri, ex.getMessage());
            failureCount.incrementAndGet();
        }

        exportMembers(file);
    }
    private void exportMembers(final File file) {
        try {
            final Model model = createDefaultModel().read(new FileInputStream(file), null, config.getRdfLanguage());
            for (final String p : config.getPredicates()) {
                for (final NodeIterator it = model.listObjectsOfProperty(createProperty(p)); it.hasNext();) {
                    export(URI.create(it.nextNode().toString()));
                }
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
        try (OutputStream out = new FileOutputStream(file)) {
            copy(response.getBody(), out);
            logger.info("Exported {} to {}", response.getUrl(), file.getAbsolutePath());
        }

        final List<URI> describedby = response.getLinkHeaders("describedby");
        for (final Iterator<URI> it = describedby.iterator(); describedby != null && it.hasNext(); ) {
            exportDescription(it.next());
        }
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
                if (response.getStatusCode() < 200 || response.getStatusCode() > 307) {
                    throw new RuntimeException("Export operation failed: unexpected status "
                            + response.getStatusCode() + " for " + uri);
                }
        }
    }
}
