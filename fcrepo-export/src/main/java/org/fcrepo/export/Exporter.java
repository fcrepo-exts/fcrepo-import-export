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
package org.fcrepo.export;

import static org.apache.commons.io.IOUtils.copy;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import org.fcrepo.client.FcrepoClient;
import org.fcrepo.client.FcrepoOperationFailedException;
import org.fcrepo.client.FcrepoResponse;
import org.fcrepo.kernel.api.RdfLexicon;
import org.slf4j.Logger;

/**
 * Fedora Export Utility
 *
 * @author awoods
 * @author escowles
 * @since 2016-08-29
 */
public class Exporter {
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
            binaryURI = new URI(RdfLexicon.NON_RDF_SOURCE.getURI());
            containerURI = new URI(RdfLexicon.CONTAINER.getURI());
        } catch (URISyntaxException ex) {
            // no-op
        }
    }

    /**
     * This method does the export
     */
    public void run() {
        System.out.println("Exporting!");
        export(config.getResource());
    }
    private void export(final URI uri) {
        try (FcrepoResponse response = client.head(uri).perform()) {
            final List<URI> linkHeaders = response.getLinkHeaders("type");
            if(linkHeaders.contains(binaryURI)) {
                exportBinary(uri);
            } else if (linkHeaders.contains(containerURI)) {
                exportContainer(uri);
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
        try (FcrepoResponse response = client.get(uri).perform()) {
            logger.info("Exporting binary: {}", uri);
            writeResponse(response, fileForBinary(uri));
        }
    }
    private void exportContainer(final URI uri)
            throws FcrepoOperationFailedException, IOException {
        try (FcrepoResponse response = client.get(uri).accept(config.getRdfLanguage()).perform()) {
            logger.info("Exporting description: {}", uri);
            writeResponse(response, fileForContainer(uri));
        } catch ( Exception ex ) { ex.printStackTrace(); }
    }
    void writeResponse(final FcrepoResponse response, final File file)
            throws IOException {
        Writer w = null;
        try {
            if (!file.getParentFile().exists()) {
                file.getParentFile().mkdirs();
            }
            w = new FileWriter(file);
            copy(response.getBody(), w, "UTF-8");
            logger.info("Exported {} to {}", response.getUrl(), file.getAbsolutePath());
        } finally {
            if (w != null) {
                w.close();
            }
        }
    }
    private File fileForBinary(final URI uri) {
        return new File(config.getBinaryDirectory(), uri.getPath());
    }
    private File fileForContainer(final URI uri) {
        return new File(config.getDescriptionDirectory(), uri.getPath() + config.getRdfExtension());
    }
}
