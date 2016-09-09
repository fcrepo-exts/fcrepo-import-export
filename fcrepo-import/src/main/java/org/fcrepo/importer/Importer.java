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
package org.fcrepo.importer;

import static org.apache.jena.rdf.model.ResourceFactory.createResource;
import static org.apache.jena.riot.RDFLanguages.contentTypeToLang;
import static org.slf4j.LoggerFactory.getLogger;
import static org.fcrepo.importexport.FcrepoConstants.CONTAINS;
import static org.fcrepo.importexport.FcrepoConstants.DESCRIBEDBY;
import static org.fcrepo.importexport.FcrepoConstants.HAS_MIME_TYPE;
import static org.fcrepo.importexport.FcrepoConstants.HAS_MESSAGE_DIGEST;
import static org.fcrepo.importexport.FcrepoConstants.HAS_SIZE;
import static org.fcrepo.importexport.FcrepoConstants.NON_RDF_SOURCE;
import static org.fcrepo.importexport.FcrepoConstants.RDF_TYPE;
import static org.fcrepo.importexport.FcrepoConstants.REPOSITORY_NAMESPACE;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ResIterator;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.fcrepo.client.FcrepoClient;
import org.fcrepo.client.FcrepoOperationFailedException;
import org.fcrepo.client.FcrepoResponse;
import org.fcrepo.importexport.AuthenticationRequiredRuntimeException;
import org.fcrepo.importexport.Config;
import org.fcrepo.importexport.TransferProcess;
import org.slf4j.Logger;

/**
 * Fedora Import Utility
 *
 * @author awoods
 * @author escowles
 * @since 2016-08-29
 */
public class Importer implements TransferProcess {
    private static final Logger logger = getLogger(Importer.class);
    private Config config;
    protected FcrepoClient.FcrepoClientBuilder clientBuilder;

    /**
     * Constructor that takes the Import/Export configuration
     *
     * @param config for import
     * @param clientBuilder for sending resources to Fedora
     */
    public Importer(final Config config, final FcrepoClient.FcrepoClientBuilder clientBuilder) {
        this.config = config;
        this.clientBuilder = clientBuilder;
    }

    private FcrepoClient client() {
        if (config.getUsername() != null) {
            clientBuilder.credentials(config.getUsername(), config.getPassword());
        }
        return clientBuilder.build();
    }

    /**
     * This method does the import
     */
    public void run() {
        logger.info("Running importer...");
        importDirectory(config.getDescriptionDirectory());
    }

    private void importDirectory(final File dir) {
        for (final File f : dir.listFiles()) {
            if (f.isDirectory()) {
                importDirectory(f);
            } else if (f.isFile()) {
                importFile(f);
            }
        }
    }

    private void importFile(final File f) {
        FcrepoResponse response = null;
        URI uri = null;
        try {
            final Model model = parseFile(f);
            final ResIterator binaryResources = model.listResourcesWithProperty(RDF_TYPE, NON_RDF_SOURCE);
            if (binaryResources.hasNext()) {
                uri = new URI(binaryResources.nextResource().getURI());
                logger.info("Importing binary {}", f.getAbsolutePath());
                response = importBinary(uri, sanitize(model));
            } else {
                uri = uriForFile(f, config.getDescriptionDirectory());
                logger.info("Importing container {}", f.getAbsolutePath());
                response = importContainer(uri, sanitize(model));
            }

            if (response.getStatusCode() == 401) {
                throw new AuthenticationRequiredRuntimeException();
            } else if (response.getStatusCode() > 204 || response.getStatusCode() < 200) {
                logger.warn("Error while importing {} ({}): {}",
                   f.getAbsolutePath(), response.getStatusCode(), IOUtils.toString(response.getBody()));
            } else {
                logger.info("Imported {}: {}", f.getAbsolutePath(), uri);
            }
        } catch (FcrepoOperationFailedException ex) {
            throw new RuntimeException("Error importing " + f.getAbsolutePath() + ": " + ex.toString(), ex);
        } catch (IOException ex) {
            throw new RuntimeException("Error reading or parsing " + f.getAbsolutePath() + ": " + ex.toString(), ex);
        } catch (URISyntaxException ex) {
            throw new RuntimeException("Error building URI for " + f.getAbsolutePath() + ": " + ex.toString(), ex);
        }
    }

    private Model parseFile(final File f) throws IOException {
        final URI source = (config.getSource() == null) ? config.getResource() : config.getSource();
        final SubjectMappingStreamRDF mapper = new SubjectMappingStreamRDF(source, config.getResource());
        try (FileInputStream in = new FileInputStream(f)) {
            RDFDataMgr.parse(mapper, in, contentTypeToLang(config.getRdfLanguage()));
        }
        return mapper.getModel();
    }

    private FcrepoResponse importBinary(final URI binaryURI, final Model model)
            throws FcrepoOperationFailedException, IOException {
        final String contentType = model.getProperty(createResource(binaryURI.toString()), HAS_MIME_TYPE).getString();
        final File binaryFile = fileForURI(binaryURI);
        final FcrepoResponse binaryResponse = client().put(binaryURI).body(binaryFile, contentType).perform();
        if (binaryResponse.getStatusCode() == 201) {
            logger.info("Imported binary: {}", binaryURI);
        } else {
            return binaryResponse;
        }

        final URI descriptionURI = binaryResponse.getLinkHeaders("describedby").get(0);
        return client().put(descriptionURI).body(modelToStream(model), config.getRdfLanguage())
            .preferLenient().perform();
    }

    private FcrepoResponse importContainer(final URI uri, final Model model) throws FcrepoOperationFailedException {
        return client().put(uri).body(modelToStream(model), config.getRdfLanguage()).preferLenient().perform();
    }

    private Model sanitize(final Model model) {
        final List<Statement> remove = new ArrayList<>();
        for (final StmtIterator it = model.listStatements(); it.hasNext(); ) {
            final Statement s = it.nextStatement();

            if (s.getPredicate().getNameSpace().equals(REPOSITORY_NAMESPACE)
                    || s.getSubject().getURI().endsWith("fcr:export?format=jcr/xml")
                    || s.getSubject().getURI().equals(REPOSITORY_NAMESPACE + "jcr/xml")
                    || s.getPredicate().equals(DESCRIBEDBY)
                    || s.getPredicate().equals(CONTAINS)
                    || s.getPredicate().equals(HAS_MESSAGE_DIGEST)
                    || s.getPredicate().equals(HAS_SIZE)
                    || (s.getPredicate().equals(RDF_TYPE)
                        && s.getResource().getNameSpace().equals(REPOSITORY_NAMESPACE)) ) {
                remove.add(s);
            }
        }
        return model.remove(remove);
    }

    private InputStream modelToStream(final Model model) {
        final ByteArrayOutputStream buf = new ByteArrayOutputStream();
        model.write(buf, config.getRdfLanguage());
        return new ByteArrayInputStream(buf.toByteArray());
    }

    private URI uriForFile(final File f, final File baseDir) throws URISyntaxException {
        String relative = baseDir.toPath().relativize(f.toPath()).toString();
        if (relative.startsWith("rest") && config.getResource().toString().endsWith("/rest")) {
            relative = relative.substring("rest".length());
        }
        if (relative.endsWith(config.getRdfExtension())) {
            relative = relative.substring(0, relative.length() - config.getRdfExtension().length());
        }

        // TODO parse RDF to figure out the real URI?
        if (relative.endsWith("/fcr_metadata")) {
            relative = relative.substring(0, relative.length() - "fcr_metadata".length()) + "/fcr:metadata";
        }
        return new URI(config.getResource() + relative);
    }

    private File fileForURI(final URI uri) {
        return new File(config.getBinaryDirectory() + TransferProcess.decodePath(uri.getPath()));
    }
}
