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
package org.fcrepo.importexport.importer;

import static java.util.Arrays.stream;
import static org.apache.jena.rdf.model.ResourceFactory.createResource;
import static org.apache.jena.riot.RDFLanguages.contentTypeToLang;
import static org.slf4j.LoggerFactory.getLogger;
import static org.fcrepo.importexport.common.FcrepoConstants.BINARY_EXTENSION;
import static org.fcrepo.importexport.common.FcrepoConstants.CONTAINS;
import static org.fcrepo.importexport.common.FcrepoConstants.DESCRIBEDBY;
import static org.fcrepo.importexport.common.FcrepoConstants.HAS_MIME_TYPE;
import static org.fcrepo.importexport.common.FcrepoConstants.HAS_MESSAGE_DIGEST;
import static org.fcrepo.importexport.common.FcrepoConstants.HAS_SIZE;
import static org.fcrepo.importexport.common.FcrepoConstants.NON_RDF_SOURCE;
import static org.fcrepo.importexport.common.FcrepoConstants.RDF_TYPE;
import static org.fcrepo.importexport.common.FcrepoConstants.REPOSITORY_NAMESPACE;

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
import org.fcrepo.importexport.common.AuthenticationRequiredRuntimeException;
import org.fcrepo.importexport.common.Config;
import org.fcrepo.importexport.common.TransferProcess;
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
     * A directory within the metadata directory that serves as the
     * root of the resource being imported.  If the export directory
     * contains /fcrepo/rest/one/two/three and we're importing
     * the resource at /fcrepo/rest/one/two, this stores that path.
     */
    protected File importContainerDirectory;

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
        final File importContainerMetadataFile = TransferProcess.fileForContainer(config.getResource(),
                config.getDescriptionDirectory(), config.getRdfExtension());
        importContainerDirectory = TransferProcess.directoryForContainer(config.getResource(),
                config.getDescriptionDirectory());
        if (!importContainerMetadataFile.exists()) {
            logger.debug("No container exists in the metadata directory {} for the requested resource {},"
                    + " importing all contained resources instead.", importContainerMetadataFile.getPath(),
                    config.getResource());
        } else {
            importFile(importContainerMetadataFile);
        }
        importDirectory(importContainerDirectory);
    }

    private void importDirectory(final File dir) {
        // process all the files first (because otherwise they might be
        // created as peartree nodes which can't be updated with properties
        // later.
        if (dir.listFiles() != null) {
            stream(dir.listFiles()).filter(File::isFile).forEach(file -> importFile(file));
            stream(dir.listFiles()).filter(File::isDirectory).forEach(directory -> importDirectory(directory));
        }
    }

    private void importFile(final File f) {
        // The path, relative to the metadata root in the export directory.
        // This is used in place of the full path to make the output more readable.
        final String sourceRelativePath = config.getDescriptionDirectory().toPath().relativize(f.toPath()).toString();

        if (f.getPath().endsWith(BINARY_EXTENSION)) {
            // ... this is only expected to happen when binaries and metadata are written to the same directory...
            logger.debug("Skipping binary {}: it will be imported when its metadata is imported.", sourceRelativePath);
            return;
        } else if (!f.getPath().endsWith(config.getRdfExtension())) {
            // this could be hidden files created by the OS
            logger.info("Skipping file with unexpected extension ({}).", sourceRelativePath);
            return;
        } else {
            FcrepoResponse response = null;
            URI destinationUri = null;
            try {
                final Model model = parseFile(f);
                if (model.contains(null, RDF_TYPE, createResource(REPOSITORY_NAMESPACE + "RepositoryRoot"))) {
                    logger.debug("Skipping import of repository root.");
                    return;
                }
                final ResIterator binaryResources = model.listResourcesWithProperty(RDF_TYPE, NON_RDF_SOURCE);
                if (binaryResources.hasNext()) {
                    destinationUri = new URI(binaryResources.nextResource().getURI());
                    logger.info("Importing binary {}", sourceRelativePath);
                    response = importBinary(destinationUri, sanitize(model));
                } else {
                    destinationUri = new URI(config.getResource().toString() + "/"
                            + uriPathForFile(f, importContainerDirectory));
                    logger.info("Importing container {} to {}", f.getAbsolutePath(), destinationUri);
                    response = importContainer(destinationUri, sanitize(model));
                }

                if (response.getStatusCode() == 401) {
                    throw new AuthenticationRequiredRuntimeException();
                } else if (response.getStatusCode() > 204 || response.getStatusCode() < 200) {
                    throw new RuntimeException("Error while importing " + f.getAbsolutePath()
                            + " (" + response.getStatusCode() + "): " + IOUtils.toString(response.getBody()));
                } else {
                    logger.info("Imported {}: {}", f.getAbsolutePath(), destinationUri);
                }
            } catch (FcrepoOperationFailedException ex) {
                throw new RuntimeException("Error importing " + f.getAbsolutePath() + ": " + ex.toString(), ex);
            } catch (IOException ex) {
                throw new RuntimeException(
                        "Error reading or parsing " + f.getAbsolutePath() + ": " + ex.toString(), ex);
            } catch (URISyntaxException ex) {
                throw new RuntimeException("Error building URI for " + f.getAbsolutePath() + ": " + ex.toString(), ex);
            }
        }
    }

    private Model parseFile(final File f) throws IOException {
        final URI source = config.getSource();
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

    private Model sanitize(final Model model) throws FcrepoOperationFailedException {
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
            } else if (s.getObject().isResource()) {
                // make sure that referenced repository objects exist
                final String obj = s.getResource().toString();
                if (obj.startsWith(config.getResource().toString())) {
                    ensureExists(URI.create(obj));
                }
            }
        }
        return model.remove(remove);
    }

    /**
     * Make sure that a URI exists in the repository.
     */
    private void ensureExists(final URI uri) throws FcrepoOperationFailedException {
        final String rdf = "<> a <http://www.w3.org/ns/ldp#Container> .";
        final FcrepoResponse response = client().put(uri)
                .body(new ByteArrayInputStream(rdf.getBytes()), "text/turtle")
                .perform();
        if (response.getStatusCode() > 204 && response.getStatusCode() != 409) {
            logger.error("Unexpected response when creating {} ({}): {}", uri,
                    response.getStatusCode(), response.getBody());
        }
    }


    private InputStream modelToStream(final Model model) {
        final ByteArrayOutputStream buf = new ByteArrayOutputStream();
        model.write(buf, config.getRdfLanguage());
        return new ByteArrayInputStream(buf.toByteArray());
    }

    private String uriPathForFile(final File f, final File baseDir) throws URISyntaxException {
        String relative = baseDir.toPath().relativize(f.toPath()).toString();
        relative = TransferProcess.decodePath(relative);

        // for exported RDF, just remove the ".extension" and you have the encoded path
        if (relative.endsWith(config.getRdfExtension())) {
            relative = relative.substring(0, relative.length() - config.getRdfExtension().length());
        }

        return relative;
    }

    private File fileForURI(final URI uri) {
        return new File(config.getBinaryDirectory() + TransferProcess.decodePath(uri.getPath()) + BINARY_EXTENSION);
    }
}
