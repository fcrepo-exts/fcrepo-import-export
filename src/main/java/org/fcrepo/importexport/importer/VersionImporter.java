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

import static org.apache.jena.rdf.model.ResourceFactory.createResource;
import static org.fcrepo.importexport.common.FcrepoConstants.CONTAINER;
import static org.fcrepo.importexport.common.FcrepoConstants.CONTAINS;
import static org.fcrepo.importexport.common.FcrepoConstants.CREATED_BY;
import static org.fcrepo.importexport.common.FcrepoConstants.CREATED_DATE;
import static org.fcrepo.importexport.common.FcrepoConstants.DESCRIBEDBY;
import static org.fcrepo.importexport.common.FcrepoConstants.FCR_VERSIONS_PATH;
import static org.fcrepo.importexport.common.FcrepoConstants.HAS_MESSAGE_DIGEST;
import static org.fcrepo.importexport.common.FcrepoConstants.HAS_MIME_TYPE;
import static org.fcrepo.importexport.common.FcrepoConstants.HAS_SIZE;
import static org.fcrepo.importexport.common.FcrepoConstants.LAST_MODIFIED_BY;
import static org.fcrepo.importexport.common.FcrepoConstants.LAST_MODIFIED_DATE;
import static org.fcrepo.importexport.common.FcrepoConstants.NON_RDF_SOURCE;
import static org.fcrepo.importexport.common.FcrepoConstants.PAIRTREE;
import static org.fcrepo.importexport.common.FcrepoConstants.RDF_SOURCE;
import static org.fcrepo.importexport.common.FcrepoConstants.RDF_TYPE;
import static org.fcrepo.importexport.common.FcrepoConstants.REPOSITORY_NAMESPACE;
import static org.fcrepo.importexport.common.FcrepoConstants.REPOSITORY_ROOT;
import static org.fcrepo.importexport.common.TransferProcess.fileForBinary;
import static org.fcrepo.importexport.common.TransferProcess.fileForExternalResources;
import static org.fcrepo.importexport.common.TransferProcess.fileForURI;
import static org.fcrepo.importexport.common.TransferProcess.isRepositoryRoot;
import static org.fcrepo.importexport.common.URITranslationUtil.addRelativePath;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.io.IOUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.fcrepo.client.FcrepoClient;
import org.fcrepo.client.FcrepoClient.FcrepoClientBuilder;
import org.fcrepo.client.FcrepoOperationFailedException;
import org.fcrepo.client.FcrepoResponse;
import org.fcrepo.client.PutBuilder;
import org.fcrepo.importexport.common.AuthenticationRequiredRuntimeException;
import org.fcrepo.importexport.common.BinaryImportException;
import org.fcrepo.importexport.common.Config;
import org.fcrepo.importexport.common.ResourceGoneRuntimeException;
import org.fcrepo.importexport.common.ResourceNotFoundRuntimeException;
import org.fcrepo.importexport.common.TransferProcess;
import org.slf4j.Logger;

import gov.loc.repository.bagit.domain.Bag;

/**
 * Fedora Import Utility
 *
 * @author lsitu
 * @author awoods
 * @author escowles
 * @author bbpennel
 * @since 2016-08-29
 */
public class VersionImporter implements TransferProcess{

    private static final String VERSIONS_FILENAME = "fcr%3Aversions";

    private static final Logger logger = getLogger(VersionImporter.class);
    private Config config;
    protected FcrepoClientBuilder clientBuilder;

    private Logger importLogger;

    final Map<String, String> versionedLabels;

    private Bag bag;
    private MessageDigest sha1;
    private final Map<String, String> sha1FileMap;

    private URI repositoryRoot = null;
    private AtomicLong successCount = new AtomicLong(); // set to zero at start

    /**
     * Construct an importer
     *
     * @param config config
     * @param clientBuilder fcrepo client builder
     */
    public VersionImporter(final Config config, final FcrepoClientBuilder clientBuilder) {

        this.config = config;
        this.clientBuilder = clientBuilder;
        this.importLogger = config.getAuditLog();
        this.versionedLabels = new HashMap<>();

        if (config.getBagProfile() == null) {
            this.bag = null;
            this.sha1 = null;
            this.sha1FileMap = null;
        } else {
            final File bagdir = config.getBaseDirectory().getParentFile();
            // TODO: Maybe use this once we get an updated release of bagit-java library
            //if (verifyBag(bagdir)) {
            final Path manifestPath = Paths.get(bagdir.getAbsolutePath()).resolve("manifest-sha1.txt");
            this.sha1FileMap = TransferProcess.getSha1FileMap(bagdir, manifestPath);
            try {
                this.sha1 = MessageDigest.getInstance("SHA-1");
            } catch (NoSuchAlgorithmException e) {
                // never happens with known algorithm names
            }
        }
    }

    private FcrepoClient client() {
        if (config.getUsername() != null) {
            clientBuilder.credentials(config.getUsername(), config.getPassword());
        }
        return clientBuilder.build();
    }

    @Override
    public void run() {
        logger.info("Running importer...");

        repositoryRoot = findRepositoryRoot(config.getResource());

        processImport(config.getResource());

        importLogger.info("Finished import... {} resources imported", successCount.get());
    }

    private void processImport(final URI resource) {
        final File importContainerMetadataFile = fileForContainerURI(resource);
        final File importContainerDirectory = importContainerMetadataFile.getParentFile();

        try {
            final Iterator<ImportEvent> rescIt = new ChronologicalImportEventIterator(
                    importContainerDirectory, config);
            while (rescIt.hasNext()) {
                final ImportEvent impEvent = rescIt.next();

                if (impEvent instanceof ImportResource) {
                    importResource((ImportResource) impEvent);
                } else if (impEvent instanceof ImportVersion) {
                    final ImportVersion version = (ImportVersion) impEvent;
                    createVersion(version.getMappedUri(), version.getLabel());
                }
            }
        } catch (FcrepoOperationFailedException e) {
            throw new RuntimeException("Failed to import", e);
        } catch (IOException e) {
            throw new RuntimeException("", e);
        }
    }

    private void importResource(final ImportResource resc) throws FcrepoOperationFailedException, IOException {
        if (resc.isBinary()) {
            importBinaryResource(resc);
        } else {
            importContainerResource(resc);
        }
    }

    private void importContainerResource(final ImportResource resc) throws FcrepoOperationFailedException {
        if (!isSkippableContainer(resc)) {
            try {
                importDescription(resc);
            } catch (ResourceGoneRuntimeException e) {
                if (config.overwriteTombstones()) {
                    deleteTombstone(e.getResourceUri(), e.getTombstone());
                    importDescription(resc);
                } else {
                    throw e;
                }
            }
        }
    }

    private boolean isSkippableContainer(final ImportResource impResource) {
        final Resource resource = impResource.getResource();
        return resource == null
                || resource.hasProperty(RDF_TYPE, REPOSITORY_ROOT)
                || resource.hasProperty(RDF_TYPE, PAIRTREE);
    }

    private void importBinaryResource(final ImportResource resc) throws FcrepoOperationFailedException, IOException {
        if (!config.isIncludeBinaries()) {
            return;
        }

        try {
            importBinaryFile(resc);
            // update metadata
            importDescription(resc);
        } catch (ResourceGoneRuntimeException e) {
            deleteTombstone(e.getResourceUri(), e.getTombstone());

            importBinaryFile(resc);
            importDescription(resc);
        } catch (BinaryImportException e) {
            logger.error(e.getMessage());
        }
    }

    private void createVersion(final URI uri, final String label) {
        final URI versionsUri = addRelativePath(uri, FCR_VERSIONS_PATH);
        try {
            final FcrepoResponse response = client().post(versionsUri)
                    .slug(label)
                    .perform();

            if (response.getStatusCode() == 201) {
                logger.info("Created version {} of {}", label, uri);
                importLogger.info("Created version {} of {}", label, uri);
                successCount.incrementAndGet();
            } else {
                throw new RuntimeException("Error creating version " + label + " of " + uri
                        + " (" + response.getStatusCode() + "): " + IOUtils.toString(response.getBody()));
            }
        } catch (FcrepoOperationFailedException | IOException e) {
            throw new RuntimeException("Error creating version " + label + " of " + uri, e);
        }
    }

    private void importDescription(final ImportResource resc) {
        final Model model = resc.getModel();
        final URI destinationUri = resc.getMappedUri();
        final String descriptionPath = resc.getDescriptionFile().getAbsolutePath();
        try {
            final FcrepoResponse response = client().put(resc.getDescriptionUri())
                    .body(modelToStream(sanitize(model)), config.getRdfLanguage())
                    .preferLenient().perform();

            if (response.getStatusCode() == 401) {
                importLogger.error("Error importing {} to {}, 401 Unauthorized",
                        descriptionPath, destinationUri);
                throw new AuthenticationRequiredRuntimeException();
            } else if (response.getStatusCode() == 410 && config.overwriteTombstones()) {
                throw new ResourceGoneRuntimeException(response);
            } else if (response.getStatusCode() > 204 || response.getStatusCode() < 200) {
                final String message = "Error while importing " + descriptionPath + " ("
                        + response.getStatusCode() + "): " + IOUtils.toString(response.getBody());
                logger.error(message);
                importLogger.error("Error importing {} to {}, received {}", descriptionPath, destinationUri,
                    response.getStatusCode());
            } else {
                logger.info("Imported {}: {}", descriptionPath, destinationUri);
                importLogger.info("import {} to {}", descriptionPath, destinationUri);
                successCount.incrementAndGet();
            }
        } catch (FcrepoOperationFailedException e) {
            importLogger.error(String.format("Error importing {} to {}, Message: {}",
                    descriptionPath, destinationUri, e.getMessage()), e);
            throw new RuntimeException(
                    "Error importing " + descriptionPath + ": " + e.toString(), e);
        } catch (IOException e) {
            importLogger.error(String.format("Error reading/parsing {} to {}, Message: {}",
                    descriptionPath, destinationUri, e.getMessage()), e);
            throw new RuntimeException(
                    "Error reading or parsing " + descriptionPath + ": " + e.toString(), e);
        }
    }

    private void importBinaryFile(final ImportResource resc) throws FcrepoOperationFailedException, IOException {
        final URI binaryURI = resc.getMappedUri();
        final Model model = resc.getModel();
        final String contentType = model
                .getProperty(createResource(binaryURI.toString()), HAS_MIME_TYPE).getString();
        final File binaryFile = resc.getBinary();
        FcrepoResponse binaryResponse;
        binaryResponse = binaryBuilder(binaryURI, binaryFile, contentType, model).perform();

        if (binaryResponse.getStatusCode() == 410 && config.overwriteTombstones()) {
            throw new ResourceGoneRuntimeException(binaryResponse);
        }

        // Check for success importing file
        if (binaryResponse.getStatusCode() == 201 || binaryResponse.getStatusCode() == 204) {
            logger.info("Imported binary: {}", binaryURI);
            importLogger.info("import {} to {}", binaryFile.getAbsolutePath(), binaryURI);
            successCount.incrementAndGet();
        } else {
            // Failed to import file, throw exception
            final String message = String.format("Error while importing %s (%s): %s", binaryFile.getAbsolutePath(),
                    binaryResponse.getStatusCode(), IOUtils.toString(binaryResponse.getBody()));
            throw new BinaryImportException(message);
        }
    }

    private PutBuilder binaryBuilder(final URI binaryURI, final File binaryFile, final String contentType,
            final Model model) throws FcrepoOperationFailedException, IOException {
        final InputStream contentStream;
        if (external(contentType)) {
            contentStream = new ByteArrayInputStream(new byte[]{});
        } else {
            contentStream = new FileInputStream(binaryFile);
        }
        PutBuilder builder = client().put(binaryURI).body(contentStream, contentType);
        if (!external(contentType)) {
            if (sha1FileMap != null) {
                // Use the bagIt checksum
                final String checksum = sha1FileMap.get(binaryFile.getAbsolutePath());
                logger.debug("Using Bagit checksum ({}) for file ({}): {}", checksum, binaryFile.getPath(), binaryURI);
                builder = builder.digest(checksum);
            } else {
                builder = builder.digest(model.getProperty(createResource(binaryURI.toString()), HAS_MESSAGE_DIGEST)
                                          .getObject().toString().replaceAll(".*:",""));
            }
        }
        return builder;
    }

    private void deleteTombstone(final URI rescUri, final URI tombstone) throws FcrepoOperationFailedException {
        if (tombstone != null) {
            client().delete(tombstone).perform();
        } else {
            String uri = rescUri.toString();
            if (uri.endsWith("/")) {
                uri = uri.substring(0, uri.length() - 1);
            }
            final URI parent = URI.create(uri.substring(0, uri.lastIndexOf("/", uri.length() - 1)));
            final FcrepoResponse response = client().head(parent).perform();
            deleteTombstone(parent, response.getLinkHeaders("hasTombstone").get(0));
        }
    }

    /**
     * Removes statements from the provided model that affect triples that need not be (and indeed
     * cannot be) modified directly through PUT, POST or PATCH requests to fedora.
     *
     * Certain triples included in a resource from fedora cannot be explicitly stored, but because
     * they're derived from other content that *can* be stored will still appear identical when the
     * other RDF and content is ingested.  Examples include those properties that reflect innate
     * characteristics of binary resources like file size and message digest,  Or triples that
     * represent characteristics of rdf resources like the number of children, whether it has
     * versions and some of the types.
     *
     * @param model the RDF statements about an exported resource
     * @return the provided model updated to omit statements that may not be updated directly through
     *         the fedora API
     * @throws IOException
     * @throws FcrepoOperationFailedException
     */
    private Model sanitize(final Model model) throws IOException, FcrepoOperationFailedException {
        final List<Statement> remove = new ArrayList<>();
        for (final StmtIterator it = model.listStatements(); it.hasNext(); ) {
            final Statement s = it.nextStatement();

            if ((s.getPredicate().getNameSpace().equals(REPOSITORY_NAMESPACE) && !relaxedPredicate(s.getPredicate()))
                    || s.getSubject().getURI().endsWith("fcr:export?format=jcr/xml")
                    || s.getSubject().getURI().equals(REPOSITORY_NAMESPACE + "jcr/xml")
                    || s.getPredicate().equals(DESCRIBEDBY)
                    || s.getPredicate().equals(CONTAINS)
                    || s.getPredicate().equals(HAS_MESSAGE_DIGEST)
                    || s.getPredicate().equals(HAS_SIZE)
                    || (s.getPredicate().equals(RDF_TYPE) && forbiddenType(s.getResource()))) {
                remove.add(s);
            } else if (s.getObject().isResource()) {
                // make sure that referenced repository objects exist
                final String obj = s.getResource().toString();
                if (obj.startsWith(repositoryRoot.toString())) {
                    ensureExists(URI.create(obj));
                }
            }
        }
        return model.remove(remove);
    }

    /**
     * RDF type URIs that have special meaning in fedora and that are managed by fedora and
     * not eligible for modification through the fedora API.
     * @param resource the URI resource that is part of an rdf:type statement
     * @return true if the resource represents a type that may not be added/removed explicitly
     */
    private boolean forbiddenType(final Resource resource) {
         return resource.getNameSpace().equals(REPOSITORY_NAMESPACE)
             || resource.getURI().equals(CONTAINER.getURI())
             || resource.getURI().equals(NON_RDF_SOURCE.getURI())
             || resource.getURI().equals(RDF_SOURCE.getURI());
    }

    /**
     * Tests whether the provided property is one of the small subset of the predicates within the
     * repository namespace that may be modified.  This method always returns false if the
     * import/export configuration is set to "legacy" mode.
     * @param p the property (predicate) to test
     * @return true if the predicate is of the type that can be modified
     */
    private boolean relaxedPredicate(final Property p) {
        return !config.isLegacy() && (p.equals(CREATED_BY) || p.equals(CREATED_DATE)
                || p.equals(LAST_MODIFIED_BY) || p.equals(LAST_MODIFIED_DATE));
    }

    /**
     * Make sure that a URI exists in the repository.
     */
    private void ensureExists(final URI uri) throws IOException, FcrepoOperationFailedException {
        try (FcrepoResponse response = client().head(uri).perform()) {
            if (response.getStatusCode() != 200) {
                makePlaceholder(uri);
            }
        }
    }

    private void makePlaceholder(final URI uri) throws IOException, FcrepoOperationFailedException {
        ensureExists(parent(uri));

        final FcrepoResponse response;
        if (fileForBinaryURI(uri, false).exists() || fileForBinaryURI(uri, true).exists()) {
            response = client().put(uri).body(new ByteArrayInputStream(new byte[]{})).perform();
        } else if (fileForContainerURI(uri).exists()) {
            response = client().put(uri).body(new ByteArrayInputStream(
                    "<> a <http://www.w3.org/ns/ldp#Container> .".getBytes()), "text/turtle").perform();
        } else {
            return;
        }

        if (response.getStatusCode() != 201) {
            logger.error("Unexpected response when creating {} ({}): {}", uri,
                    response.getStatusCode(), response.getBody());
        }
    }

    private static URI parent(final URI uri) {
        String s = uri.toString();
        if (s.endsWith("/")) {
            s = s.substring(0, s.length() - 1);
        }
        return URI.create(s.substring(0, s.lastIndexOf("/")));
    }

    private InputStream modelToStream(final Model model) {
        final ByteArrayOutputStream buf = new ByteArrayOutputStream();
        model.write(buf, config.getRdfLanguage());
        return new ByteArrayInputStream(buf.toByteArray());
    }

    private File fileForBinaryURI(final URI uri, final boolean external) {
        if (external) {
            return fileForExternalResources(uri, config.getSourcePath(), config.getDestinationPath(),
                    config.getBaseDirectory());
        } else {
            return fileForBinary(uri, config.getSourcePath(), config.getDestinationPath(),
                    config.getBaseDirectory());
        }
    }

    private File fileForContainerURI(final URI uri) {
        return fileForURI(withSlash(uri), config.getSourcePath(), config.getDestinationPath(),
                config.getBaseDirectory(), config.getRdfExtension());
    }

    private File directoryForContainer(final URI uri) {
        return TransferProcess.directoryForContainer(withSlash(uri), config.getSourcePath(),
                config.getDestinationPath(), config.getBaseDirectory());
    }

    private boolean external(final String contentType) {
        return contentType.startsWith("message/external-body");
    }

    private static URI withSlash(final URI uri) {
        return uri.toString().endsWith("/") ? uri : URI.create(uri.toString() + "/");
    }

    /**
     * Method to find and set the repository root from the resource uri.
     *
     * Note: This method is public to allow access for testing purposes.
     * @param uri the URI for the resource
     * @return The URI of the repository root, or the URI with path removed if neither the URI nor none of its
     *         parent paths declare themselves fedora:RepositoryRoot.
     */
    public URI findRepositoryRoot(final URI uri) {
        final String s = uri.toString();
        final URI u = s.endsWith("/") ? URI.create(s.substring(0, s.length() - 1)) : uri;

        try {
            if (u.getPath() == null || u.getPath().equals("") || isRepositoryRoot(u, client(), config)) {
                return u;
            } else {
                return findRepositoryRoot(URI.create(u.toString().substring(0,
                        u.toString().lastIndexOf("/"))));
            }
        } catch (ResourceNotFoundRuntimeException ex) {
            // The targeted resource that need to be imported next
            return findRepositoryRoot(URI.create(u.toString().substring(0,
                    u.toString().lastIndexOf("/"))));
        } catch (final IOException ex) {
            throw new RuntimeException("Error finding repository root " + u, ex);
        } catch (final FcrepoOperationFailedException ex) {
            throw new RuntimeException("Error finding repository root " + u, ex);
        }
    }
}
