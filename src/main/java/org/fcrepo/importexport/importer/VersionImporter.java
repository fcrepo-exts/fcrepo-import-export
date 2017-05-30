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
import static org.apache.jena.riot.RDFLanguages.contentTypeToLang;
import static org.fcrepo.importexport.common.FcrepoConstants.BINARY_EXTENSION;
import static org.fcrepo.importexport.common.FcrepoConstants.CONTAINER;
import static org.fcrepo.importexport.common.FcrepoConstants.CONTAINS;
import static org.fcrepo.importexport.common.FcrepoConstants.CREATED_BY;
import static org.fcrepo.importexport.common.FcrepoConstants.CREATED_DATE;
import static org.fcrepo.importexport.common.FcrepoConstants.DESCRIBEDBY;
import static org.fcrepo.importexport.common.FcrepoConstants.EXTERNAL_RESOURCE_EXTENSION;
import static org.fcrepo.importexport.common.FcrepoConstants.FCR_METADATA_PATH;
import static org.fcrepo.importexport.common.FcrepoConstants.FCR_VERSIONS_PATH;
import static org.fcrepo.importexport.common.FcrepoConstants.HAS_MESSAGE_DIGEST;
import static org.fcrepo.importexport.common.FcrepoConstants.HAS_MIME_TYPE;
import static org.fcrepo.importexport.common.FcrepoConstants.HAS_SIZE;
import static org.fcrepo.importexport.common.FcrepoConstants.HAS_VERSION;
import static org.fcrepo.importexport.common.FcrepoConstants.HAS_VERSIONS;
import static org.fcrepo.importexport.common.FcrepoConstants.HAS_VERSION_LABEL;
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
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.jena.datatypes.xsd.XSDDateTime;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.riot.RDFDataMgr;
import org.fcrepo.client.FcrepoClient;
import org.fcrepo.client.FcrepoClient.FcrepoClientBuilder;
import org.fcrepo.client.FcrepoOperationFailedException;
import org.fcrepo.client.FcrepoResponse;
import org.fcrepo.client.PutBuilder;
import org.fcrepo.importexport.common.AuthenticationRequiredRuntimeException;
import org.fcrepo.importexport.common.Config;
import org.fcrepo.importexport.common.ResourceNotFoundRuntimeException;
import org.fcrepo.importexport.common.TransferProcess;
import org.fcrepo.importexport.common.URITranslationUtil;
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
    private final URITranslationUtil uriTranslator;

    private ImportResourceFactory importRescFactory;
    private Logger importLogger;

    private FcrepoClient client;

    private Bag bag;
    private MessageDigest sha1;
    private final Map<String, String> sha1FileMap;

    private URI repositoryRoot = null;
    private AtomicLong successCount = new AtomicLong(); // set to zero at start

    /**
     * Construct an importer
     * 
     * @param config
     * @param clientBuilder
     */
    public VersionImporter(final Config config, final FcrepoClientBuilder clientBuilder) {
        this.uriTranslator = new URITranslationUtil(config);

        importRescFactory = new ImportResourceFactory(config, uriTranslator);

        this.config = config;
        this.clientBuilder = clientBuilder;
        this.importLogger = config.getAuditLog();

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

        findRepositoryRoot(config.getResource());

        processImport(config.getResource());

        importLogger.info("Finished import... {} resources imported", successCount.get());
    }

    private void processImport(final URI resource) {
        final URI parentUri = parent(resource);
        final File importContainerDirectory = directoryForContainer(parentUri);

        importDirectory(importContainerDirectory);
    }

    private void importDirectory(final File directory) {
        // Group the files/directories in this directory up into resource entities
        final List<ImportResource> importResources = importRescFactory.createFromDirectory(directory);

        // Import each resource
        importResources.forEach(importResc -> {
            importResource(importResc);
        });
    }

    private void importResource(final ImportResource resc) {
        if (resc.isBinary()) {
            importBinaryResource(resc);
        } else {
            importContainerResource(resc);
        }
    }

    private void importContainerResource(final ImportResource resc) {
        final Set<URI> previousResourceUris = null;
        for (final ImportResource version : importRescFactory.createVersionResourceList(resc)) {
            // update description for containers other than root
            if (!isSkippableContainer(resc)) {
                importDescription(resc);
            }

            final File subDirectory = resc.getDirectory();
            if (subDirectory != null && subDirectory.exists()) {
                importDirectory(subDirectory);
            }

            if (resc.isVersion()) {
                createVersion(resc.getUri(), version.getId());
            }
        }
    }

    private boolean isSkippableContainer(final ImportResource impResource) {
        final Resource resource = impResource.getResource();
        return resource == null
                || resource.hasProperty(RDF_TYPE, REPOSITORY_ROOT)
                || resource.hasProperty(RDF_TYPE, PAIRTREE);
    }

    private void importBinaryResource(final ImportResource resc) {
        if (!config.isIncludeBinaries()) {
            return;
        }

        String previousChecksum = null;
        for (final ImportResource version : importRescFactory.createVersionResourceList(resc)) {
            try {
                // check to see if the checksum has changed since previous version
                final String currentChecksum = getBinaryChecksum(resc);
                if (!currentChecksum.equals(previousChecksum)) {
                    // Import the modified binary
                    importBinaryFile(resc);
                }

                // update metadata
                importDescription(resc);

                if (resc.isVersion()) {
                    createVersion(resc.getUri(), version.getId());
                }

                previousChecksum = currentChecksum;
            } catch (RuntimeException e) {
                logger.error("Failed to import binary {}", resc.getUri(), e);
            }
        }
    }

    private void createVersion(final URI uri, final String label) {
        final URI versionsUri = addRelativePath(uri, FCR_VERSIONS_PATH);
        try {
            final FcrepoResponse response = client().post(versionsUri).slug(label).perform();

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
        final URI destinationUri = resc.getUri();
        final String descriptionPath = resc.getMetadataFile().getAbsolutePath();
        try {
            final FcrepoResponse response = client().put(resc.getDescriptionUri())
                    .body(modelToStream(sanitize(model)), config.getRdfLanguage())
                    .preferLenient().perform();

            if (response.getStatusCode() == 401) {
                importLogger.error("Error importing {} to {}, 401 Unauthorized",
                        descriptionPath, destinationUri);
                throw new AuthenticationRequiredRuntimeException();
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

    private void importBinaryFile(final ImportResource resc) {
        final URI binaryURI = resc.getUri();
        final Model model = resc.getModel();
        final String contentType = model
                .getProperty(createResource(binaryURI.toString()), HAS_MIME_TYPE).getString();
        final File binaryFile = resc.getBinary();
        FcrepoResponse binaryResponse;
        try {
            binaryResponse = binaryBuilder(binaryURI, binaryFile, contentType, model).perform();

            if (binaryResponse.getStatusCode() == 410 && config.overwriteTombstones()) {
                // Collided with tombstone, cleanup and try again
                deleteTombstone(binaryResponse);
                binaryResponse = binaryBuilder(binaryURI, binaryFile, contentType, model).perform();
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
                throw new RuntimeException(message);
            }
        } catch (FcrepoOperationFailedException | IOException e) {
           throw new RuntimeException("Error while importing " + binaryFile.getAbsolutePath(), e);
        }
    }

    @Deprecated
    private FcrepoResponse importBinary(final URI binaryURI, final File binaryFile, final Model model)
            throws FcrepoOperationFailedException, IOException {
        final String contentType = model.getProperty(createResource(binaryURI.toString()), HAS_MIME_TYPE).getString();
        // final File binaryFile =  fileForBinaryURI(binaryURI, external(contentType));
        final FcrepoResponse binaryResponse = binaryBuilder(binaryURI, binaryFile, contentType, model).perform();
        if (binaryResponse.getStatusCode() == 201 || binaryResponse.getStatusCode() == 204) {
            logger.info("Imported binary: {}", binaryURI);
            importLogger.info("import {} to {}", binaryFile.getAbsolutePath(), binaryURI);
            successCount.incrementAndGet();

            final URI descriptionURI = binaryResponse.getLinkHeaders("describedby").get(0);
            return client().put(descriptionURI).body(modelToStream(sanitize(model)), config.getRdfLanguage())
                .preferLenient().perform();
        } else if (binaryResponse.getStatusCode() == 410 && config.overwriteTombstones()) {
            deleteTombstone(binaryResponse);
            return binaryBuilder(binaryURI, binaryFile, contentType, model).perform();
        } else {
            logger.error("Error while importing {} ({}): {}", binaryFile.getAbsolutePath(),
                    binaryResponse.getStatusCode(), IOUtils.toString(binaryResponse.getBody()));
            return null;
        }
    }

    private String getBinaryChecksum(final ImportResource resc) {
        final File binaryFile = resc.getBinary();
        final URI binaryURI = resc.getUri();
        if (sha1FileMap != null) {
            // Use the bagIt checksum
            final String checksum = sha1FileMap.get(binaryFile.getAbsolutePath());
            logger.debug("Using Bagit checksum ({}) for file ({}): {}", checksum, binaryFile.getPath(), binaryURI);
            return checksum;
        } else {
            return resc.getResource().getProperty(HAS_MESSAGE_DIGEST).getResource().getURI().replaceAll(".*:","");
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

    private void deleteTombstone(final FcrepoResponse response) throws FcrepoOperationFailedException {
        final URI tombstone = response.getLinkHeaders("hasTombstone").get(0);
        if (tombstone != null) {
            client().delete(tombstone).perform();
        } else {
            String uri = response.getUrl().toString();
            if (uri.endsWith("/")) {
                uri = uri.substring(0, uri.length() - 1);
            }
            final URI parent = URI.create(uri.substring(0, uri.lastIndexOf("/", uri.length() - 1)));
            deleteTombstone(client().head(parent).perform());
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

    private static String baseURI(final URI uri) {
        final String base = uri.toString().replaceFirst(uri.getPath() + "$", "");
        return (base.endsWith("/")) ? base : base + "/";
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

    private boolean external(final String contentType) {
        return contentType.startsWith("message/external-body");
    }

    private File directoryForContainer(final URI uri) {
        return TransferProcess.directoryForContainer(withSlash(uri), config.getSourcePath(),
                config.getDestinationPath(), config.getBaseDirectory());
    }

    private static URI withSlash(final URI uri) {
        return uri.toString().endsWith("/") ? uri : URI.create(uri.toString() + "/");
    }

    /**
     * Method to find and set the repository root from the resource uri.
     * @param uri the URI for the resource
     * @throws IOException
     * @throws FcrepoOperationFailedException
     */
    private void findRepositoryRoot(final URI uri) {
        repositoryRoot = uri;
        try {
            repositoryRoot = uri;
            if (!isRepositoryRoot(uri, client(), config)) {
                findRepositoryRoot(URI.create(repositoryRoot.toString().substring(0,
                        repositoryRoot.toString().lastIndexOf("/"))));
            }
        } catch (ResourceNotFoundRuntimeException ex) {
            // The targeted resource that need to be imported next
            findRepositoryRoot(URI.create(repositoryRoot.toString().substring(0,
                    repositoryRoot.toString().lastIndexOf("/"))));
        } catch (final IOException ex) {
            throw new RuntimeException("Error finding repository root " + repositoryRoot, ex);
        } catch (final FcrepoOperationFailedException ex) {
            throw new RuntimeException("Error finding repository root " + repositoryRoot, ex);
        }
        logger.debug("Repository root {}", repositoryRoot);
    }

    private static Model parseStream(final InputStream in, final Config config) throws IOException {
        final SubjectMappingStreamRDF mapper = new SubjectMappingStreamRDF(config.getSource(),
                                                                           config.getDestination());
        try (final InputStream in2 = in) {
            RDFDataMgr.parse(mapper, in2, contentTypeToLang(config.getRdfLanguage()));
        }
        return mapper.getModel();
    }

    public static class ImportResource {
        private final Config config;

        private final String id;
        private final List<File> files;
        private final URI uri;
        private URI descriptionUri;
        private Model model;
        private Resource resource;
        private boolean isVersion;

        /**
         * Construct new ImportResource
         * 
         * @param id
         * @param uri
         * @param config
         */
        public ImportResource(final String id, final URI uri, final Config config) {
            this.id = id;
            this.config = config;
            this.files = new ArrayList<>();
            this.uri = uri;
        }

        /**
         * Get Id for this resource
         * 
         * @return
         */
        public String getId() {
            return id;
        }

        /**
         * Get the URI for this resource
         * 
         * @return
         */
        public URI getUri() {
            return uri;
        }

        /**
         * Get the URI for metadata for this resource
         * 
         * @return
         */
        public URI getDescriptionUri() {
            if (descriptionUri == null) {
                if (isBinary()) {
                    descriptionUri = addRelativePath(uri, FCR_METADATA_PATH);
                } else {
                    descriptionUri = uri;
                }
            }
            return descriptionUri;
        }

        /**
         * Get a list of all files associated with this resource
         * 
         * @return
         */
        public List<File> getFiles() {
            return files;
        }

        /**
         * Associate a file with this resource.
         * 
         * @param file
         */
        public void addFile(final File file) {
            files.add(file);
        }

        /**
         * Test if this resource is a binary
         * 
         * @return
         */
        public boolean isBinary() {
            return getBinary() != null;
        }

        /**
         * Get the binary file for this resource
         * 
         * @return the binary for this resource or null if not found
         */
        public File getBinary() {
            return files.stream()
                    .filter(f -> f.getName().equals(id + BINARY_EXTENSION)
                            || f.getName().equals(id + EXTERNAL_RESOURCE_EXTENSION))
                    .findFirst().orElse(null);
        }

        /**
         * Get the directory for subpaths belonging to this resource
         * 
         * @return
         */
        public File getDirectory() {
            return files.stream()
                    .filter(File::isDirectory)
                    .findFirst()
                    .orElse(null);
        }

        /**
         * Get the model containing properties assigned to this resource
         * 
         * @return
         */
        public Model getModel() {
            if (model == null) {
                final File mdFile = getMetadataFile();
                if (mdFile == null) {
                    return null;
                }
                try {
                    model = parseStream(new FileInputStream(mdFile), config);
                } catch (IOException e) {
                    throw new RuntimeException("Failed to read model for " + id, e);
                }
            }
            return model;
        }

        /**
         * Get the file containing metadata for this resource
         * 
         * @return
         */
        public File getMetadataFile() {
            if (isBinary()) {
                // For a file, retrieve the fcr:metadata file from its subdirectory
                return Arrays.stream(getDirectory().listFiles())
                        .filter(f -> f.getName().endsWith(config.getRdfExtension()))
                        .findFirst().orElse(null);
            }
            // For containers, find the model file
            return files.stream()
                    .filter(f -> f.getName().equals(id + config.getRdfExtension()))
                    .findFirst().orElse(null);
        }

        /**
         * Get the resource representing this ImportResource from its model
         * 
         * @return
         */
        public Resource getResource() {
            if (resource == null) {
                final Model model = getModel();
                if (model == null) {
                    return null;
                }
                resource = model.getResource(uri.toString());
            }
            return resource;
        }

        /**
         * Return true if this resource is a version
         * 
         * @return
         */
        public boolean isVersion() {
            return isVersion;
        }

        /**
         * Setter for isVersion property
         * 
         * @param isVersion value to set
         */
        public void setIsVersion(final boolean isVersion) {
            this.isVersion = isVersion;
        }

        /**
         * Get the file containing version info for this resource.
         * 
         * @return
         */
        private File getVersionsFile() {
            return new File(getDirectory(), VERSIONS_FILENAME + config.getRdfExtension());
        }

        /**
         * Get the directory containing versions of this resource
         * 
         * @return
         */
        public File getVersionsDirectory() {
            return new File(getDirectory(), VERSIONS_FILENAME);
        }

        /**
         * Return true if this resource is versioned
         * 
         * @return
         */
        public boolean hasVersions() {
            final Resource resc = getResource();
            return resc.hasProperty(HAS_VERSIONS) && getVersionsFile().exists();
        }

        private List<String> extractOrderedVersionLabels(final File versionsFile) {
            try {
                final Model versionsModel = parseStream(new FileInputStream(versionsFile), config);

                final List<Entry<Long, String>> versions = new ArrayList<>();
                versionsModel.listObjectsOfProperty(HAS_VERSION).forEachRemaining(v -> {
                    final Resource versionResc = v.asResource();
                    final XSDDateTime created = (XSDDateTime) versionResc
                            .getProperty(CREATED_DATE).getLiteral().getValue();
                    final long createdMillis = created.asCalendar().getTimeInMillis();

                    final String versionLabel = versionResc.getProperty(HAS_VERSION_LABEL).getString();

                    versions.add(new SimpleEntry<>(createdMillis, versionLabel));
                });

                return versions.stream()
                        .sorted((v1, v2) -> Long.compare(v1.getKey(), v2.getKey()))
                        .map(v -> v.getValue())
                        .collect(Collectors.toList());

            } catch (IOException ex) {
                throw new RuntimeException(
                        "Error reading or parsing " + versionsFile.getAbsolutePath() + ": " + ex.toString(), ex);
            }
        }

        /**
         * Get the labels of all versions of this resource, in chronological order.
         * 
         * @return
         */
        public List<String> getVersionLabels() {
            if (!hasVersions()) {
                return Collections.emptyList();
            }

            final File versionsFile = getVersionsFile();
            // Get a list of version labels ordered chronologically
            return extractOrderedVersionLabels(versionsFile);
        }
    }
}
