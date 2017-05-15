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
import static org.fcrepo.importexport.common.FcrepoConstants.BINARY_EXTENSION;
import static org.fcrepo.importexport.common.FcrepoConstants.CONTAINS;
import static org.fcrepo.importexport.common.FcrepoConstants.CONTAINER;
import static org.fcrepo.importexport.common.FcrepoConstants.DESCRIBEDBY;
import static org.fcrepo.importexport.common.FcrepoConstants.EXTERNAL_RESOURCE_EXTENSION;
import static org.fcrepo.importexport.common.FcrepoConstants.HAS_MESSAGE_DIGEST;
import static org.fcrepo.importexport.common.FcrepoConstants.HAS_MIME_TYPE;
import static org.fcrepo.importexport.common.FcrepoConstants.HAS_SIZE;
import static org.fcrepo.importexport.common.FcrepoConstants.MEMBERSHIP_RESOURCE;
import static org.fcrepo.importexport.common.FcrepoConstants.NON_RDF_SOURCE;
import static org.fcrepo.importexport.common.FcrepoConstants.PAIRTREE;
import static org.fcrepo.importexport.common.FcrepoConstants.RDF_SOURCE;
import static org.fcrepo.importexport.common.FcrepoConstants.RDF_TYPE;
import static org.fcrepo.importexport.common.FcrepoConstants.REPOSITORY_NAMESPACE;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.fcrepo.client.FcrepoClient;
import org.fcrepo.client.FcrepoOperationFailedException;
import org.fcrepo.client.FcrepoResponse;
import org.fcrepo.client.PutBuilder;
import org.fcrepo.importexport.common.AuthenticationRequiredRuntimeException;
import org.fcrepo.importexport.common.Config;
import org.fcrepo.importexport.common.TransferProcess;

import org.apache.commons.io.IOUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.ResIterator;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RiotException;
import org.slf4j.Logger;

import gov.loc.repository.bagit.domain.Bag;
import gov.loc.repository.bagit.reader.BagReader;
import gov.loc.repository.bagit.verify.BagVerifier;

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
    private final List<URI> membershipResources = new ArrayList<>();

    private Bag bag;

    private MessageDigest sha1;

    private Map<File, String> sha1FileMap;

    private Logger importLogger;
    private AtomicLong successCount = new AtomicLong(); // set to zero at start

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
        this.importLogger = config.getAuditLog();
        if (config.getBagProfile() == null) {
            this.bag = null;
            this.sha1 = null;
            this.sha1FileMap = null;
        } else {

            try {
                final File bagdir = config.getBaseDirectory().getParentFile();
                // TODO: Maybe use this once we get an updated release of bagit-java library
                //if (verifyBag(bagdir)) {
                final Path manifestPath = Paths.get(bagdir.getAbsolutePath()).resolve("manifest-sha1.txt");
                this.sha1FileMap = TransferProcess.getSha1FileMap(bagdir, manifestPath);
                this.sha1 = MessageDigest.getInstance("SHA-1");
                // }
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

    /**
     * This method does the import
     */
    @Override
    public void run() {
        logger.info("Running importer...");
        final File importContainerMetadataFile = fileForContainerURI(config.getResource());
        importContainerDirectory = TransferProcess.directoryForContainer(config.getResource(),
                config.getBaseDirectory());

        discoverMembershipResources(importContainerDirectory);

        if (!importContainerMetadataFile.exists()) {
            logger.debug("No container exists in the metadata directory {} for the requested resource {},"
                    + " importing all contained resources instead.", importContainerMetadataFile.getPath(),
                    config.getResource());
        } else {
            importFile(importContainerMetadataFile);
        }
        importDirectory(importContainerDirectory);

        importMembershipResources();
        importLogger.info("Finished import... {} resources imported", successCount.get());
    }

    private void discoverMembershipResources(final File dir) {
        if (dir.listFiles() != null) {
            stream(dir.listFiles()).filter(File::isFile).forEach(f -> parseMembershipResources(f));
            stream(dir.listFiles()).filter(File::isDirectory).forEach(d -> discoverMembershipResources(d));
        }
    }

    private void parseMembershipResources(final File f) {
        // skip files that aren't RDF
        if (!f.getName().endsWith(config.getRdfExtension())) {
            return;
        }

        try {
            final Model model = parseStream(new FileInputStream(f));
            if (model.contains(null, MEMBERSHIP_RESOURCE, (RDFNode)null)) {
                    model.listObjectsOfProperty(MEMBERSHIP_RESOURCE).forEachRemaining(node -> {
                        logger.info("Membership resource: {}", node);
                        membershipResources.add(URI.create(node.toString()));
                    });
            }
        } catch (final IOException e) {
            throw new RuntimeException("Error reading file: " + f.getAbsolutePath() + ": " + e.toString());
        } catch (final RiotException e) {
            throw new RuntimeException("Error parsing RDF: " + f.getAbsolutePath() + ": " + e.toString());
        }
    }

    private void importMembershipResources() {
        membershipResources.stream().forEach(uri -> importMembershipResource(uri));
    }

    private void importMembershipResource(final URI uri) {
        final File f = fileForContainerURI(uri);
        try {
            final Model diskModel = parseStream(new FileInputStream(f));
            final Model repoModel = parseStream(client().get(uri).perform().getBody());
            final FcrepoResponse response = importContainer(uri, sanitize(diskModel.difference(repoModel)));
            if (response.getStatusCode() == 401) {
                importLogger.error("Error importing {} to {}, 401 Unauthorized", f.getAbsolutePath(), uri);
                throw new AuthenticationRequiredRuntimeException();
            } else if (response.getStatusCode() > 204 || response.getStatusCode() < 200) {
                importLogger.error("Error importing {} to {}, received {}", f.getAbsolutePath(), uri,
                    response.getStatusCode());
                throw new RuntimeException("Error while importing membership resource " + f.getAbsolutePath()
                        + " (" + response.getStatusCode() + "): " + IOUtils.toString(response.getBody()));
            } else {
                logger.info("Imported membership resource {}: {}", f.getAbsolutePath(), uri);
                importLogger.info("import {} to {}", f.getAbsolutePath(), uri);
                successCount.incrementAndGet();
            }
        } catch (FcrepoOperationFailedException ex) {
            importLogger.error(
                String.format("Error importing: {} to {}, Message: {}", f.getAbsolutePath(), uri, ex.getMessage()), ex);
            throw new RuntimeException("Error importing " + f.getAbsolutePath() + ": " + ex.toString(), ex);
        } catch (IOException ex) {
            importLogger.error(
                String.format("Error reading/parsing file: {}, Message: {}", f.getAbsolutePath(), ex.getMessage()), ex);
            throw new RuntimeException(
                    "Error reading or parsing " + f.getAbsolutePath() + ": " + ex.toString(), ex);
        }
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
        // The path, relative to the base in the export directory.
        // This is used in place of the full path to make the output more readable.
        final String sourceRelativePath =
                config.getBaseDirectory().toPath().relativize(f.toPath()).toString();
        final String filePath = f.getPath();
        if (filePath.endsWith(BINARY_EXTENSION) || filePath.endsWith(EXTERNAL_RESOURCE_EXTENSION)) {
            // ... this is only expected to happen when binaries and metadata are written to the same directory...
            if (config.isIncludeBinaries()) {
                logger.debug("Skipping binary {}: it will be imported when its metadata is imported.",
                        sourceRelativePath);
            } else {
                logger.debug("Skipping binary {}", sourceRelativePath);
            }
            return;
        } else if (!filePath.endsWith(config.getRdfExtension())) {
            // this could be hidden files created by the OS
            logger.info("Skipping file with unexpected extension ({}).", sourceRelativePath);
            return;
        } else {

            FcrepoResponse response = null;
            URI destinationUri = null;
            try {
                final Model model = parseStream(new FileInputStream(f));
                if (model.contains(null, RDF_TYPE, createResource(REPOSITORY_NAMESPACE + "RepositoryRoot"))) {
                    logger.debug("Skipping import of repository root.");
                    return;
                }
                final ResIterator binaryResources = model.listResourcesWithProperty(RDF_TYPE, NON_RDF_SOURCE);
                if (binaryResources.hasNext()) {
                    if (!config.isIncludeBinaries()) {
                        return;
                    }
                    destinationUri = new URI(binaryResources.nextResource().getURI());
                    logger.info("Importing binary {}", sourceRelativePath);
                    response = importBinary(destinationUri, model);
                } else {
                    destinationUri = new URI(config.getResource().toString() + "/"
                            + uriPathForFile(f, importContainerDirectory));
                    if (membershipResources.contains(destinationUri)) {
                        logger.warn("Skipping Membership Resource: {}", destinationUri);
                        return;
                    }
                    if (model.contains(null, RDF_TYPE, PAIRTREE)) {
                        logger.info("Skipping PairTree Resource: {}", destinationUri);
                        return;
                    }

                    logger.info("Importing container {} to {}", f.getAbsolutePath(), destinationUri);
                    response = importContainer(destinationUri, sanitize(model));

                }

                if (response == null) {
                    logger.warn("Failed to import {}", f.getAbsolutePath());
                } else if (response.getStatusCode() == 401) {
                    importLogger.error("Error importing {} to {}, 401 Unauthorized", f.getAbsolutePath(),
                        destinationUri);
                    throw new AuthenticationRequiredRuntimeException();
                } else if (response.getStatusCode() > 204 || response.getStatusCode() < 200) {
                    final String message = "Error while importing " + f.getAbsolutePath() + " ("
                            + response.getStatusCode() + "): " + IOUtils.toString(response.getBody());
                    logger.error(message);
                    importLogger.error("Error importing {} to {}, received {}", f.getAbsolutePath(), destinationUri,
                        response.getStatusCode());
                } else {
                    logger.info("Imported {}: {}", f.getAbsolutePath(), destinationUri);
                    importLogger.info("import {} to {}", f.getAbsolutePath(), destinationUri);
                    successCount.incrementAndGet();
                }
            } catch (FcrepoOperationFailedException ex) {
                importLogger.error(String.format("Error importing {} to {}, Message: {}", f.getAbsolutePath(),
                    destinationUri, ex.getMessage()), ex);
                throw new RuntimeException("Error importing " + f.getAbsolutePath() + ": " + ex.toString(), ex);
            } catch (IOException ex) {
                importLogger.error(String.format("Error reading/parsing {} to {}, Message: {}", f.getAbsolutePath(),
                    destinationUri, ex.getMessage()), ex);
                throw new RuntimeException(
                        "Error reading or parsing " + f.getAbsolutePath() + ": " + ex.toString(), ex);
            } catch (URISyntaxException ex) {
                importLogger.error(
                    String.format("Error building URI for {}, Message: {}", f.getAbsolutePath(), ex.getMessage()), ex);
                throw new RuntimeException("Error building URI for " + f.getAbsolutePath() + ": " + ex.toString(), ex);
            }
        }
    }

    private Model parseStream(final InputStream in) throws IOException {
        final URI source = config.getSource();
        final SubjectMappingStreamRDF mapper = new SubjectMappingStreamRDF(source, config.getResource());
        try (final InputStream in2 = in) {
            RDFDataMgr.parse(mapper, in2, contentTypeToLang(config.getRdfLanguage()));
        }
        return mapper.getModel();
    }

    private FcrepoResponse importBinary(final URI binaryURI, final Model model)
            throws FcrepoOperationFailedException, IOException {
        final Resource binaryRes = createResource(binaryURI.toString());
        final String contentType = model.getProperty(binaryRes, HAS_MIME_TYPE).getString();
        final boolean external = contentType.contains("message/external-body");
        final File binaryFile =  fileForBinaryURI(binaryURI, external);
        final InputStream contentStream;
        if (external) {
            contentStream = new ByteArrayInputStream(new byte[]{});
        } else {
            contentStream = new FileInputStream(binaryFile);
        }
        PutBuilder builder = client().put(binaryURI).body(contentStream, contentType);
        if (!external) {
            if (sha1FileMap != null) {
                // Use the bagIt checksum
                final String checksum = sha1FileMap.get(binaryFile);
                logger.debug("Using Bagit checksum ({}) for file ({})", checksum, binaryFile.getPath());
                builder = builder.digest(checksum);
            } else {
                builder = builder.digest(model.getProperty(binaryRes, HAS_MESSAGE_DIGEST)
                                          .getObject().toString().replaceAll(".*:",""));
            }
        }
        final FcrepoResponse binaryResponse = builder.perform();

        if (binaryResponse.getStatusCode() == 201 || binaryResponse.getStatusCode() == 204) {
            logger.info("Imported binary: {}", binaryURI);
            importLogger.info("import {} to {}", binaryFile.getAbsolutePath(), binaryURI);
            successCount.incrementAndGet();

            final URI descriptionURI = binaryResponse.getLinkHeaders("describedby").get(0);
            return client().put(descriptionURI).body(modelToStream(sanitize(model)), config.getRdfLanguage())
                .preferLenient().perform();
        } else if (binaryResponse.getStatusCode() == 410 && config.overwriteTombstones()) {
            deleteTombstone(binaryResponse);
            return builder.perform();
        } else {
            logger.error("Error while importing {} ({}): {}", binaryFile.getAbsolutePath(),
                    binaryResponse.getStatusCode(), IOUtils.toString(binaryResponse.getBody()));
            return null;
        }
    }

    private FcrepoResponse importContainer(final URI uri, final Model model) throws FcrepoOperationFailedException {
        PutBuilder builder = client().put(uri).body(modelToStream(model), config.getRdfLanguage());
        if (sha1FileMap != null && config.getBagProfile() != null) {
            // Use the bagIt checksum
            final File baseDir = config.getBaseDirectory();
            final File containerFile = Paths.get(fileForContainerURI(uri).toURI()).normalize().toFile();
            final String checksum = sha1FileMap.get(containerFile);
            logger.debug("Using Bagit checksum ({}) for file ({})", checksum, containerFile.getPath());
            builder = builder.digest(checksum);
        }
        final FcrepoResponse response = builder.preferLenient().perform();
        if (response.getStatusCode() == 410 && config.overwriteTombstones()) {
            deleteTombstone(response);
            return builder.preferLenient().perform();
        } else {
            return response;
        }
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

    private Model sanitize(final Model model) throws IOException, FcrepoOperationFailedException {
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
                    || (s.getPredicate().equals(RDF_TYPE) && forbiddenType(s.getResource()))) {
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

    private boolean forbiddenType(final Resource resource) {
         return resource.getNameSpace().equals(REPOSITORY_NAMESPACE)
             || resource.getURI().equals(CONTAINER.getURI())
             || resource.getURI().equals(NON_RDF_SOURCE.getURI())
             || resource.getURI().equals(RDF_SOURCE.getURI());
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

    private String uriPathForFile(final File f, final File baseDir) throws URISyntaxException {
        String relative = baseDir.toPath().relativize(f.toPath()).toString();
        relative = TransferProcess.decodePath(relative);

        // for exported RDF, just remove the ".extension" and you have the encoded path
        if (relative.endsWith(config.getRdfExtension())) {
            relative = relative.substring(0, relative.length() - config.getRdfExtension().length());
        }

        return relative;
    }

    private File fileForBinaryURI(final URI uri, final boolean external) {
        return new File(config.getBaseDirectory() + TransferProcess.decodePath(uri.getPath()) +
                    (external ? EXTERNAL_RESOURCE_EXTENSION : BINARY_EXTENSION));
    }

    private File fileForContainerURI(final URI uri) {
        return TransferProcess.fileForURI(uri, config.getBaseDirectory(), config.getRdfExtension());
    }

    /**
     * Verify the bag we are going to import
     *
     * @param bagDir root directory of the bag
     * @return true if valid
     */
    public static boolean verifyBag(final File bagDir) {
        try {
            final Bag bag = BagReader.read(bagDir);
            BagVerifier.isValid(bag, true);
            return true;
        } catch (Exception e) {
            throw new RuntimeException(String.format("Error verifying bag: %s", e.getMessage()), e);
        }
    }
}
