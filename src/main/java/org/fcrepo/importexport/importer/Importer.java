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

import static java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME;
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toList;
import static org.apache.jena.rdf.model.ResourceFactory.createProperty;
import static org.apache.jena.rdf.model.ResourceFactory.createResource;
import static org.apache.jena.riot.RDFLanguages.contentTypeToLang;
import static org.fcrepo.importexport.common.BagProfileConstants.BAGIT_MD5;
import static org.fcrepo.importexport.common.BagProfileConstants.BAGIT_SHA1;
import static org.fcrepo.importexport.common.BagProfileConstants.BAGIT_SHA_256;
import static org.fcrepo.importexport.common.FcrepoConstants.BINARY_EXTENSION;
import static org.fcrepo.importexport.common.FcrepoConstants.CONTAINS;
import static org.fcrepo.importexport.common.FcrepoConstants.CONTENT_TYPE_HEADER;
import static org.fcrepo.importexport.common.FcrepoConstants.CREATED_BY;
import static org.fcrepo.importexport.common.FcrepoConstants.CREATED_DATE;
import static org.fcrepo.importexport.common.FcrepoConstants.DESCRIBEDBY;
import static org.fcrepo.importexport.common.FcrepoConstants.DIRECT_CONTAINER;
import static org.fcrepo.importexport.common.FcrepoConstants.EXTERNAL_RESOURCE_EXTENSION;
import static org.fcrepo.importexport.common.FcrepoConstants.HAS_MESSAGE_DIGEST;
import static org.fcrepo.importexport.common.FcrepoConstants.HAS_MIME_TYPE;
import static org.fcrepo.importexport.common.FcrepoConstants.HEADERS_EXTENSION;
import static org.fcrepo.importexport.common.FcrepoConstants.INDIRECT_CONTAINER;
import static org.fcrepo.importexport.common.FcrepoConstants.LAST_MODIFIED_BY;
import static org.fcrepo.importexport.common.FcrepoConstants.LAST_MODIFIED_DATE;
import static org.fcrepo.importexport.common.FcrepoConstants.LDP_NAMESPACE;
import static org.fcrepo.importexport.common.FcrepoConstants.MEMBERSHIP_RESOURCE;
import static org.fcrepo.importexport.common.FcrepoConstants.MEMENTO;
import static org.fcrepo.importexport.common.FcrepoConstants.MEMENTO_DATETIME_HEADER;
import static org.fcrepo.importexport.common.FcrepoConstants.MEMENTO_NAMESPACE;
import static org.fcrepo.importexport.common.FcrepoConstants.NON_RDF_SOURCE;
import static org.fcrepo.importexport.common.FcrepoConstants.PAIRTREE;
import static org.fcrepo.importexport.common.FcrepoConstants.RDF_TYPE;
import static org.fcrepo.importexport.common.FcrepoConstants.REPOSITORY_NAMESPACE;
import static org.fcrepo.importexport.common.FcrepoConstants.TIMEMAP;
import static org.fcrepo.importexport.common.TransferProcess.fileForBinary;
import static org.fcrepo.importexport.common.TransferProcess.fileForExternalResources;
import static org.fcrepo.importexport.common.TransferProcess.fileForURI;
import static org.fcrepo.importexport.common.TransferProcess.isRepositoryRoot;
import static org.fcrepo.importexport.common.UriUtils.withSlash;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZonedDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import gov.loc.repository.bagit.domain.Manifest;
import gov.loc.repository.bagit.verify.BagVerifier;
import org.apache.commons.lang3.StringUtils;
import org.apache.jena.rdf.model.Property;
import org.fcrepo.client.FcrepoClient;
import org.fcrepo.client.FcrepoLink;
import org.fcrepo.client.FcrepoOperationFailedException;
import org.fcrepo.client.FcrepoResponse;
import org.fcrepo.client.PostBuilder;
import org.fcrepo.client.PutBuilder;
import org.fcrepo.importexport.common.AuthenticationRequiredRuntimeException;
import org.fcrepo.importexport.common.BagDeserializer;
import org.fcrepo.importexport.common.BagProfile;
import org.fcrepo.importexport.common.Config;
import org.fcrepo.importexport.common.ResourceNotFoundRuntimeException;
import org.fcrepo.importexport.common.SerializationSupport;
import org.fcrepo.importexport.common.TransferProcess;

import org.apache.commons.io.IOUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.NodeIterator;
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

/**
 * Fedora Import Utility
 *
 * @author lsitu
 * @author awoods
 * @author escowles
 * @since 2016-08-29
 */
public class Importer implements TransferProcess {
    private static final Logger logger = getLogger(Importer.class);
    private final Config config;
    protected FcrepoClient.FcrepoClientBuilder clientBuilder;
    private final List<URI> membershipResources = new ArrayList<>();
    private final List<URI> relatedResources = new ArrayList<>();
    private final List<URI> importedResources = new ArrayList<>();
    private URI repositoryRoot = null;

    /**
     * When importing a BagIt bag, this stores a mapping of filenames to checksums from the bag's payload manifest
      */
    private Map<String, String> bagItFileMap;
    private String digestAlgorithm;

    private Logger importLogger;
    private AtomicLong successCount = new AtomicLong(); // set to zero at start

    final static Set<String> INTERACTION_MODELS = new HashSet<>(Arrays.asList(DIRECT_CONTAINER.getURI(),
                                                                              INDIRECT_CONTAINER.getURI()));

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
        final String bagProfile = config.getBagProfile();
        if (bagProfile == null) {
            this.bagItFileMap = null;
        } else {
            try {
                // load the bag profile
                final URL url = this.getClass().getResource("/profiles/" + bagProfile + ".json");
                final InputStream in = url == null ? new FileInputStream(bagProfile) : url.openStream();
                final BagProfile profile = new BagProfile(in);

                final Path root;
                final File bagDir = config.getBaseDirectory().getAbsoluteFile().getParentFile();
                // if the given file is serialized (a single file), try to extract first
                if (bagDir.isFile() && (profile.getSerialization() == BagProfile.Serialization.OPTIONAL ||
                                        profile.getSerialization() == BagProfile.Serialization.REQUIRED)) {
                    final BagDeserializer deserializer = SerializationSupport.deserializerFor(bagDir.toPath(), profile);
                    root = deserializer.deserialize(bagDir.toPath());
                    // update the base directory so we don't attempt to work on the serialized bag later
                    config.setBaseDirectory(root.toString());
                } else {
                    root = bagDir.toPath();
                }
                final Bag bag = verifyBag(root);
                configureBagItFileMap(bag);
            } catch (IOException e) {
                logger.error("Unable to open BagProfile for {}!", bagProfile);
                throw new RuntimeException(e);
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

        repositoryRoot = findRepositoryRoot(config.getResource());
        logger.debug("Repository root {}", repositoryRoot);

        processImport(config.getResource());

        importLogger.info("Finished import... {} resources imported", successCount.get());
    }

    private void processImport(final URI resource) {
        importedResources.add(resource);

        final File importContainerMetadataFile = fileForContainerURI(resource);
        importContainerDirectory = directoryForContainer(resource);

        // clean up the membership resources that were imported.
        membershipResources.clear();
        discoverMembershipResources(importContainerDirectory);

        // In order to get the dates right, we need to create resources, *then* set their RDF since
        // creation of children screws up the dates of parents.
        try {
            ensureExists(resource);
        } catch (FcrepoOperationFailedException | IOException e) {
            throw new RuntimeException("Unable to create placeholder " + resource, e);
        }

        importDirectory(importContainerDirectory);

        logger.debug("Importing resource {} for file {}", resource, importContainerMetadataFile.getPath());

        // discover the related resources in the resource being imported.
        if (!importContainerMetadataFile.exists()) {
            logger.debug("No container exists in the metadata directory {} for the requested resource {},"
                    + " importing all contained resources instead.", importContainerMetadataFile.getPath(),
                    config.getResource());
        } else {
            logger.debug("Parsing membership resource in file {}", importContainerMetadataFile.getAbsolutePath());
            parseMembershipResources(importContainerMetadataFile);
            importMembershipResources();
            importRelatedResources();
            importFile(importContainerMetadataFile);
        }

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

            // Discover all the related resources with member predicates. Those related resources that aren't imported
            // during importing the targeted resource are in other container hierarchy that need to handle specifically.
            // The related resources referenced by default predicate could be ignored.
            for (final String p : config.getPredicates()) {
                if (!p.equals(CONTAINS.toString())) {
                    for (final NodeIterator it = model.listObjectsOfProperty(createProperty(p)); it.hasNext();) {
                        final String uri = it.nextNode().toString();

                        logger.debug("Discovered related resource {} for source {}.", uri, config.getSource());
                        if (!importedResources.contains(URI.create(uri))) {
                            // add related resource to list, exclude those that are already imported
                            final URI resURI = URI.create(uri);
                            if ((fileForContainerURI(resURI).exists())) {
                                relatedResources.add(URI.create(uri));

                                logger.debug("Added related resource {}", uri);
                            } else if ((fileForBinaryURI(resURI).exists() || fileForBinaryURI(resURI)
                                    .exists())) {
                                importedResources.add(URI.create(uri));

                                // The binary file will be imported when the non-RDF metadata file is being imported.
                                importDirectory(directoryForContainer(resURI));
                            }
                        }
                    }
                }
            }
        } catch (final IOException e) {
            throw new RuntimeException("Error reading file: " + f.getAbsolutePath() + ": " + e.toString());
        } catch (final RiotException e) {
            throw new RuntimeException("Error parsing RDF: " + f.getAbsolutePath() + ": " + e.toString());
        }
    }

    private void importRelatedResources() {
        if (relatedResources.size() > 0) {
            final List<URI> referenceResources = relatedResources.stream().collect(toList());
            relatedResources.clear();
            // loop through for nested related resources
            referenceResources.stream().forEach(uri -> {
                logger.info("Importing related resources {} ...", uri);
                processImport(uri);
            });
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
            final FcrepoResponse response = importContainer(uri,
                                                            sanitize(diskModel.difference(repoModel)),
                                                            parseHeaders(getHeadersFile(f)));
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
                String.format("Error importing: %1$s to %2$s, Message: %3$s",
                        f.getAbsolutePath(), uri, ex.getMessage()), ex);
            throw new RuntimeException("Error importing " + f.getAbsolutePath() + ": " + ex.toString(), ex);
        } catch (IOException ex) {
            importLogger.error(
                String.format("Error reading/parsing file: %1$s, Message: %2$s",
                        f.getAbsolutePath(), ex.getMessage()), ex);
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

        //ignore header files
        if (filePath.endsWith(".headers")) {
            // this could be hidden files created by the OS
            logger.debug("Skipping .headers files ({}).", sourceRelativePath);
            return;

        }

        //parse headers from headers file
        final Map<String,List<String>> headers = parseHeaders(getHeadersFile(f));

        //always skip timemaps since they are derived from the mementos they contain.
        if (isTimeMap(headers)) {
            logger.debug("Skipping {} :  TimeMaps are never imported.", sourceRelativePath);
            return;
        }

        final boolean isMemento = isMemento(headers);

        //skip versions when include versions flag is false.
        if (!config.includeVersions() && isMemento) {
            logger.debug("Skipping {}: Versions import disabled.", sourceRelativePath);
            return;
        }

        if (filePath.endsWith(BINARY_EXTENSION) || filePath.endsWith(EXTERNAL_RESOURCE_EXTENSION)) {
            // ... this is only expected to happen when binaries and metadata are written to the same directory...

            if (!isMemento) {
                if (config.isIncludeBinaries()) {
                    logger.debug("Skipping binary {}: it will be imported when its metadata is imported.",
                        sourceRelativePath);
                } else {
                    logger.debug("Skipping binary {}", sourceRelativePath);
                }
                return;
            } // else continue processing

        } else if (!filePath.endsWith(config.getRdfExtension())) {
            // this could be hidden files created by the OS
            logger.info("Skipping file with unexpected extension ({}).", sourceRelativePath);
            return;
        }

        FcrepoResponse response = null;
        URI destinationUri = null;
        try {

            if (isMemento) {
                response = importMemento(f, headers);
            } else {

                final Model model = parseStream(new FileInputStream(f));

                // remove the member resources that are being imported
                for (final ResIterator it = model.listSubjects(); it.hasNext();) {
                    final URI uri = URI.create(it.next().toString());
                    if (relatedResources.contains(uri)) {
                        relatedResources.remove(uri);
                    }
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
                    destinationUri = uriForFile(f);
                    if (membershipResources.contains(destinationUri)) {
                        logger.warn("Skipping Membership Resource: {}", destinationUri);
                        return;
                    }
                    if (model.contains(null, RDF_TYPE, PAIRTREE)) {
                        logger.info("Skipping PairTree Resource: {}", destinationUri);
                        return;
                    }

                    logger.info("Importing container {} to {}", f.getAbsolutePath(), destinationUri);
                    response = importContainer(destinationUri, sanitize(model), headers);
                }
            }


            if (response.getStatusCode() == 401) {
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
            importLogger.error(String.format("Error importing %1$s to %2$s, Message: %3$s", f.getAbsolutePath(),
                destinationUri, ex.getMessage()), ex);
            throw new RuntimeException("Error importing " + f.getAbsolutePath() + ": " + ex.toString(), ex);
        } catch (IOException ex) {
            importLogger.error(String.format("Error reading/parsing %1$s to %2$s, Message: %3$s",
                    f.getAbsolutePath(), destinationUri, ex.getMessage()), ex);
            throw new RuntimeException(
                    "Error reading or parsing " + f.getAbsolutePath() + ": " + ex.toString(), ex);
        } catch (URISyntaxException ex) {
            importLogger.error(
                String.format("Error building URI for %1$s, Message: %2$s",
                        f.getAbsolutePath(), ex.getMessage()), ex);
            throw new RuntimeException("Error building URI for " + f.getAbsolutePath() + ": " + ex.toString(), ex);
        }
    }

    private FcrepoResponse importMemento(final File mementoFile, final Map<String, List<String>> headers)
        throws IOException, FcrepoOperationFailedException {
        final String mementoDatetime = getFirstByKey(headers, MEMENTO_DATETIME_HEADER);
        final String contentType = getFirstByKey(headers, CONTENT_TYPE_HEADER);
        final URI timeMapURI = getLinkValueByRel(headers, "timemap");
        final PostBuilder builder = client().post(timeMapURI)
            .body(new FileInputStream(mementoFile), contentType)
            .addHeader(MEMENTO_DATETIME_HEADER, mementoDatetime);
        return builder.perform();
    }

    private boolean hasType(final Map<String, List<String>> headers, final String typeUri) {
        final List<String> values = headers.get("Link");
        if (values != null) {
            for (String linkstr : values) {
                final FcrepoLink link = FcrepoLink.valueOf(linkstr);
                if (link.getRel().equals("type") && link.getUri().toString().equals(typeUri)) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean isMemento(final Map<String, List<String>> headers) {
        return hasType(headers, MEMENTO.getURI());
    }

    private boolean isTimeMap(final Map<String, List<String>> headers) {
        return hasType(headers, TIMEMAP.getURI());
    }

    private URI getLinkValueByRel(final Map<String, List<String>> headers, final String rel) {
        final List<String> values = headers.get("Link");
        for (String linkstr : values) {
            final FcrepoLink link = FcrepoLink.valueOf(linkstr);
            if (link.getRel().equals(rel)) {
                return link.getUri();
            }
        }

        return null;
    }

    private String getFirstByKey(final Map<String, List<String>> headers, final String key) {
        final List<String> values = headers.get(key);
        if (values != null && values.size() > 0) {
            return values.get(0);
        }

        return null;
    }

    private File getHeadersFile(final File f) {
        return new File(f.getParentFile(), f.getName() + HEADERS_EXTENSION);
    }

    private Map<String, List<String>> parseHeaders(final File headersFile) {
        try {
            if (!headersFile.exists()) {
                return new HashMap<>();
            }

            //converting json to Map
            final byte[] mapData = Files.readAllBytes(Paths.get(headersFile.toURI()));
            final ObjectMapper objectMapper = new ObjectMapper();
            final Map<String, List<String>> headers =
                objectMapper.readValue(mapData, new TypeReference<HashMap<String, List<String>>>() {
                });
            return headers;
        } catch (IOException ex) {
            importLogger.error(String.format("Error reading/parsing headers file for %1$s - Message: %2$s",
                headersFile.getAbsolutePath(), ex.getMessage()), ex);
            throw new RuntimeException(
                "Error reading or parsing headers file for" + headersFile.getAbsolutePath() + ": " + ex.toString(), ex);
        }
    }

    private Model parseStream(final InputStream in) throws IOException {
        final SubjectMappingStreamRDF mapper = new SubjectMappingStreamRDF(config.getSource(),
                                                                           config.getDestination());
        try (final InputStream in2 = in) {
            RDFDataMgr.parse(mapper, in2, contentTypeToLang(config.getRdfLanguage()));
        }
        return mapper.getModel();
    }

    private FcrepoResponse importBinary(final URI binaryURI, final Model model)
            throws FcrepoOperationFailedException, IOException {
        final String contentType = model.getProperty(createResource(binaryURI.toString()), HAS_MIME_TYPE).getString();
        final File binaryFile =  fileForBinaryURI(binaryURI);

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
            return binaryResponse;
        }
    }

    private PutBuilder binaryBuilder(final URI binaryURI, final File binaryFile, final String contentType,
            final Model model) throws FcrepoOperationFailedException, IOException {
        final Map<String, List<String>> headers = parseHeaders(getHeadersFile(binaryFile));
        String externalContentLocation = null;

        boolean isRedirect = false;
        if (headers.containsKey("Location")) {
            externalContentLocation = getFirstByKey(headers, "Location");
            isRedirect = true;
        } else if (headers.containsKey("Content-Location")) {
            externalContentLocation = getFirstByKey(headers, "Content-Location");
        }

        PutBuilder builder = client().put(binaryURI)
                                     .filename(null);

        if (externalContentLocation != null) {
            builder.externalContent(URI.create(externalContentLocation), contentType,
                                    isRedirect ? "redirect" : "proxy");
        } else {
            builder.body(new FileInputStream(binaryFile), contentType).ifUnmodifiedSince(currentTimestamp());

            if (bagItFileMap != null) {
                // Use the bagIt checksum
                final String checksum = bagItFileMap.get(binaryFile.getAbsolutePath());
                logger.debug("Using Bagit checksum ({}) for file ({}): {}", checksum, binaryFile.getPath(), binaryURI);
                builder = builder.digest(checksum, digestAlgorithm);
            } else {
                builder = builder.digest(model.getProperty(createResource(binaryURI.toString()), HAS_MESSAGE_DIGEST)
                                          .getObject().toString().replaceAll(".*:",""));
            }
        }
        return builder;
    }


    private FcrepoResponse importContainer(final URI uri, final Model model, final Map<String,List<String>> headers)
        throws FcrepoOperationFailedException {
        final FcrepoResponse response = containerBuilder(uri, model, headers).preferLenient().perform();
        if (response.getStatusCode() == 410 && config.overwriteTombstones()) {
            deleteTombstone(response);
            return containerBuilder(uri, model, headers).preferLenient().perform();
        } else {
            return response;
        }
    }

    private PutBuilder containerBuilder(final URI uri, final Model model, final Map<String,List<String>> headers) {
        PutBuilder builder = client().put(uri)
                                     .body(modelToStream(model), config.getRdfLanguage())
                                     .ifUnmodifiedSince(currentTimestamp());
        if (bagItFileMap != null && config.getBagProfile() != null) {
            // Use the bagIt checksum
            final File containerFile = Paths.get(fileForContainerURI(uri).toURI()).normalize().toFile();
            final String checksum = bagItFileMap.get(containerFile.getAbsolutePath());
            logger.debug("Using Bagit checksum ({}) for file ({})", checksum, containerFile.getPath());
            builder = builder.digest(checksum, digestAlgorithm);
        }

        addInteractionModels(builder, headers);
        return builder;
    }

    private void addInteractionModels(final PutBuilder builder, final Map<String, List<String>> headers) {
        headers.entrySet().stream().filter(entry -> entry.getKey().equals("Link"))
            .flatMap(entry -> entry.getValue().stream())
            .forEach(linkstr -> {
                final FcrepoLink link = FcrepoLink.valueOf(linkstr);
                if (link.getRel().equals("type")) {
                    final String interactionModel = link.getUri().toString();
                    if (INTERACTION_MODELS.contains(interactionModel)) {
                        builder.addInteractionModel(interactionModel);
                    }
                }
            });
    }

    private String currentTimestamp() {
        return RFC_1123_DATE_TIME.format(ZonedDateTime.now(ZoneId.of("GMT")));
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
            || resource.getNameSpace().equals(MEMENTO_NAMESPACE)
            || resource.getNameSpace().equals(LDP_NAMESPACE);
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
        final ByteArrayInputStream emptyStream = new ByteArrayInputStream(new byte[]{});
        if (fileForBinaryURI(uri).exists()) {
            response = client().put(uri).body(emptyStream).perform();
        } else if (fileForContainerURI(uri).exists()) {
            response = client().put(uri).body(emptyStream, "text/turtle").perform();
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

    private URI uriForFile(final File f) {
        // get path of file relative to the data directory
        String relative = config.getBaseDirectory().toURI().relativize(f.toURI()).toString();
        relative = TransferProcess.decodePath(relative);

        // rebase the path on the destination uri (translating source/destination if needed)
        if ( config.getSource() != null && config.getDestination() != null ) {
            relative = baseURI(config.getSource()) + relative;
            relative = relative.replaceFirst(config.getSource().toString(), config.getDestination().toString());
        } else {
            relative = baseURI(config.getResource()) + relative;
        }

        // for exported RDF, just remove the ".extension" and you have the encoded path
        if (relative.endsWith(config.getRdfExtension())) {
            relative = relative.substring(0, relative.length() - config.getRdfExtension().length());
        }
        return URI.create(relative);
    }

    private static String baseURI(final URI uri) {
        final String base = uri.toString().replaceFirst(uri.getPath() + "$", "");
        return (base.endsWith("/")) ? base : base + "/";
    }

    private File fileForBinaryURI(final URI uri) {
        final File file = fileForExternalResources(uri, config.getSourcePath(), config.getDestinationPath(),
                    config.getBaseDirectory());

        if (file.exists()) {
            return file;
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

    /**
     * Verify the bag we are going to import
     *
     * @param bagDir root directory of the bag
     * @return the {@link Bag} if valid
     */
    private Bag verifyBag(final Path bagDir) {
        try {
            final BagReader bagReader = new BagReader();
            final Bag bag = bagReader.read(bagDir);

            final BagVerifier bagVerifier = new BagVerifier();
            bagVerifier.isValid(bag, false);

            return bag;
        } catch (Exception e) {
            logger.error("Unable to read bag ", e);
            throw new RuntimeException(String.format("Error reading bag: %s", e.getMessage()), e);
        }
    }

    /**
     * Query a {@link Bag} for the highest ranking {@link Manifest} and use that in order to populate the
     * {@code bagItFileMap} and {@code digestAlgorithm} for use when importing files
     *
     * @param bag The {@link Bag} to read from
     */
    private void configureBagItFileMap(final Bag bag) {
        // The fcrepo-client-java only supports up to sha256 so we only check against each of md5, sha1, and sha256
        final Set<String> fcrepoSupported = new HashSet<>(Arrays.asList(BAGIT_MD5, BAGIT_SHA1, BAGIT_SHA_256));
        final Manifest manifest = bag.getPayLoadManifests().stream()
               .filter(streamManifest -> fcrepoSupported.contains(streamManifest.getAlgorithm().getBagitName()))
               .reduce((m1, m2) -> manifestPriority(m1) > manifestPriority(m2) ? m1 : m2)
               .orElseThrow(() -> new RuntimeException("Bag does not contain any manifests the import " +
                                                       "utility can use! Available algorithms are: " +
                                                       StringUtils.join(fcrepoSupported, ",")));

        this.bagItFileMap = manifest.getFileToChecksumMap().entrySet().stream()
                                    .collect(Collectors.toMap(
                                        entry -> entry.getKey().toAbsolutePath().toString(),
                                        Map.Entry::getValue));
        logger.debug("loaded checksum map: {}", bagItFileMap);

        switch(manifest.getAlgorithm().getBagitName()) {
            case BAGIT_MD5:
                this.digestAlgorithm = BAGIT_MD5;
                break;
            case BAGIT_SHA1:
                // fcrepo-client expects only "sha" for sha1 headers
                this.digestAlgorithm = "sha";
                break;
            case BAGIT_SHA_256:
                this.digestAlgorithm = BAGIT_SHA_256;
                break;
            default: throw new RuntimeException("Unsupported bagit algorithm!");
        }
    }

    /**
     * Receive the priority for a given {@link Manifest} based on its digest algorithm
     *
     * @param manifest the manifest to prioritize
     * @return the priority of the digest algorithm
     */
    private int manifestPriority(final Manifest manifest) {
        final String bagItAlgorithm = manifest.getAlgorithm().getBagitName();
        switch (bagItAlgorithm) {
            case BAGIT_MD5: return 0;
            case BAGIT_SHA1: return 1;
            case BAGIT_SHA_256: return 2;
            default: throw new RuntimeException("Algorithm not allowed! " + bagItAlgorithm);
        }
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
