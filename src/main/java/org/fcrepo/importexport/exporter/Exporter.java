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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.NodeIterator;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.riot.RDFDataMgr;
import org.duraspace.bagit.BagConfig;
import org.duraspace.bagit.BagItDigest;
import org.duraspace.bagit.BagWriter;
import org.duraspace.bagit.profile.BagProfile;
import org.duraspace.bagit.profile.BagProfileConstants;
import org.duraspace.bagit.serialize.BagSerializer;
import org.duraspace.bagit.serialize.SerializationSupport;
import org.fcrepo.client.FcrepoClient;
import org.fcrepo.client.FcrepoOperationFailedException;
import org.fcrepo.client.FcrepoResponse;
import org.fcrepo.client.GetBuilder;
import org.fcrepo.client.HeadBuilder;
import org.fcrepo.importexport.common.Config;
import org.fcrepo.importexport.common.TransferProcess;
import org.slf4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.apache.commons.codec.binary.Hex.encodeHex;
import static org.apache.commons.io.FileUtils.byteCountToDisplaySize;
import static org.apache.jena.rdf.model.ModelFactory.createDefaultModel;
import static org.apache.jena.rdf.model.ResourceFactory.createProperty;
import static org.apache.jena.riot.RDFLanguages.contentTypeToLang;
import static org.fcrepo.importexport.common.FcrepoConstants.CONTAINER;
import static org.fcrepo.importexport.common.FcrepoConstants.CONTAINS;
import static org.fcrepo.importexport.common.FcrepoConstants.HEADERS_EXTENSION;
import static org.fcrepo.importexport.common.FcrepoConstants.INBOUND_REFERENCES;
import static org.fcrepo.importexport.common.FcrepoConstants.MEMENTO;
import static org.fcrepo.importexport.common.FcrepoConstants.NON_RDF_SOURCE;
import static org.fcrepo.importexport.common.FcrepoConstants.PREFER_MEMBERSHIP;
import static org.fcrepo.importexport.common.FcrepoConstants.RDF_SOURCE;
import static org.fcrepo.importexport.common.FcrepoConstants.REPOSITORY_NAMESPACE;
import static org.fcrepo.importexport.common.FcrepoConstants.TIMEMAP;
import static org.fcrepo.importexport.common.TransferProcess.checkValidResponse;
import static org.fcrepo.importexport.common.TransferProcess.fileForBinary;
import static org.fcrepo.importexport.common.TransferProcess.fileForExternalResources;
import static org.fcrepo.importexport.common.TransferProcess.fileForURI;
import static org.fcrepo.importexport.common.TransferProcess.isRepositoryRoot;
import static org.fcrepo.importexport.common.UriUtils.withSlash;
import static org.fcrepo.importexport.common.UriUtils.withoutSlash;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Fedora Export Utility
 *
 * @author mikejritter
 * @author lsitu
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
    private URI rdfSourceURI;
    private BagWriter bag;
    private BagSerializer bagSerializer;
    private String bagProfileId;
    private MessageDigest sha1 = null;
    private MessageDigest sha256 = null;
    private MessageDigest sha512 = null;
    private MessageDigest md5 = null;
    private HashMap<File, String> sha1FileMap = null;
    private HashMap<File, String> sha256FileMap = null;
    private HashMap<File, String> sha512FileMap = null;
    private HashMap<File, String> md5FileMap = null;

    private Logger exportLogger;
    private SimpleDateFormat dateFormat;
    private AtomicLong successCount = new AtomicLong(); // set to zero at start
    private AtomicLong successBytes = new AtomicLong();
    protected URI repositoryRoot = null;

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
        this.rdfSourceURI = URI.create(RDF_SOURCE.getURI());
        this.exportLogger = config.getAuditLog();
        this.dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        this.repositoryRoot = config.getRepositoryRoot();

        if (config.getBagProfile() != null) {
            configureBagItParameters();
        }
    }

    private void configureBagItParameters() {
        try {
            // parse config + load profile
            final BagConfig bagConfig = loadBagConfig(config.getBagConfigPath());
            final BagProfile bagProfile = config.initBagProfile();

            // configure the BagIt algorithms to use + setup the fields for the Exporter
            final Set<BagItDigest> algorithms = setupBagItDigests(bagProfile);

            // enforce default metadata
            bagProfile.validateConfig(bagConfig);
            final Map<String, String> profileMetadata = bagProfile.getProfileMetadata();

            // the profile identifier must exist per the bagit-profiles spec
            bagProfileId = profileMetadata.get(BagProfileConstants.BAGIT_PROFILE_IDENTIFIER);

            // check if serialization is required
            final String serializationFormat = config.getBagSerialization();
            if (serializationFormat != null) {
                // this can throw exceptions if the serialization format is not supported
                bagSerializer = SerializationSupport.serializerFor(serializationFormat, bagProfile);
            }

            // setup bag
            final File bagdir = config.getBaseDirectory().getParentFile();
            this.bag = new BagWriter(bagdir, algorithms);
            for (final String tagFile : bagConfig.getTagFiles()) {
                this.bag.addTags(tagFile, bagConfig.getFieldsForTagFile(tagFile));
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException(String.format("Error loading bag config file: %1$s", e.getMessage()), e);
        } catch (IOException e) {
            throw new RuntimeException(String.format("Error reading bag profile: %1$s", e.getMessage()), e);
        }
    }

    /**
     * Set up the hash algorithms (as {@link BagItDigest}) to use based on the {@link Config} and {@link BagProfile}
     * Always use the user specified + profile required algorithms
     * If neither is specified, use the profile "manifest-allowed" and "tag-manifest-allowed"
     * If the algorithms are still empty, use sha1
     *
     * @param profile the profile to get BagItDigests for
     * @return the algorithms which were initialized, as {@link BagItDigest}s
     */
    private Set<BagItDigest> setupBagItDigests(final BagProfile profile) {
        final Set<String> algorithms = new HashSet<>();
        final Set<String> allowedAlgorithms = profile.getAllowedPayloadAlgorithms();
        final Set<String> requiredAlgorithms = profile.getPayloadDigestAlgorithms();

        // first validate that the profile supports user specified algorithms
        final String[] userAlgorithms = config.getBagAlgorithms();
        if (userAlgorithms != null) {
            for (String algorithm : userAlgorithms) {
                if (!allowedAlgorithms.isEmpty() && !allowedAlgorithms.contains(algorithm)) {
                    throw new RuntimeException("Bag Profile does not allow specified algorithm: " + algorithm +
                                               ". Allowed algorithms are: " + allowedAlgorithms);
                }

                algorithms.add(algorithm);
            }
        }

        // always add required algorithms
        algorithms.addAll(requiredAlgorithms);

        // check if we should fallback to the allowed algorithms or sha1
        if (algorithms.isEmpty() && !allowedAlgorithms.isEmpty()) {
            algorithms.addAll(allowedAlgorithms);
        } else if (algorithms.isEmpty()) {
            algorithms.add(BagItDigest.SHA1.bagitName());
        }

        return algorithms.stream()
                             .map(String::toUpperCase)
                             .map(BagItDigest::valueOf)
                             .peek(this::setupFileMap)
                             .collect(Collectors.toSet());
    }

    /**
     * Configure the digest algorithm and fileMap for a given BagItDigest
     *
     * @param digest the BagItDigest to initialize fields for
     */
    private void setupFileMap(final BagItDigest digest) {
        switch (digest) {
            case MD5:
                this.md5 = digest.messageDigest();
                this.md5FileMap = new HashMap<>();
                break;
            case SHA1:
                this.sha1 = digest.messageDigest();
                this.sha1FileMap = new HashMap<>();
                break;
            case SHA256:
                this.sha256 = digest.messageDigest();
                this.sha256FileMap = new HashMap<>();
                break;
            case SHA512:
                this.sha512 = digest.messageDigest();
                this.sha512FileMap = new HashMap<>();
                break;
            default:
                throw new IllegalStateException("Unexpected BagIt algorithm: " + digest);
        }
    }

    /**
     * Loads a bag config from path
     * @param bagConfigPath The path to the bag config yaml.
     * @throws IOException if the bagConfigPath cannot be read
     * @return the initialized {@link BagConfig}
     */
    private BagConfig loadBagConfig(final String bagConfigPath) throws IOException {
        if (bagConfigPath == null) {
            throw new RuntimeException("The bag config path must not be null.");
        }
        final Path bagConfigFile = Paths.get(bagConfigPath);
        try (Reader bagConfigReader = Files.newBufferedReader(bagConfigFile)) {
            return new BagConfig(bagConfigReader);
        }
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
        if (repositoryRoot == null) {
            try {
                logger.info("Attempting to automatically determine the repository root");
                findRepositoryRoot(config.getResource());
            } catch (IOException | FcrepoOperationFailedException e) {
                throw new RuntimeException("Failed to locate the root of the repository being exported", e);
            }
        }
        logger.debug("Repository root is " + repositoryRoot);

        export(config.getResource());
        if (bag != null) {
            try {
                logger.info("Finishing bag manifests...");
                bag.addTags(BagConfig.BAG_INFO_KEY, bagTechMetadata());
                if (sha1 != null) {
                    bag.registerChecksums(BagItDigest.SHA1, sha1FileMap);
                }
                if (sha256 != null) {
                    bag.registerChecksums(BagItDigest.SHA256, sha256FileMap);
                }
                if (sha512 != null) {
                    bag.registerChecksums(BagItDigest.SHA512, sha512FileMap);
                }
                if (md5 != null) {
                    bag.registerChecksums(BagItDigest.MD5, md5FileMap);
                }
                bag.write();

                if (bagSerializer != null) {
                    // Make sure the path is an absolute path because the BagSerializer uses Path#relativize which
                    // requires both Paths to be of the same type
                    bagSerializer.serialize(bag.getRootDir().getAbsoluteFile().toPath());
                }
            } catch (IOException e) {
                throw new RuntimeException("Error finishing Bag: " + e.toString());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        exportLogger.info("Finished export... {} bytes/{} resources exported", successBytes.get(), successCount.get());
    }

    private Map<String, String> bagTechMetadata() {
        final Map<String, String> metadata = new HashMap<>();
        metadata.put(BagProfileConstants.BAGIT_PROFILE_IDENTIFIER, bagProfileId);
        metadata.put(BagConfig.BAG_SIZE_KEY, byteCountToDisplaySize(successBytes.longValue()));
        metadata.put(BagConfig.PAYLOAD_OXUM_KEY, successBytes.toString() + "." + successCount.toString());
        metadata.put(BagConfig.BAGGING_DATE_KEY, dateFormat.format(new Date()));
        return metadata;
    }

    private void export(final URI uri) {
        logger.trace("HEAD " + uri);
        try (FcrepoResponse response = client().head(uri).disableRedirects().perform()) {
            if (response.getStatusCode() == 404 && uri.toString().endsWith("fcr:acl")) {
                logger.debug("ACL {} not found and thus will not be exported.", uri);
                return;
            }

            checkValidResponse(response, uri, config.getUsername());
            final List<URI> linkHeaders = response.getLinkHeaders("type");
            final URI acl = response.getLinkHeaders("acl").stream().findFirst().orElse(null);

            if (linkHeaders.contains(binaryURI)) {
                logger.debug("Found binary at " + uri);
                final boolean external = response.getHeaderValue("Content-Location") != null;
                final boolean redirect = response.getStatusCode() >= 300 && response.getStatusCode() < 400;
                final List<URI> describedby = response.getLinkHeaders("describedby");
                exportBinary(uri, describedby, external, redirect);
            } else if (linkHeaders.contains(containerURI) || linkHeaders.contains(rdfSourceURI)) {
                logger.debug("Found container at " + uri);
                exportRdf(uri, null);
                // Export versions for this container
                exportVersions(uri);
            } else if (uri.equals(URI.create(repositoryRoot.toString() + "/fcr:acl").normalize())) {
                logger.info("The repository default root ACL is not being exported: {}", uri);
            } else {
                logger.error("Resource is not an LDP Container, LDP RDFSource,  or an LDP NonRDFSource: {}", uri);
                exportLogger.error("Resource is not an LDP Container, LDP RDFSource, or an LDP NonRDFSource: {}", uri);
            }

            if (acl != null && config.isIncludeAcls()) {
                export(acl);
            }


        } catch (FcrepoOperationFailedException ex) {
            logger.warn("Error retrieving content: {}", ex.toString());
            exportLogger.error(String.format("Error retrieving context of uri: %1$s, Message: %2$s",
                    uri, ex.toString()),
                ex);
        } catch (IOException ex) {
            logger.warn("Error writing content: {}", ex.toString());
            exportLogger.error(String.format("Error writing content from uri: %1$s, Message: %2$s", uri, ex.toString()),
                ex);
        }
    }

    private void exportBinary(final URI uri, final List<URI> describedby, final boolean external,
                              final boolean redirect) throws FcrepoOperationFailedException, IOException {
        if (!config.isIncludeBinaries()) {
            logger.debug("Skipping: {} -> binaries are not included in this export configuration", uri);
            return;
        }

        GetBuilder getBuilder = client().get(uri);
        if (external && !config.retrieveExternal()) {
            getBuilder = getBuilder.disableRedirects();
        }
        try (FcrepoResponse response = getBuilder.perform()) {
            checkValidResponse(response, uri, config.getUsername());

            final File file = external ? fileForExternalResources(uri, null, null, config.getBaseDirectory()) :
                    fileForBinary(uri, null, null, config.getBaseDirectory());

            //only retrieve content of external resources when retrieve external flag is enabled
            //otherwise write a zero length file.
            try (final InputStream is = external && !config.retrieveExternal() ?
                    IOUtils.toInputStream("", Charset.defaultCharset()) : response.getBody()) {
                logger.info("Exporting binary: {}", uri);
                writeResponse(uri, is, describedby, file);
                writeHeadersFile(response, getHeadersFile(file));

                // For redirected content export headers from the repository as well
                if (redirect && config.retrieveExternal()) {
                    writeNonRedirectedHeaders(uri, file);
                }
                exportLogger.info("export {} to {}", uri, file.getAbsolutePath());
                successCount.incrementAndGet();
            }

        }

        // Export versions for this binary
        exportVersions(uri);
    }

    private void writeNonRedirectedHeaders(final URI uri, final File file)
        throws FcrepoOperationFailedException, IOException {
        final HeadBuilder headBuilder = client().head(uri).disableRedirects();
        try (final FcrepoResponse response = headBuilder.perform()) {
            final File headers = new File(file.getParentFile(), file.getName() + ".fcrepo" + HEADERS_EXTENSION);
            writeHeadersFile(response, headers);
        }
    }

    private void exportRdf(final URI uri, final URI binaryURI)
            throws FcrepoOperationFailedException {
        final File file = fileForURI(uri, null, null, config.getBaseDirectory(), config.getRdfExtension());
        if (file == null) {
            logger.info("Skipping {}", uri);
            return;
        } else if (file.exists()) {
            logger.info("Already exported {}", uri);
            return;
        }

        final GetBuilder getBuilder = client().get(uri).accept(config.getRdfLanguage());

        final List<URI> includeUris = new ArrayList<>();
        final List<URI> omitUris = new ArrayList<>();

        if (config.retrieveInbound()) {
            includeUris.add(URI.create(INBOUND_REFERENCES.getURI()));
        }
        if (!config.includeMembership()) {
            omitUris.add(URI.create(PREFER_MEMBERSHIP.getURI()));
        }

        getBuilder.preferRepresentation(includeUris, omitUris);

        try (FcrepoResponse response = getBuilder.perform()) {
            checkValidResponse(response, uri, config.getUsername());
            logger.info("Exporting rdf: {}", uri);

            final String responseBody = IOUtils.toString(response.getBody(), StandardCharsets.UTF_8);
            final Model model = createDefaultModel().read(new ByteArrayInputStream(responseBody.getBytes()),
                    null, config.getRdfLanguage());
            Set<URI> inboundMembers = null;

            if (!config.isIncludeBinaries() || config.retrieveInbound()) {

                if (!config.isIncludeBinaries()) {
                    filterBinaryReferences(uri, model);
                }

                if (config.retrieveInbound()) {
                    final URI subject = (binaryURI != null) ? binaryURI : uri;
                    inboundMembers = filterInboundReferences(subject, model);
                }

                try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
                    RDFDataMgr.write(out, model, contentTypeToLang(config.getRdfLanguage()));
                    writeResponse(uri, new ByteArrayInputStream(out.toByteArray()), null, file);
                }
            } else {
                // we can write the body to disk unfiltered
                writeResponse(uri, new ByteArrayInputStream(responseBody.getBytes()), null, file);
            }

            //write headers file
            writeHeadersFile(response, getHeadersFile(file));

            exportLogger.info("export {} to {}", uri, file.getAbsolutePath());
            successCount.incrementAndGet();

            exportMembers(model, inboundMembers);
            exportVersions(uri);
        } catch ( Exception ex ) {
            ex.printStackTrace();
            exportLogger.error(String.format("Error exporting description: %1$s, Cause: %2$s",
                    uri, ex.getMessage()), ex);
        }

    }

    private File getHeadersFile(final File file) {
        return new File(file.getParentFile(), file.getName() + HEADERS_EXTENSION);
    }

    void writeHeadersFile(final FcrepoResponse response, final File file) throws IOException {
        final Map<String, List<String>> headers = response.getHeaders();
        if (!headers.isEmpty()) {
            final String json = new ObjectMapper().writeValueAsString(headers);
            final InputStream byteInputStream = new ByteArrayInputStream(json.getBytes());
            try (OutputStream headerOut = Files.newOutputStream(file.toPath())) {
                copy(byteInputStream, headerOut);
                if (md5FileMap != null) {
                    md5FileMap.put(file, new String(encodeHex(md5.digest())));
                }
                if (sha1FileMap != null) {
                    sha1FileMap.put(file, new String(encodeHex(sha1.digest())));
                }
                if (sha256FileMap != null) {
                    sha256FileMap.put(file, new String(encodeHex(sha256.digest())));
                }
                if (sha512FileMap != null) {
                    sha512FileMap.put(file, new String(encodeHex(sha512.digest())));
                }
            }
        }
    }

    private Set<URI> filterInboundReferences(final URI uri, final Model model) {
        final String withSlash = withSlash(uri).toString();
        final String withoutSlash = withoutSlash(uri).toString();
        final Set<URI> inboundMembers = new HashSet<>();
        final List<Statement> removeList = new ArrayList<>();
        for (final StmtIterator inbound = model.listStatements(); inbound.hasNext(); ) {
            final Statement s = inbound.next();
            final String subject = s.getSubject().toString();
            if (!subject.equals(withSlash) && !subject.equals(withoutSlash)) {
                removeList.add(s);
                logger.trace("Filtering inbound reference: {}", s);
                inboundMembers.add(URI.create(subject));
            }
        }

        model.remove(removeList);
        return inboundMembers;
    }

    private void exportMembers(final Model model, final Set<URI> inboundMembers) {
        for (final String p : config.getPredicates()) {
            final NodeIterator members = model.listObjectsOfProperty(createProperty(p));
            while (members.hasNext()) {
                export(URI.create(members.nextNode().toString()));
            }
        }

        if (inboundMembers != null) {
            for (final URI inbound : inboundMembers) {
                export(inbound);
            }
        }
    }

    /**
     * Filter out the binary resource references from the model
     * @param uri the URI for the resource
     * @param model the RDF Model of the resource
     * @return the RDF model with no binary references
     * @throws FcrepoOperationFailedException
     * @throws IOException
     */
    private void filterBinaryReferences(final URI uri, final Model model) throws IOException,
            FcrepoOperationFailedException {

        final List<Statement> removeList = new ArrayList<>();
        for (final StmtIterator it = model.listStatements(); it.hasNext();) {
            final Statement s = it.nextStatement();

            final RDFNode obj = s.getObject();
            if (obj.isResource() && obj.toString().startsWith(repositoryRoot.toString())
                    && !s.getPredicate().toString().equals(REPOSITORY_NAMESPACE + "hasTransactionProvider")) {
                try (final FcrepoResponse resp = client().head(URI.create(obj.toString())).disableRedirects()
                        .perform()) {
                    checkValidResponse(resp, URI.create(obj.toString()), config.getUsername());
                    final List<URI> linkHeaders = resp.getLinkHeaders("type");
                    if (linkHeaders.contains(binaryURI)) {
                        removeList.add(s);
                    }
                }
            }
        }

        model.remove(removeList);
    }

    /**
     * Method to find and set the repository root from the resource uri.
     * @param uri the URI for the resource
     * @throws IOException If an I/O error occurs
     * @throws FcrepoOperationFailedException If a FcrepoOperationFailedException error occurs
     */
    private void findRepositoryRoot(final URI uri) throws IOException, FcrepoOperationFailedException {
        repositoryRoot = uri;
        logger.debug("Checking if " + uri + " is the repository root");
        if (!isRepositoryRoot(uri, client(), config)) {
            findRepositoryRoot(URI.create(repositoryRoot.toString().substring(0,
                    repositoryRoot.toString().lastIndexOf("/"))));
        }
    }

    /**
     * Initiates export of versions for the given resource if it is a versioned resourced
     * 
     * @param uri resource uri
     * @throws FcrepoOperationFailedException
     * @throws IOException
     */
    private void exportVersions(final URI uri) throws FcrepoOperationFailedException, IOException {
        // Do not check for versions if disabled, already exporting a version, or the repo root
        if (!config.includeVersions() || uri.equals(repositoryRoot)) {
            return;
        }

        // Resolve the timemap endpoint for this resource
        final URI timemapURI;
        try (FcrepoResponse response = client().head(uri).disableRedirects().perform()) {
            checkValidResponse(response, uri, config.getUsername());
            if (response.getLinkHeaders("type").contains(URI.create(MEMENTO.toString()))) {
                logger.trace("Resource {} is a memento and therefore not versioned:  ", uri);
                return;
            } else if (response.getLinkHeaders("type").contains(URI.create(TIMEMAP.toString()))) {
                logger.trace("Resource {} is a timemap and therefore not versioned:  ", uri);
                return;
            }

            timemapURI = response.getLinkHeaders("timemap").stream().findFirst().orElse(null);
            if (timemapURI == null) {
                logger.trace("Resource {} is not versioned:  no rel=\"timemap\" Link header present on {}", uri);
                return;
            }
        }

        export(timemapURI);

        try (FcrepoResponse response = client().get(timemapURI).accept(config.getRdfLanguage()).perform()) {
           // Verify that timemapURI can be accessed, which will fail if the resource is not versioned
            checkValidResponse(response, timemapURI, config.getUsername());
            // Extract uris of mementos for export
            final Model model = createDefaultModel().read(response.getBody(), null, config.getRdfLanguage());
            final StmtIterator versionsIt = model.listStatements();
            while (versionsIt.hasNext()) {
                final Statement versionSt = versionsIt.next();
                if (versionSt.getPredicate().equals(CONTAINS)) {
                    final Resource versionResc = versionSt.getResource();
                    exportLogger.info("Exporting version: {}", versionResc.getURI());
                    logger.info("Exporting version {} for {}", versionResc.getURI(), uri);
                    export(URI.create(versionResc.getURI()));
                }
            }
        }
    }

    private URI addRelativePath(final URI uri, final String path) {
        final String base = uri.toString();

        if (base.charAt(base.length() - 1) == '/') {
            if (path.charAt(0) == '/') {
                return URI.create(base + path.substring(1, path.length()));
            }
            return URI.create(base + path);
        } else if (path.charAt(0) == '/') {
            return URI.create(base + path);
        }

        return URI.create(base + "/" + path);
    }

    void writeResponse(final URI uri, final InputStream in, final List<URI> describedby, final File file)
            throws IOException, FcrepoOperationFailedException {
        if (!file.getParentFile().exists()) {
            file.getParentFile().mkdirs();
        }
        try (OutputStream out = new FileOutputStream(file)) {
            copy(in, out);
            logger.info("Exported {} to {}", uri, file.getAbsolutePath());

            if (md5FileMap != null) {
                md5FileMap.put(file, new String(encodeHex(md5.digest())));
            }
            if (sha1FileMap != null) {
                sha1FileMap.put(file, new String(encodeHex(sha1.digest())));
            }
            if (sha256FileMap != null) {
                sha256FileMap.put(file, new String(encodeHex(sha256.digest())));
            }
            if (sha512FileMap != null) {
                sha512FileMap.put(file, new String(encodeHex(sha512.digest())));
            }
        }

        if (describedby != null) {
            for (final Iterator<URI> it = describedby.iterator(); it.hasNext(); ) {
                exportRdf(it.next(), uri);
            }
        }
    }

    /**
     * Copy bytes and generate checksums
     * @param in Source data
     * @param out Data destination
     * @throws IOException If an I/O error occurs
     */
    private void copy(final InputStream in, final OutputStream out) throws IOException {
        if (md5 != null) {
            md5.reset();
        }
        if (sha1 != null) {
            sha1.reset();
        }
        if (sha256 != null) {
            sha256.reset();
        }
        if (sha512 != null) {
            sha512.reset();
        }

        int read = 0;
        final byte[] buf = new byte[8192];
        while ((read = in.read(buf)) != -1) {
            if (md5 != null) {
                md5.update(buf, 0, read);
            }
            if (sha1 != null) {
                sha1.update(buf, 0, read);
            }
            if (sha256 != null) {
                sha256.update(buf, 0, read);
            }
            if (sha512 != null) {
                sha512.update(buf, 0, read);
            }
            if (out != null) {
                out.write(buf, 0, read);
                successBytes.addAndGet(read);
            }
        }
    }
}
