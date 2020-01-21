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
import static org.fcrepo.importexport.common.FcrepoConstants.TIMEMAP;
import static org.fcrepo.importexport.common.FcrepoConstants.NON_RDF_SOURCE;
import static org.fcrepo.importexport.common.FcrepoConstants.RDF_SOURCE;
import static org.fcrepo.importexport.common.FcrepoConstants.REPOSITORY_NAMESPACE;

import static org.fcrepo.importexport.common.TransferProcess.checkValidResponse;
import static org.fcrepo.importexport.common.TransferProcess.fileForBinary;
import static org.fcrepo.importexport.common.TransferProcess.fileForExternalResources;
import static org.fcrepo.importexport.common.TransferProcess.fileForURI;
import static org.fcrepo.importexport.common.TransferProcess.isRepositoryRoot;
import static org.fcrepo.importexport.common.UriUtils.withSlash;
import static org.fcrepo.importexport.common.UriUtils.withoutSlash;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.NodeIterator;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.riot.RDFDataMgr;

import org.fcrepo.client.FcrepoClient;
import org.fcrepo.client.FcrepoOperationFailedException;
import org.fcrepo.client.FcrepoResponse;
import org.fcrepo.client.GetBuilder;
import org.fcrepo.importexport.common.BagConfig;
import org.fcrepo.importexport.common.BagProfile;
import org.fcrepo.importexport.common.ProfileFieldRule;
import org.fcrepo.importexport.common.BagWriter;
import org.fcrepo.importexport.common.Config;
import org.fcrepo.importexport.common.ProfileValidationException;
import org.fcrepo.importexport.common.ProfileValidationUtil;
import org.fcrepo.importexport.common.TransferProcess;

import org.slf4j.Logger;

/**
 * Fedora Export Utility
 *
 * @author lsitu
 * @author awoods
 * @author escowles
 * @since 2016-08-29
 */
public class Exporter implements TransferProcess {

    private static final Logger logger = getLogger(Exporter.class);
    private static final String APTRUST_INFO_TXT = "aptrust-info.txt";

    private Config config;
    protected FcrepoClient.FcrepoClientBuilder clientBuilder;
    private URI binaryURI;
    private URI containerURI;
    private URI rdfSourceURI;
    private BagWriter bag;
    private MessageDigest sha1 = null;
    private MessageDigest sha256 = null;
    private MessageDigest md5 = null;
    private HashMap<File, String> sha1FileMap = null;
    private HashMap<File, String> sha256FileMap = null;
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
            try {
                // parse profile
                final URL url = this.getClass().getResource("/profiles/" + config.getBagProfile() + ".json");
                final BagConfig bagConfig = loadBagConfig(config.getBagConfigPath());
                final InputStream in = (url == null) ? new FileInputStream(config.getBagProfile()) : url.openStream();
                final BagProfile bagProfile = new BagProfile(in);

                // always do sha1, do md5/sha256 if the profile asks for it
                final HashSet<String> algorithms = new HashSet<>();
                this.sha1FileMap = new HashMap<>();
                this.sha1 = MessageDigest.getInstance("SHA-1");
                algorithms.add("sha1");
                if (bagProfile.getPayloadDigestAlgorithms().contains("md5")) {
                    this.md5FileMap = new HashMap<>();
                    this.md5 = MessageDigest.getInstance("MD5");
                    algorithms.add("md5");
                }
                if (bagProfile.getPayloadDigestAlgorithms().contains("sha256")) {
                    this.sha256FileMap = new HashMap<>();
                    this.sha256 = MessageDigest.getInstance("SHA-256");
                    algorithms.add("sha256");
                }

                //enforce default metadata
                bagProfile.validateConfig(bagConfig);

                // setup bag
                final File bagdir = config.getBaseDirectory().getParentFile();
                this.bag = new BagWriter(bagdir, algorithms);
                for (final String tagFile : bagConfig.getTagFiles()) {
                    this.bag.addTags(tagFile, bagConfig.getFieldsForTagFile(tagFile));
                }

            } catch (NoSuchAlgorithmException e) {
                // never happens with known algorithm names
            } catch (FileNotFoundException e) {
                throw new RuntimeException(String.format("Error loading bag config file: %1$s", e.getMessage()), e);
            } catch (IOException e) {
                throw new RuntimeException(String.format("Error reading bag profile: %1$s", e.getMessage()), e);
            }
        }
    }

    /**
     * Loads a bag config from path
     * @param bagConfigPath The path to the bag config yaml.
     * @return
     */
    private BagConfig loadBagConfig(final String bagConfigPath) {
        if (bagConfigPath == null) {
            throw new RuntimeException("The bag config path must not be null.");
        }
        final File bagConfigFile = new File(bagConfigPath);
        return new BagConfig(bagConfigFile);
    }

    protected void validateProfile(final String profileSection, final Map<String, ProfileFieldRule> requiredFields,
            final Map<String, String> fields) throws ProfileValidationException {
        ProfileValidationUtil.validate(profileSection, requiredFields, fields);
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
                final Map<String, String> bagMetadata = new HashMap<>();
                bagMetadata.putAll(bag.getTags("bag-info.txt"));
                bagMetadata.putAll(bagTechMetadata());
                bag.addTags("bag-info.txt", bagMetadata);
                bag.registerChecksums("sha1", sha1FileMap);
                if (sha256 != null) {
                    bag.registerChecksums("sha256", sha256FileMap);
                }
                if (md5 != null) {
                    bag.registerChecksums("md5", md5FileMap);
                }
                bag.write();
            } catch (IOException e) {
                throw new RuntimeException("Error finishing Bag: " + e.toString());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        exportLogger.info("Finished export... {} bytes/{} resources exported", successBytes.get(),
                successCount.get());
    }

    private Map<String, String> bagTechMetadata() {
        final Map<String, String> metadata = new HashMap<>();
        metadata.put("Bag-Size", byteCountToDisplaySize(successBytes.longValue()));
        metadata.put("Payload-Oxum", successBytes.toString() + "." + successCount.toString());
        metadata.put("Bagging-Date", dateFormat.format(new Date()));
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
                final List<URI> describedby = response.getLinkHeaders("describedby");
                exportBinary(uri, describedby, external);
            } else if (linkHeaders.contains(containerURI) || linkHeaders.contains(rdfSourceURI)) {
                logger.debug("Found container at " + uri);
                exportRdf(uri, null);
                // Export versions for this container
                exportVersions(uri);
            } else {
                logger.error("Resource is not an LDP Container, LDP RDFSource,  or an LDP NonRDFSource: {}", uri);
                exportLogger.error("Resource is not an LDP Container, LDP RDFSource, or an LDP NonRDFSource: {}", uri);
            }

            if (acl != null) {
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

    private void exportBinary(final URI uri, final List<URI> describedby, final boolean external)
            throws FcrepoOperationFailedException, IOException {

        if (!config.isIncludeBinaries()) {
            logger.info("Skipping {}", uri);
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

            logger.info("Exporting binary: {}", uri);
            writeResponse(uri, response.getBody(), describedby, file);
            writeHeadersFile(response, getHeadersFile(file));
            exportLogger.info("export {} to {}", uri, file.getAbsolutePath());
            successCount.incrementAndGet();

        }

        // Export versions for this binary
        exportVersions(uri);
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

        GetBuilder getBuilder = client().get(uri).accept(config.getRdfLanguage());
        if (config.retrieveInbound()) {
            getBuilder = getBuilder.preferRepresentation(
                Arrays.asList(URI.create(INBOUND_REFERENCES.getURI())), null);
        }

        try (FcrepoResponse response = getBuilder.perform()) {
            checkValidResponse(response, uri, config.getUsername());
            logger.info("Exporting rdf: {}", uri);

            final String responseBody = IOUtils.toString(response.getBody(), "UTF-8");
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
        final String json = new ObjectMapper().writeValueAsString(headers);
        try (final FileWriter writer = new FileWriter(file)) {
            writer.write(json);
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
            if (response.getLinkHeaders("type").stream()
                .filter(x -> x.toString().equals(MEMENTO.getURI()))
                .count() > 0) {
                logger.trace("Resource {} is a memento and therefore not versioned:  ", uri);
                return;
            } else if (response.getLinkHeaders("type").stream()
                .filter(x -> x.toString().equals(TIMEMAP.getURI()))
                .count() > 0) {
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
            if (out != null) {
                out.write(buf, 0, read);
                successBytes.addAndGet(read);
            }
        }
    }
}
