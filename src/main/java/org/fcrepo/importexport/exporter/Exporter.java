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
import static org.fcrepo.importexport.common.FcrepoConstants.CONTAINER;
import static org.fcrepo.importexport.common.FcrepoConstants.NON_RDF_SOURCE;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.fcrepo.client.FcrepoClient;
import org.fcrepo.client.FcrepoOperationFailedException;
import org.fcrepo.client.FcrepoResponse;
import org.fcrepo.importexport.common.AuthenticationRequiredRuntimeException;
import org.fcrepo.importexport.common.AuthorizationDeniedRuntimeException;
import org.fcrepo.importexport.common.BagConfig;
import org.fcrepo.importexport.common.BagProfile;
import org.fcrepo.importexport.common.BagWriter;
import org.fcrepo.importexport.common.Config;
import org.fcrepo.importexport.common.ProfileValidationException;
import org.fcrepo.importexport.common.ProfileValidationUtil;
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
    private static final String APTRUST_INFO_TXT = "aptrust-info.txt";

    private Config config;
    protected FcrepoClient.FcrepoClientBuilder clientBuilder;
    private URI binaryURI;
    private URI containerURI;
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
        this.exportLogger = config.getAuditLog();
        this.dateFormat = new SimpleDateFormat("yyyy-MM-dd");

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
                throw new RuntimeException(String.format("Error loading bag config file: {}", e.getMessage()), e);
            } catch (IOException e) {
                throw new RuntimeException(String.format("Error reading bag profile: {}", e.getMessage()), e);
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

    protected void validateProfile(final String profileSection, final Map<String, Set<String>> requiredFields,
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
            }
        } catch (FcrepoOperationFailedException ex) {
            logger.warn("Error retrieving content: {}", ex.toString());
            exportLogger.error(String.format("Error retrieving context of uri: {}, Message: {}", uri, ex.toString()),
                ex);
        } catch (IOException ex) {
            logger.warn("Error writing content: {}", ex.toString());
            exportLogger.error(String.format("Error writing content from uri: {}, Message: {}", uri, ex.toString()),
                ex);
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
            exportLogger.info("export {} to {}", uri, file.getAbsolutePath());
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
            exportLogger.info("export {} to {}", uri, file.getAbsolutePath());
            successCount.incrementAndGet();
        } catch ( Exception ex ) {
            ex.printStackTrace();
            exportLogger.error(String.format("Error exporting description: {}, Cause: {}", uri, ex.getMessage()), ex);
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

        final List<URI> describedby = response.getLinkHeaders("describedby");
        for (final Iterator<URI> it = describedby.iterator(); describedby != null && it.hasNext(); ) {
            exportDescription(it.next());
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
