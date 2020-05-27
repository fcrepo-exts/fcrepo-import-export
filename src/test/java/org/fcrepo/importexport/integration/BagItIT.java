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
package org.fcrepo.importexport.integration;

import static org.apache.commons.codec.binary.Hex.encodeHex;
import static org.apache.http.HttpStatus.SC_CREATED;
import static org.duraspace.bagit.BagConfig.BAG_INFO_KEY;
import static org.duraspace.bagit.BagProfileConstants.BAGIT_MD5;
import static org.duraspace.bagit.BagProfileConstants.BAGIT_PROFILE_IDENTIFIER;
import static org.duraspace.bagit.BagProfileConstants.BAGIT_SHA1;
import static org.duraspace.bagit.BagProfileConstants.BAGIT_SHA_256;
import static org.fcrepo.importexport.common.Config.DEFAULT_RDF_EXT;
import static org.fcrepo.importexport.common.Config.DEFAULT_RDF_LANG;
import static org.fcrepo.importexport.common.FcrepoConstants.CONTAINS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.IOUtils;
import org.apache.jena.ext.com.google.common.collect.ImmutableMap;
import org.duraspace.bagit.BagItDigest;
import org.duraspace.bagit.BagProfile;
import org.duraspace.bagit.BagProfileConstants;
import org.fcrepo.client.FcrepoOperationFailedException;
import org.fcrepo.client.FcrepoResponse;
import org.fcrepo.importexport.common.Config;
import org.fcrepo.importexport.exporter.Exporter;
import org.fcrepo.importexport.importer.Importer;
import org.junit.Test;
import org.slf4j.Logger;

/**
 * @author whikloj
 * @since 2016-12-12
 */
public class BagItIT extends AbstractResourceIT {

    private final String defaultConfig = "src/test/resources/configs/bagit-config.yml";
    private final String btrConfig = "src/test/resources/configs/bagit-config-no-aptrust.yml";

    /**
     * Common ops for export
     *
     * @param id the id to assign to the bag
     * @param bagProfile the bag profile to use
     * @param bagConfig the path to the bag config for bag-info data
     * @param bagItDigest a BagItDigest which is expected to be found when the export is complete
     * @throws Exception
     */
    public void runExportBag(final String id, final BagProfile.BuiltIn bagProfile, final String bagConfig,
                             final BagItDigest bagItDigest) throws Exception {
        final URI uri = URI.create(serverAddress + id);
        final String manifestName = "manifest-" + bagItDigest.bagitName() + ".txt";

        final FcrepoResponse response = create(uri);
        assertEquals(SC_CREATED, response.getStatusCode());
        assertEquals(uri, response.getLocation());

        final Config config = new Config();
        config.setMode("export");
        config.setBaseDirectory(TARGET_DIR + File.separator + id);
        config.setIncludeBinaries(true);
        config.setResource(uri);
        config.setPredicates(new String[]{CONTAINS.toString()});
        config.setRdfExtension(DEFAULT_RDF_EXT);
        config.setRdfLanguage(DEFAULT_RDF_LANG);
        config.setUsername(USERNAME);
        config.setPassword(PASSWORD);
        config.setBagProfile(bagProfile.getIdentifier());
        config.setBagConfigPath(bagConfig);
        new Exporter(config, clientBuilder).run();

        final Path target = Paths.get(TARGET_DIR, id);
        assertTrue(target.resolve("bagit.txt").toFile().exists());
        assertTrue(target.resolve(manifestName).toFile().exists());

        final Path dataDir = target.resolve("data");
        assertTrue(dataDir.toFile().exists());
        assertTrue(dataDir.toFile().isDirectory());

        final Path resourceFile = Paths.get(dataDir.toString(), uri.getPath() + DEFAULT_RDF_EXT);
        assertTrue(resourceFile.toFile().exists());

        final FcrepoResponse response1 = clientBuilder.build().get(uri).perform();
        final MessageDigest messageDigest = bagItDigest.messageDigest();
        final byte[] buf = new byte[8192];
        int read = 0;
        while ((read = response1.getBody().read(buf)) != -1) {
            messageDigest.update(buf, 0, read);
        }
        final String checksum = new String(encodeHex(messageDigest.digest()));

        try (Stream<String> manifest = Files.lines(target.resolve(manifestName))) {
            final Set<String> checksums = manifest.map(line -> line.split(" ")[0].trim()).collect(Collectors.toSet());

            assertEquals(2, checksums.size());
            assertTrue(checksums.contains(checksum));
        }
    }

    @Test
    public void testExportDefault() throws Exception {
        final BagProfile bagProfile = new BagProfile(BagProfile.BuiltIn.DEFAULT);
        final String bagProfileId = BAGIT_PROFILE_IDENTIFIER + ": " + bagProfile.getIdentifier();

        final String exampleID = UUID.randomUUID().toString();
        runExportBag(exampleID, BagProfile.BuiltIn.DEFAULT, defaultConfig, BagItDigest.SHA1);

        final Path target = Paths.get(TARGET_DIR, exampleID);
        final Path bagInfo = target.resolve(BAG_INFO_KEY);
        assertTrue(bagInfo.toFile().exists());
        final List<String> bagInfoLines = Files.readAllLines(bagInfo);
        assertTrue(bagInfoLines.contains(bagProfileId));
    }

    @Test
    public void testExportApTrust() throws Exception {
        final BagProfile bagProfile = new BagProfile(BagProfile.BuiltIn.APTRUST);
        final String bagProfileId = BAGIT_PROFILE_IDENTIFIER + ": " + bagProfile.getIdentifier();

        final String exampleID = UUID.randomUUID().toString();
        runExportBag(exampleID, BagProfile.BuiltIn.APTRUST, defaultConfig, BagItDigest.MD5);

        final Path target = Paths.get(TARGET_DIR, exampleID);
        final Path bagInfo = target.resolve(BAG_INFO_KEY);
        assertTrue(bagInfo.toFile().exists());
        assertTrue(target.resolve("aptrust-info.txt").toFile().exists());
        final List<String> bagInfoLines = Files.readAllLines(bagInfo);
        assertTrue(bagInfoLines.contains(bagProfileId));
    }

    @Test
    public void testExportBeyondTheRepository() throws Exception {
        final String exampleID = UUID.randomUUID().toString();
        final BagProfile bagProfile = new BagProfile(BagProfile.BuiltIn.BEYOND_THE_REPOSITORY);
        final String bagProfileId = BAGIT_PROFILE_IDENTIFIER + ": " + bagProfile.getIdentifier();
        runExportBag(exampleID, BagProfile.BuiltIn.BEYOND_THE_REPOSITORY, btrConfig, BagItDigest.SHA1);

        final Path target = Paths.get(TARGET_DIR, exampleID);
        final Path bagInfo = target.resolve(BAG_INFO_KEY);
        assertTrue(bagInfo.toFile().exists());
        final List<String> bagInfoLines = Files.readAllLines(bagInfo);
        assertTrue(bagInfoLines.contains(bagProfileId));
    }

    @Test
    public void testExportBtRSerialized() throws Exception {
        final String id = UUID.randomUUID().toString();
        final String serializationType = "tar";

        final URI uri = URI.create(serverAddress + id);

        final FcrepoResponse response = create(uri);
        assertEquals(SC_CREATED, response.getStatusCode());
        assertEquals(uri, response.getLocation());

        final Config config = new Config();
        config.setMode("export");
        config.setBaseDirectory(TARGET_DIR + File.separator + id);
        config.setIncludeBinaries(true);
        config.setResource(uri);
        config.setPredicates(new String[]{CONTAINS.toString()});
        config.setRdfExtension(DEFAULT_RDF_EXT);
        config.setRdfLanguage(DEFAULT_RDF_LANG);
        config.setUsername(USERNAME);
        config.setPassword(PASSWORD);
        config.setBagProfile(BagProfile.BuiltIn.BEYOND_THE_REPOSITORY.getIdentifier());
        config.setBagConfigPath(btrConfig);
        config.setBagSerialization(serializationType);
        new Exporter(config, clientBuilder).run();

        final Path target = Paths.get(TARGET_DIR, id + "." + serializationType);
        assertTrue(Files.exists(target));
        assertTrue(Files.isRegularFile(target));

        // Create a map of the bag files and their content instead of extracting
        final Map<String, List<String>> filesFromArchive = new HashMap<>();
        try (final InputStream is = Files.newInputStream(target);
             final TarArchiveInputStream tais = new TarArchiveInputStream(is)) {
            TarArchiveEntry entry;

            while ((entry = tais.getNextTarEntry()) != null) {
                final List<String> lines = IOUtils.readLines(tais, Charset.defaultCharset());
                filesFromArchive.put(entry.getName(), lines);
            }
        }

        // check bagit.txt, bag-info.txt, and data files
        final String bagItPath = id + "/bagit.txt";
        final String bagInfoPath = id + "/" + BAG_INFO_KEY;
        final String dataFilePath = id + "/data" + uri.getPath() + DEFAULT_RDF_EXT;
        final String headersFilePath = dataFilePath + ".headers";
        assertTrue("Could not find: " + bagItPath, filesFromArchive.containsKey(bagItPath));
        assertTrue("Could not find: " + bagInfoPath, filesFromArchive.containsKey(bagInfoPath));
        assertTrue("Could not find: " + dataFilePath, filesFromArchive.containsKey(dataFilePath));
        assertTrue("Could not find: " + headersFilePath, filesFromArchive.containsKey(headersFilePath));

        // check the BagIt-Profile-Identifier value exists in the bag-info
        final BagProfile bagProfile = new BagProfile(BagProfile.BuiltIn.BEYOND_THE_REPOSITORY);
        final String bagProfileId = BAGIT_PROFILE_IDENTIFIER + ": " + bagProfile.getIdentifier();
        final List<String> packagedBagInfo = filesFromArchive.get(bagInfoPath);
        assertTrue("Could not find bag-info entry for: " + bagProfileId, packagedBagInfo.contains(bagProfileId));

        // use the allowed manifests to find each tag + payload manifest and check accuracy
        // there isn't a helper in BagItDigest yet to create from the algorithm name, so create a mapping
        final Map<String, BagItDigest> digestMap = ImmutableMap.of(BAGIT_MD5, BagItDigest.MD5,
                                                                   BAGIT_SHA1, BagItDigest.SHA1,
                                                                   BAGIT_SHA_256, BagItDigest.SHA256,
                                                                   "sha512", BagItDigest.SHA512);
        for (String algorithm : bagProfile.getAllowedPayloadAlgorithms()) {
            // check both tag and payload manifests were found in the tarball
            final String manifestPath = id + "/manifest-" + algorithm + BagProfileConstants.BAGIT_TAG_SUFFIX;
            final String tagManifestPath = id + "/tagmanifest-" + algorithm + BagProfileConstants.BAGIT_TAG_SUFFIX;
            assertTrue("Could not find: " + manifestPath, filesFromArchive.containsKey(manifestPath));
            assertTrue("Could not find: " + tagManifestPath, filesFromArchive.containsKey(tagManifestPath));

            // Calculate the checksum for the data file
            final BagItDigest bagItDigest = digestMap.get(algorithm);
            final FcrepoResponse fcrepoResponse = clientBuilder.build().get(uri).perform();
            final MessageDigest messageDigest = bagItDigest.messageDigest();
            final byte[] buf = new byte[8192];
            int read = 0;
            while ((read = fcrepoResponse.getBody().read(buf)) != -1) {
                messageDigest.update(buf, 0, read);
            }
            final String checksum = new String(encodeHex(messageDigest.digest()));

            // and check that the entry exists for the data file
            // note that the entryPath is in the manifest so it is relative to the bag root
            final String entryPath = "data" + uri.getPath() + DEFAULT_RDF_EXT;
            final String expectedEntry = checksum + "  " + entryPath;
            final List<String> foundEntry = filesFromArchive.get(manifestPath);
            assertTrue("Could not find matching manifest entry for: " + expectedEntry + "; existing are\n" + foundEntry,
                       foundEntry.contains(expectedEntry));
        }
    }

    @Test
    public void testImportBag() throws Exception {
        final URI rootURI = URI.create(serverAddress);
        final URI resourceURI = URI.create(serverAddress + "testBagImport");
        final URI metadataURI = URI.create(serverAddress + "image0/fcr:metadata");
        final String bagPath = TARGET_DIR + "/test-classes/sample/bag";

        final Config config = new Config();
        config.setMode("import");
        config.setBaseDirectory(bagPath);
        config.setRdfLanguage(DEFAULT_RDF_LANG);
        config.setResource(rootURI);
        config.setMap(new String[]{"http://localhost:8080/fcrepo/rest/", serverAddress});
        config.setUsername(USERNAME);
        config.setPassword(PASSWORD);
        config.setBagProfile(DEFAULT_BAG_PROFILE);
        config.setIncludeBinaries(true);
        config.setLegacy(true);

        // Resource doesn't exist
        if (exists(resourceURI)) {
            removeAndReset(resourceURI);
        }
        assertFalse(exists(resourceURI));

        // run import
        final Importer importer = new Importer(config, clientBuilder);
        importer.run();

        // Resource does exist.
        assertTrue(exists(resourceURI));
        assertTrue(exists(metadataURI));

        final String metadata = getAsString(metadataURI);
        assertNotNull(metadata);
        assertTrue(metadata.contains("urn:sha-256"));
    }

    @Test
    public void testImportBeyondTheRepositoryBag() throws FcrepoOperationFailedException {
        final URI resourceURI = URI.create(serverAddress + "testBagBtRImport");
        final String bagPath = TARGET_DIR + "/test-classes/sample/bag";

        final Config config = new Config();
        config.setMode("import");
        config.setBaseDirectory(bagPath);
        config.setRdfLanguage(DEFAULT_RDF_LANG);
        config.setResource(resourceURI);
        config.setMap(new String[]{"http://localhost:8080/fcrepo/rest/", serverAddress});
        config.setUsername(USERNAME);
        config.setPassword(PASSWORD);
        config.setBagProfile(BagProfile.BuiltIn.BEYOND_THE_REPOSITORY.getIdentifier());
        config.setLegacy(true);

        // Remove resource from any previously run ITs and verify it does not exist
        if (exists(resourceURI)) {
            removeAndReset(resourceURI);
            assertFalse(exists(resourceURI));
        }

        final Importer importer = new Importer(config, clientBuilder);
        importer.run();

        // Resource does exist.
        assertTrue(exists(resourceURI));
    }

    @Test
    public void testImportSerializedBag() throws FcrepoOperationFailedException {
        final URI rootURI = URI.create(serverAddress);
        final URI resourceURI = URI.create(serverAddress + "testBagImport");
        final String bagPath = TARGET_DIR + "/test-classes/sample/compress/bag-tar.tar";

        final Config config = new Config();
        config.setMode("import");
        config.setBaseDirectory(bagPath);
        config.setRdfLanguage(DEFAULT_RDF_LANG);
        config.setResource(rootURI);
        config.setMap(new String[]{"http://localhost:8080/fcrepo/rest/", serverAddress});
        config.setUsername(USERNAME);
        config.setPassword(PASSWORD);
        config.setBagProfile(BagProfile.BuiltIn.BEYOND_THE_REPOSITORY.getIdentifier());
        config.setIncludeBinaries(true);
        config.setLegacy(true);

        // Resource doesn't exist
        if (exists(resourceURI)) {
            removeAndReset(resourceURI);
        }
        assertFalse(exists(resourceURI));

        // run import
        final Importer importer = new Importer(config, clientBuilder);
        importer.run();

        // Resource does exist.
        assertTrue(exists(resourceURI));
    }

    @Test(expected = RuntimeException.class)
    public void testImportBagVerifyBinaryDigest() {
        final URI resourceURI = URI.create(serverAddress);
        final String bagPath = TARGET_DIR + "/test-classes/sample/bagcorrupted";

        final Config config = new Config();
        config.setMode("import");
        config.setBaseDirectory(bagPath);
        config.setIncludeBinaries(true);
        config.setRdfLanguage(DEFAULT_RDF_LANG);
        config.setResource(resourceURI);
        config.setMap(new String[] { "http://localhost:8080/fcrepo/rest/", serverAddress });
        config.setUsername(USERNAME);
        config.setPassword(PASSWORD);
        config.setBagProfile(DEFAULT_BAG_PROFILE);

        // run import, expected to fail on bag validation
        final Importer importer = new Importer(config, clientBuilder);
        importer.run();
    }

    @Test(expected = RuntimeException.class)
    public void testImportBagFailsProfileValidation() {
        final URI resourceURI = URI.create(serverAddress);
        final String bagPath = TARGET_DIR + "/test-classes/sample/baginvalid";

        final Config config = new Config();
        config.setMode("import");
        config.setBaseDirectory(bagPath);
        config.setIncludeBinaries(true);
        config.setRdfLanguage(DEFAULT_RDF_LANG);
        config.setResource(resourceURI);
        config.setMap(new String[] { "http://localhost:8080/fcrepo/rest/", serverAddress });
        config.setUsername(USERNAME);
        config.setPassword(PASSWORD);
        config.setBagProfile(DEFAULT_BAG_PROFILE);

        // run import, expected to fail on bag validation
        final Importer importer = new Importer(config, clientBuilder);
        importer.run();
    }

    @Override
    protected Logger logger() {
        return getLogger(BagItIT.class);
    }

}
