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
import static org.fcrepo.importexport.common.Config.DEFAULT_RDF_EXT;
import static org.fcrepo.importexport.common.Config.DEFAULT_RDF_LANG;
import static org.fcrepo.importexport.common.FcrepoConstants.CONTAINS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.List;
import java.util.UUID;

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

    private final String apTrustProfile = "aptrust";
    private final String btrProfile = "beyondtherepository";
    private final String defaultConfig = "src/test/resources/configs/bagit-config.yml";
    private final String btrConfig = "src/test/resources/configs/bagit-config-no-aptrust.yml";

    /**
     * Common ops for export
     *
     * @param id the id to assign to the bag
     * @param bagProfile the bag profile to use
     * @param bagConfig the path to the bag config for bag-info data
     * @throws Exception
     */
    public void runExportBag(final String id, final String bagProfile, final String bagConfig) throws Exception {
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
        config.setBagProfile(bagProfile);
        config.setBagConfigPath(bagConfig);
        new Exporter(config, clientBuilder).run();

        final Path target = Paths.get(TARGET_DIR, id);
        assertTrue(target.resolve("bagit.txt").toFile().exists());
        assertTrue(target.resolve("manifest-sha1.txt").toFile().exists());

        final Path dataDir = target.resolve("data");
        assertTrue(dataDir.toFile().exists());
        assertTrue(dataDir.toFile().isDirectory());

        final Path resourceFile = Paths.get(dataDir.toString(), uri.getPath() + DEFAULT_RDF_EXT);
        assertTrue(resourceFile.toFile().exists());

        final FcrepoResponse response1 = clientBuilder.build().get(uri).perform();
        final MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        final byte[] buf = new byte[8192];
        int read = 0;
        while ((read = response1.getBody().read(buf)) != -1) {
            sha1.update(buf, 0, read);
        }

        final String checksum = new String(encodeHex(sha1.digest()));
        final BufferedReader reader = new BufferedReader(new FileReader(target.resolve("manifest-sha1.txt").toFile()));
        final String checksumLine = reader.readLine();
        reader.close();
        assertEquals(checksum, checksumLine.split(" ")[0]);
    }

    @Test
    public void testExportDefault() throws Exception {
        final String exampleID = UUID.randomUUID().toString();
        runExportBag(exampleID, DEFAULT_BAG_PROFILE, defaultConfig);

        final Path target = Paths.get(TARGET_DIR, exampleID);
        final Path bagInfo = target.resolve("bag-info.txt");
        assertTrue(bagInfo.toFile().exists());
        final List<String> bagInfoLines = Files.readAllLines(bagInfo);
        assertTrue(bagInfoLines.contains("BagIt-Profile-Identifier: http://fedora.info/bagprofile/default.json"));
    }

    @Test
    public void testExportApTrust() throws Exception {
        final String exampleID = UUID.randomUUID().toString();
        runExportBag(exampleID, apTrustProfile, defaultConfig);

        final Path target = Paths.get(TARGET_DIR, exampleID);
        final Path bagInfo = target.resolve("bag-info.txt");
        assertTrue(bagInfo.toFile().exists());
        assertTrue(target.resolve("aptrust-info.txt").toFile().exists());
        final List<String> bagInfoLines = Files.readAllLines(bagInfo);
        assertTrue(bagInfoLines.contains("BagIt-Profile-Identifier: http://fedora.info/bagprofile/aptrust.json"));
    }

    @Test
    public void testExportBeyondTheRepository() throws Exception {
        final String exampleID = UUID.randomUUID().toString();
        final String bagProfileId = "BagIt-Profile-Identifier: http://fedora.info/bagprofile/beyondtherepository.json";
        runExportBag(exampleID, btrProfile, btrConfig);

        final Path target = Paths.get(TARGET_DIR, exampleID);
        final Path bagInfo = target.resolve("bag-info.txt");
        assertTrue(bagInfo.toFile().exists());
        final List<String> bagInfoLines = Files.readAllLines(bagInfo);
        assertTrue(bagInfoLines.contains(bagProfileId));
    }

    @Test
    public void testImportBag() throws Exception {
        final URI resourceURI = URI.create(serverAddress + "testBagImport");
        final String bagPath = TARGET_DIR + "/test-classes/sample/bag";

        final Config config = new Config();
        config.setMode("import");
        config.setBaseDirectory(bagPath);
        config.setRdfLanguage(DEFAULT_RDF_LANG);
        config.setResource(resourceURI);
        config.setMap(new String[]{"http://localhost:8080/fcrepo/rest/", serverAddress});
        config.setUsername(USERNAME);
        config.setPassword(PASSWORD);
        config.setBagProfile(DEFAULT_BAG_PROFILE);
        config.setLegacy(true);

        // Resource doesn't exist
        assertFalse(exists(resourceURI));

        // run import
        final Importer importer = new Importer(config, clientBuilder);
        importer.run();

        // Resource does exist.
        assertTrue(exists(resourceURI));
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
        config.setBagProfile(btrProfile);
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

    @Override
    protected Logger logger() {
        return getLogger(BagItIT.class);
    }

}
