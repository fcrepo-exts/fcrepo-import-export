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
package org.fcrepo.importexport.common;

import static org.fcrepo.importexport.common.TransferProcess.IMPORT_EXPORT_LOG_PREFIX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.slf4j.helpers.NOPLogger.NOP_LOGGER;

import java.io.File;
import java.net.URI;
import java.util.Map;

import org.junit.Test;

/**
 * @author dfield
 */
public class ConfigTest {

    @Test
    public void testDefaults() {
        final Config config = new Config();
        assertTrue(config.isExport());
        assertFalse(config.isImport());
        assertNull(config.getSource());
        assertNull(config.getSourcePath());
        assertNull(config.getDestination());
        assertNull(config.getDestinationPath());
        assertFalse(config.isRdfSet());
        assertEquals(Config.DEFAULT_RDF_LANG, config.getRdfLanguage());
        assertSame(NOP_LOGGER, config.getAuditLog());
        assertFalse(config.isSkipTombstoneErrors());
    }

    @Test
    public void testStreamingDefaultLanguage() {
        final Config config = new Config();
        config.setStreaming(true);
        assertEquals(Config.DEFAULT_STREAMING_RDF_LANG, config.getRdfLanguage());
    }

    @Test
    public void testNullBaseDirectory() {
        final Config config = new Config();
        config.setBaseDirectory(null);
        assertNull(config.getBaseDirectory());
    }

    @Test
    public void testBagBaseDirectory() {
        final Config config = new Config();
        config.setBaseDirectory("/tmp/bag");
        config.setBagProfile("default");
        assertEquals(new File("/tmp/bag", "data"), config.getBaseDirectory());
    }

    @Test
    public void testResourceTrailingSlash() {
        final Config config = new Config();
        config.setResource("http://localhost:8080/rest/");
        assertEquals(URI.create("http://localhost:8080/rest"), config.getResource());
    }

    @Test
    public void testMap() {
        final Config config = new Config();
        config.setMap(new String[]{"http://localhost:8080/rest/", "http://example.org/fcrepo/rest/"});
        assertEquals(URI.create("http://localhost:8080/rest/"), config.getSource());
        assertEquals("/rest/", config.getSourcePath());
        assertEquals("/fcrepo/rest/", config.getDestinationPath());
    }

    @Test
    public void testMapMismatchedTrailingSlash() {
        final Config config = new Config();
        config.setMap(new String[]{"http://localhost:8080/rest", "http://example.org/rest/"});
        assertEquals(URI.create("http://localhost:8080/rest"), config.getSource());
        assertEquals(URI.create("http://example.org/rest"), config.getDestination());

        config.setMap(new String[]{"http://localhost:8080/rest/", "http://example.org/rest"});
        assertEquals(URI.create("http://localhost:8080/rest"), config.getSource());
        assertEquals(URI.create("http://example.org/rest"), config.getDestination());
    }

    @Test
    public void testInvalidMap() {
        final Config config = new Config();
        assertThrows(IllegalArgumentException.class, () -> config.setMap(new String[]{"http://localhost/rest"}));
        assertThrows(IllegalArgumentException.class,
                () -> config.setMap(new String[]{null, "http://localhost/rest"}));
        assertThrows(IllegalArgumentException.class,
                () -> config.setMap(new String[]{"http://localhost/rest", null}));
    }

    @Test
    public void testInvalidRdfLanguage() {
        final Config config = new Config();
        assertThrows(RuntimeException.class, () -> config.setRdfLanguage("application/not-rdf"));
    }

    @Test
    public void testAuditLog() {
        final Config config = new Config();
        config.setAuditLog(true);
        assertEquals(IMPORT_EXPORT_LOG_PREFIX, config.getAuditLog().getName());
    }

    @Test
    public void testRepositoryRoot() {
        final Config config = new Config();
        config.setRepositoryRoot(URI.create("http://localhost:8080/rest"));
        assertEquals(URI.create("http://localhost:8080/rest"), config.getRepositoryRoot());
        config.setRepositoryRoot("http://localhost:8080/fcrepo/rest");
        assertEquals(URI.create("http://localhost:8080/fcrepo/rest"), config.getRepositoryRoot());
    }

    @Test
    public void testThreadCount() {
        final Config config = new Config();
        config.setThreadCount(4);
        assertEquals(Integer.valueOf(4), config.getThreadCount());
        config.setThreadCount(0);
        assertNull(config.getThreadCount());
        config.setThreadCount(null);
        assertNull(config.getThreadCount());
    }

    @Test
    public void testGetMapMinimal() {
        final Config config = new Config();
        config.setMode("import");
        config.setResource("http://localhost:8080/rest/1");
        config.setBaseDirectory("/tmp/import");

        final Map<String, String> map = config.getMap();
        assertEquals("import", map.get("mode"));
        assertEquals("http://localhost:8080/rest/1", map.get("resource"));
        assertFalse(map.containsKey("map"));
        assertFalse(map.containsKey("bag-profile"));
        assertFalse(map.containsKey("threadCount"));
        assertFalse(map.containsKey("resourceFile"));
    }

    @Test
    public void testGetMapFull() {
        final Config config = new Config();
        config.setMode("export");
        config.setResource("http://localhost:8080/rest/1");
        config.setBaseDirectory("/tmp/export");
        config.setMap(new String[]{"http://localhost:8080/rest", "http://example.org/rest"});
        config.setRdfLanguage("application/ld+json");
        config.setIncludeAcls(true);
        config.setIncludeBinaries(true);
        config.setRetrieveExternal(true);
        config.setRetrieveInbound(true);
        config.setIncludeMembership(true);
        config.setOverwriteTombstones(true);
        config.setLegacy(true);
        config.setIncludeVersions(true);
        config.setBagProfile("default");
        config.setBagConfigPath("/tmp/bag-config.yml");
        config.setBagSerialization("tar");
        config.setBagAlgorithms(new String[]{"sha1", "md5"});
        config.setPredicates(new String[]{"http://example.org/a", "http://example.org/b"});
        config.setAuditLog(true);
        config.setThreadCount(3);
        config.setResourceFile(new File("/tmp/resources.txt").toPath());
        config.setStreaming(true);

        final Map<String, String> map = config.getMap();
        assertEquals("export", map.get("mode"));
        assertEquals("http://localhost:8080/rest,http://example.org/rest", map.get("map"));
        assertEquals("application/ld+json", map.get("rdfLang"));
        assertEquals("true", map.get("acls"));
        assertEquals("true", map.get("binaries"));
        assertEquals("true", map.get("external"));
        assertEquals("true", map.get("inbound"));
        assertEquals("true", map.get("membership"));
        assertEquals("true", map.get("overwriteTombstones"));
        assertEquals("true", map.get("legacyMode"));
        assertEquals("true", map.get("versions"));
        assertEquals("default", map.get("bag-profile"));
        assertEquals("/tmp/bag-config.yml", map.get("bag-config"));
        assertEquals("tar", map.get("bag-serialization"));
        assertEquals("sha1,md5", map.get("bag-algorithms"));
        assertEquals("http://example.org/a,http://example.org/b", map.get("predicates"));
        assertEquals("true", map.get("auditLog"));
        assertEquals("3", map.get("threadCount"));
        assertEquals(new File("/tmp/resources.txt").getAbsolutePath(), map.get("resourceFile"));
        assertEquals("true", map.get("streaming"));
        assertEquals("true", map.get("isRdfSet"));
    }
}
