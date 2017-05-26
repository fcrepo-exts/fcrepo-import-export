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
package org.fcrepo.importexport;

import static java.lang.System.getProperty;
import static org.fcrepo.importexport.ArgParser.CONFIG_FILE_NAME;
import static org.fcrepo.importexport.common.FcrepoConstants.CONTAINS;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.util.Map;

import org.apache.commons.lang3.ArrayUtils;
import org.fcrepo.importexport.common.Config;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


/**
 * @author awoods
 * @since 2016-08-29
 */
public class ArgParserTest {

    private ArgParser parser;

    private static final String[] MINIMAL_VALID_EXPORT_ARGS = new String[]{"-m", "export",
            "-d", "/tmp/rdf",
            "-r", "http://localhost:8080/rest/1"};

    private static final String[] MINIMAL_VALID_IMPORT_ARGS = new String[]{"-m", "import",
            "-d", "/tmp/rdf",
            "-r", "http://localhost:8080/rest/1"};


    @Before
    public void setUp() throws Exception {
        parser = new ArgParser();
    }

    @Test
    public void parseValidExport() throws Exception {
        final String[] args = new String[]{"-m", "export",
                                           "-d", "/tmp/rdf",
                                           "-l", "application/ld+json",
                                           "-r", "http://localhost:8080/rest/1"};
        final Config config = parser.parseConfiguration(args);
        Assert.assertTrue(config.isExport());
        Assert.assertEquals(new File("/tmp/rdf"), config.getBaseDirectory());
        Assert.assertEquals(false, config.isIncludeBinaries());
        Assert.assertEquals(new String[]{ CONTAINS.toString() }, config.getPredicates());
        Assert.assertEquals(".jsonld", config.getRdfExtension());
        Assert.assertEquals("application/ld+json", config.getRdfLanguage());
        Assert.assertEquals(new URI("http://localhost:8080/rest/1"), config.getResource());
        Assert.assertFalse(config.retrieveExternal());
        Assert.assertFalse(config.retrieveInbound());
    }

    @Test
    public void parseRetrieveExternal() throws Exception {
        final String[] args = new String[]{"-m", "export",
                                           "-d", "/tmp/rdf",
                                           "-x",
                                           "-r", "http://localhost:8080/rest/1"};
        final Config config = parser.parseConfiguration(args);
        Assert.assertTrue(config.isExport());
        Assert.assertEquals(true, config.retrieveExternal());
    }

    @Test
    public void parseRetrieveInbound() throws Exception {
        final String[] args = new String[]{"-m", "export",
                                           "-d", "/tmp/rdf",
                                           "-i",
                                           "-r", "http://localhost:8080/rest/1"};
        final Config config = parser.parseConfiguration(args);
        Assert.assertTrue(config.isExport());
        Assert.assertTrue(config.retrieveInbound());
    }

    @Test
    public void parseLegacyModeShort() throws Exception {
        final Config config = parser.parseConfiguration(
                ArrayUtils.addAll(MINIMAL_VALID_IMPORT_ARGS, "-L"));
        Assert.assertTrue(config.isImport());
        Assert.assertTrue(config.isLegacy());
    }

    @Test
    public void parseLegacyMode() throws Exception {
        final Config config = parser.parseConfiguration(
                ArrayUtils.addAll(MINIMAL_VALID_IMPORT_ARGS, "--legacyMode"));
        Assert.assertTrue(config.isImport());
        Assert.assertTrue(config.isLegacy());
    }

    @Test
    public void parseOverwriteTombstones() throws Exception {
        final String[] args = new String[]{"-m", "import",
                                           "-d", "/tmp/rdf",
                                           "-t",
                                           "-r", "http://localhost:8080/rest/1"};
        final Config config = parser.parseConfiguration(args);
        Assert.assertTrue(config.isImport());
        Assert.assertTrue(config.overwriteTombstones());
    }

    @Test
    public void parseIncludeVersions() throws Exception {
        final String[] args = new String[]{"-m", "export",
            "-d", "/tmp/rdf",
            "-V",
            "-r", "http://localhost:8080/rest/1"};
        final Config config = parser.parseConfiguration(args);
        Assert.assertTrue(config.isExport());
        Assert.assertTrue(config.includeVersions());
    }

    @Test
    public void parseMinimalValidExport() throws Exception {
        final Config config = parser.parseConfiguration(MINIMAL_VALID_EXPORT_ARGS);
        Assert.assertTrue(config.isExport());
        Assert.assertEquals(new File("/tmp/rdf"), config.getBaseDirectory());
        Assert.assertEquals(false, config.isIncludeBinaries());
        Assert.assertEquals(new String[]{ CONTAINS.toString() }, config.getPredicates());
        Assert.assertEquals(".ttl", config.getRdfExtension());
        Assert.assertEquals("text/turtle", config.getRdfLanguage());
        Assert.assertEquals(new URI("http://localhost:8080/rest/1"), config.getResource());
        Assert.assertNull(config.getBagProfile());
    }

    @Test(expected = RuntimeException.class)
    public void parseInvalidRdfLanguage() throws Exception {
        parser.parseConfiguration(ArrayUtils.addAll(MINIMAL_VALID_EXPORT_ARGS, "-l", "invalid/language" ));
    }

    @Test
    public void parseBagProfile() throws Exception {
        final Config config = parser.parseConfiguration(ArrayUtils.addAll(MINIMAL_VALID_EXPORT_ARGS,
                "-g", "default", "-G", "path/config.yaml" ));
        Assert.assertEquals("default", config.getBagProfile());
        Assert.assertEquals(new File("/tmp/rdf/data"), config.getBaseDirectory());
        Assert.assertEquals("path/config.yaml", config.getBagConfigPath());
    }

    @Test(expected = RuntimeException.class)
    public void parseBagProfileWithNoConfigSpecified() throws Exception {
        parser.parseConfiguration(ArrayUtils.addAll(MINIMAL_VALID_EXPORT_ARGS, "-g", "default"));
    }

    @Test(expected = RuntimeException.class)
    public void parseBagConfigWithNoProfileSpecified() throws Exception {
        parser.parseConfiguration(ArrayUtils.addAll(MINIMAL_VALID_EXPORT_ARGS, "-G", "/path/to/bag-config.yaml"));
    }

    @Test
    public void parseConfigFile() throws IOException {
        // Create test config file
        final File configFile = File.createTempFile("config-test", ".txt");
        final FileWriter writer = new FileWriter(configFile);
        writer.append("binaries: true\n");
        writer.append("mode: export\n");
        writer.append("resource: http://localhost:8080/rest/test\n");
        writer.append("dir: /tmp/import-export-dir\n");
        writer.append("predicates: http://www.w3.org/ns/ldp#contains,http://example.org/custom\n");
        writer.flush();

        final String[] args = new String[]{"-c", configFile.getAbsolutePath()};
        final Config config = parser.parseConfiguration(args);
        Assert.assertTrue(config.isExport());
        Assert.assertEquals(new File("/tmp/import-export-dir"), config.getBaseDirectory());
        Assert.assertEquals(true, config.isIncludeBinaries());
        Assert.assertEquals(new String[]{"http://www.w3.org/ns/ldp#contains", "http://example.org/custom"},
                config.getPredicates());
        Assert.assertEquals(".ttl", config.getRdfExtension());
        Assert.assertEquals("text/turtle", config.getRdfLanguage());
        Assert.assertEquals(URI.create("http://localhost:8080/rest/test"), config.getResource());

        Assert.assertTrue("Config file should be created!",
                new File(getProperty("java.io.tmpdir"), CONFIG_FILE_NAME).exists());
    }

    @Test (expected = RuntimeException.class)
    public void parseConfigBadKey() throws IOException {
        // Create test config file
        final File configFile = File.createTempFile("config-test", ".txt");
        final FileWriter writer = new FileWriter(configFile);
        writer.append("binaries: true\n");
        writer.append("mode: export\n");
        writer.append("resource: http://localhost:8080/rest/test\n");
        writer.append("baditem: oops\n");
        writer.append("dir: /tmp/import-export-dir\n");
        writer.flush();

        final String[] args = new String[]{"-c", configFile.getAbsolutePath()};
        final Config config = parser.parseConfiguration(args);
    }

    @Test (expected = RuntimeException.class)
    public void parseConfigBadValue() throws IOException {
        // Create test config file
        final File configFile = File.createTempFile("config-test", ".txt");
        final FileWriter writer = new FileWriter(configFile);
        writer.append("binaries: yep\n");
        writer.append("mode: export\n");
        writer.append("resource: http://localhost:8080/rest/test\n");
        writer.append("dir: /tmp/import-export-dir\n");
        writer.flush();

        final String[] args = new String[] { "-c", configFile.getAbsolutePath() };
        final Config config = parser.parseConfiguration(args);

    }

    @Test(expected = RuntimeException.class)
    public void parseDescriptionDirectoryRequired() throws Exception {
        final String[] args = new String[]{"-m", "export", "-r", "http://localhost:8080/rest/1"};
        parser.parse(args);
    }

    @Test (expected = RuntimeException.class)
    public void parseResourceRequired() throws Exception {
        final String[] args = new String[]{"-m", "export", "-d", "/tmp/rdf"};
        parser.parse(args);
    }

    @Test (expected = RuntimeException.class)
    public void parseInvalid() throws Exception {
        final String[] args = new String[]{"junk"};
        parser.parse(args);
    }

    @Test(expected = RuntimeException.class)
    public void parseHelpWithNoOtherArgs() {
        parser.parseConfiguration(new String[] {"-h"});
    }

    @Test(expected = RuntimeException.class)
    public void parseHelpWithMinimumValidArgs() {
        final String[] args = ArrayUtils.addAll(MINIMAL_VALID_EXPORT_ARGS, "-h");
        parser.parseConfiguration(args);
    }

    @Test
    public void parseValidUsername() {
        final String[] args = ArrayUtils.addAll(MINIMAL_VALID_EXPORT_ARGS, "-u",  "user:pass");
        final Config config = parser.parseConfiguration(args);
        Assert.assertEquals("user", config.getUsername());
        Assert.assertEquals("pass", config.getPassword());
    }

    @Test
    public void parseValidUsernameLong() {
        final String[] args = ArrayUtils.addAll(MINIMAL_VALID_EXPORT_ARGS, "--user", "user:pass");
        final Config config = parser.parseConfiguration(args);
        Assert.assertEquals("user", config.getUsername());
        Assert.assertEquals("pass", config.getPassword());
    }

    @Test (expected = RuntimeException.class)
    public void parseInvalidUser() {
        parser.parseConfiguration(ArrayUtils.addAll(MINIMAL_VALID_EXPORT_ARGS, "--u", "wrong"));
    }

    @Test
    public void testImportMap() {
        final String map = "http://localhost:7777/rest,http://localhost:8888/fcrepo/rest";
        final String[] args = new String[]{"-m", "import",
                                           "-d", "/tmp/rdf",
                                           "-r", "http://localhost:8888/fcrepo/rest/1",
                                           "-M", map};
        final Config config = parser.parseConfiguration(args);
        Assert.assertEquals("http://localhost:7777/rest", config.getSource().toString());
        Assert.assertEquals("/rest", config.getSourcePath());
        Assert.assertEquals("http://localhost:8888/fcrepo/rest", config.getDestination().toString());
        Assert.assertEquals("/fcrepo/rest", config.getDestinationPath());
    }

    @Test
    public void testImportEmptySource() {
        final String resource = "http://localhost:8080/rest/1";
        final String[] args = new String[]{"-m", "import",
                                           "-d", "/tmp/rdf",
                                           "-r", resource};
        final Config config = parser.parseConfiguration(args);
        Assert.assertEquals(null, config.getSource());
        Assert.assertEquals(null, config.getDestination());
    }

    @Test
    public void retrieveConfig() {
        final File configFile = new File("src/test/resources/configs/importexport.yml");
        final File dir = new File("/tmp/rdf");
        final File baseDir = new File(dir, "data");
        final Config config = parser.retrieveConfig(configFile);

        Assert.assertEquals("http://www.w3.org/ns/ldp#contains", config.getPredicates()[0]);
        Assert.assertEquals(URI.create("http://localhost:8080/rest/1"), config.getResource());
        Assert.assertEquals(URI.create("http://localhost:8080/rest/2"), config.getSource());
        Assert.assertEquals("default", config.getBagProfile());
        Assert.assertEquals("path/config.yaml", config.getBagConfigPath());
        Assert.assertEquals(baseDir, config.getBaseDirectory());
        Assert.assertEquals(dir.getAbsolutePath(), config.getMap().get("dir"));
        Assert.assertEquals("text/turtle", config.getRdfLanguage());
        Assert.assertEquals(".ttl", config.getRdfExtension());

        Assert.assertFalse(config.retrieveExternal());
        Assert.assertTrue(config.isIncludeBinaries());
        Assert.assertTrue(config.overwriteTombstones());
    }

    @Test
    public void testSerializedConfigCustom() {
        final File baseDir = new File("/path/to/export");
        final String[] args = new String[] {"-m", "import",
                                            "-r", "http://localhost:8686/rest",
                                            "-M", "http://localhost:8686/rest,http://localhost:8080/f4/rest",
                                            "-d", baseDir.getAbsolutePath(),
                                            "-l", "application/ld+json",
                                            "-g", "custom-profile.yml",
                                            "-G", "custom-metadata.yml",
                                            "-p", "http://example.org/sample",
                                            "-b", "-x", "-i", "-t", "-a", "-V"};
        final Map<String, String> config = parser.parseConfiguration(args).getMap();
        Assert.assertEquals("import", config.get("mode"));
        Assert.assertEquals("http://localhost:8686/rest", config.get("resource"));
        Assert.assertEquals("http://localhost:8686/rest,http://localhost:8080/f4/rest", config.get("map"));
        Assert.assertEquals(baseDir.getAbsolutePath(), config.get("dir"));
        Assert.assertEquals("application/ld+json", config.get("rdfLang"));
        Assert.assertEquals("custom-profile.yml", config.get("bag-profile"));
        Assert.assertEquals("custom-metadata.yml", config.get("bag-config"));
        Assert.assertEquals("http://example.org/sample", config.get("predicates"));
        Assert.assertEquals("true", config.get("binaries"));
        Assert.assertEquals("true", config.get("external"));
        Assert.assertEquals("true", config.get("inbound"));
        Assert.assertEquals("true", config.get("overwriteTombstones"));
        Assert.assertEquals("true", config.get("auditLog"));
        Assert.assertEquals("true", config.get("versions"));
    }

    @Test
    public void testSerializedConfigDefault() {
        final File baseDir = new File("/tmp/rdf");
        final String[] args = new String[] {"-m", "import",
                                            "-r", "http://localhost:8080/rest",
                                            "-d", baseDir.getAbsolutePath()};
        final Map<String, String> config = parser.parseConfiguration(args).getMap();
        Assert.assertEquals("import", config.get("mode"));
        Assert.assertEquals("http://localhost:8080/rest", config.get("resource"));
        Assert.assertEquals(null, config.get("map"));
        Assert.assertEquals(baseDir.getAbsolutePath(), config.get("dir"));
        Assert.assertEquals("text/turtle", config.get("rdfLang"));
        Assert.assertEquals(null, config.get("bag-profile"));
        Assert.assertEquals(null, config.get("bag-config"));
        Assert.assertEquals("http://www.w3.org/ns/ldp#contains", config.get("predicates"));
        Assert.assertEquals("false", config.get("binaries"));
        Assert.assertEquals("false", config.get("external"));
        Assert.assertEquals("false", config.get("inbound"));
        Assert.assertEquals("false", config.get("overwriteTombstones"));
        Assert.assertEquals("false", config.get("auditLog"));
        Assert.assertEquals("false", config.get("versions"));
    }
}
