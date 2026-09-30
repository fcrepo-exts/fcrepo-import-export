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

import static org.duraspace.bagit.profile.BagProfile.BuiltIn.FEDORA_IMPORT_EXPORT;
import static org.fcrepo.importexport.common.FcrepoConstants.CONTAINS;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.text.ParseException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.lang3.ArrayUtils;
import org.duraspace.bagit.profile.BagProfile;
import org.fcrepo.importexport.common.Config;
import org.fcrepo.importexport.common.TransferProcess;
import org.fcrepo.importexport.exporter.Exporter;
import org.fcrepo.importexport.importer.Importer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


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


    @BeforeEach
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
        Assertions.assertTrue(config.isExport());
        Assertions.assertEquals(new File("/tmp/rdf"), config.getBaseDirectory());
        Assertions.assertFalse(config.isIncludeBinaries());
        Assertions.assertArrayEquals(new String[]{ CONTAINS.toString() }, config.getPredicates());
        Assertions.assertEquals(".jsonld", config.getRdfExtension());
        Assertions.assertEquals("application/ld+json", config.getRdfLanguage());
        Assertions.assertEquals(new URI("http://localhost:8080/rest/1"), config.getResource());
        Assertions.assertFalse(config.retrieveExternal());
        Assertions.assertFalse(config.retrieveInbound());
        Assertions.assertNull(config.getWriteConfig());
    }

    @Test
    public void parseRetrieveExternal() throws Exception {
        final String[] args = new String[]{"-m", "export",
                                           "-d", "/tmp/rdf",
                                           "-x",
                                           "-r", "http://localhost:8080/rest/1"};
        final Config config = parser.parseConfiguration(args);
        Assertions.assertTrue(config.isExport());
        Assertions.assertEquals(true, config.retrieveExternal());
    }

    @Test
    public void parseWriteConfig() throws Exception {
        final String[] args = new String[]{"-m", "export",
                                           "-d", "/tmp/rdf",
                                           "-w", "target/sample.yml",
                                           "-r", "http://localhost:8080/rest/1"};
        final Config config = parser.parseConfiguration(args);
        Assertions.assertEquals(new File("target/sample.yml"), config.getWriteConfig());
    }

    @Test
    public void parseRetrieveInbound() throws Exception {
        final String[] args = new String[]{"-m", "export",
                                           "-d", "/tmp/rdf",
                                           "-i",
                                           "-r", "http://localhost:8080/rest/1"};
        final Config config = parser.parseConfiguration(args);
        Assertions.assertTrue(config.isExport());
        Assertions.assertTrue(config.retrieveInbound());
    }

    @Test
    public void parseLegacyModeShort() throws Exception {
        final Config config = parser.parseConfiguration(
                ArrayUtils.addAll(MINIMAL_VALID_IMPORT_ARGS, "-L"));
        Assertions.assertTrue(config.isImport());
        Assertions.assertTrue(config.isLegacy());
    }

    @Test
    public void parseLegacyMode() throws Exception {
        final Config config = parser.parseConfiguration(
                ArrayUtils.addAll(MINIMAL_VALID_IMPORT_ARGS, "--legacyMode"));
        Assertions.assertTrue(config.isImport());
        Assertions.assertTrue(config.isLegacy());
    }

    @Test
    public void parseOverwriteTombstones() throws Exception {
        final String[] args = new String[]{"-m", "import",
                                           "-d", "/tmp/rdf",
                                           "-t",
                                           "-r", "http://localhost:8080/rest/1"};
        final Config config = parser.parseConfiguration(args);
        Assertions.assertTrue(config.isImport());
        Assertions.assertTrue(config.overwriteTombstones());
    }

    @Test
    public void parseIncludeVersions() throws Exception {
        final String[] args = new String[]{"-m", "export",
            "-d", "/tmp/rdf",
            "-V",
            "-r", "http://localhost:8080/rest/1"};
        final Config config = parser.parseConfiguration(args);
        Assertions.assertTrue(config.isExport());
        Assertions.assertTrue(config.includeVersions());
    }

    @Test
    public void parseMinimalValidExport() throws Exception {
        final Config config = parser.parseConfiguration(MINIMAL_VALID_EXPORT_ARGS);
        Assertions.assertTrue(config.isExport());
        Assertions.assertEquals(new File("/tmp/rdf"), config.getBaseDirectory());
        Assertions.assertFalse(config.isIncludeBinaries());
        Assertions.assertArrayEquals(new String[]{ CONTAINS.toString() }, config.getPredicates());
        Assertions.assertEquals(".ttl", config.getRdfExtension());
        Assertions.assertEquals("text/turtle", config.getRdfLanguage());
        Assertions.assertEquals(new URI("http://localhost:8080/rest/1"), config.getResource());
        Assertions.assertNull(config.getBagProfile());
    }

    @Test
    public void parseInvalidRdfLanguage() throws Exception {
        assertThrows(RuntimeException.class, () ->
            parser.parseConfiguration(ArrayUtils.addAll(MINIMAL_VALID_EXPORT_ARGS, "-l", "invalid/language")));
    }

    @Test
    public void parseBagProfile() throws Exception {
        final Config config = parser.parseConfiguration(ArrayUtils.addAll(MINIMAL_VALID_EXPORT_ARGS,
                "-g", "default", "-G", "path/config.yaml", "-s", "zip", "--bag-algorithms", "md5,sha1" ));
        Assertions.assertEquals("zip", config.getBagSerialization());
        Assertions.assertEquals("default", config.getBagProfile());
        Assertions.assertEquals(new File("/tmp/rdf/data"), config.getBaseDirectory());
        Assertions.assertEquals("path/config.yaml", config.getBagConfigPath());
        Assertions.assertArrayEquals(new String[]{"md5", "sha1"}, config.getBagAlgorithms());

        final BagProfile bagProfile = config.initBagProfile();
        final BagProfile fedoraProfile = new BagProfile(FEDORA_IMPORT_EXPORT);
        Assertions.assertEquals(fedoraProfile.getIdentifier(), bagProfile.getIdentifier());
    }

    @Test
    public void parseBagProfileWithNoConfigSpecified() throws Exception {
        assertThrows(RuntimeException.class, () ->
            parser.parseConfiguration(ArrayUtils.addAll(MINIMAL_VALID_EXPORT_ARGS, "-g", "default")));
    }

    @Test
    public void parseBagConfigWithNoProfileSpecified() throws Exception {
        assertThrows(RuntimeException.class, () ->
            parser.parseConfiguration(ArrayUtils.addAll(MINIMAL_VALID_EXPORT_ARGS, "-G", "/path/to/bag-config.yaml")));
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
        Assertions.assertTrue(config.isExport());
        Assertions.assertEquals(new File("/tmp/import-export-dir"), config.getBaseDirectory());
        Assertions.assertTrue(config.isIncludeBinaries());
        Assertions.assertArrayEquals(new String[]{"http://www.w3.org/ns/ldp#contains", "http://example.org/custom"},
                config.getPredicates());
        Assertions.assertEquals(".ttl", config.getRdfExtension());
        Assertions.assertEquals("text/turtle", config.getRdfLanguage());
        Assertions.assertEquals(URI.create("http://localhost:8080/rest/test"), config.getResource());
    }

    @Test
    public void parseConfigBadKey() throws IOException {
        assertThrows(RuntimeException.class, () -> {
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
        });
    }

    @Test
    public void parseConfigBadValue() throws IOException {
        assertThrows(RuntimeException.class, () -> {
            // Create test config file
            final File configFile = File.createTempFile("config-test", ".txt");
            final FileWriter writer = new FileWriter(configFile);
            writer.append("binaries: yep\n");
            writer.append("mode: export\n");
            writer.append("resource: http://localhost:8080/rest/test\n");
            writer.append("dir: /tmp/import-export-dir\n");
            writer.flush();

            final String[] args = new String[]{"-c", configFile.getAbsolutePath()};
            final Config config = parser.parseConfiguration(args);

        });

    }

    @Test
    public void parseDescriptionDirectoryRequired() throws Exception {
        assertThrows(RuntimeException.class, () -> {
            final String[] args = new String[]{"-m", "export", "-r", "http://localhost:8080/rest/1"};
            parser.parse(args);
        });
    }

    @Test
    public void parseResourceRequired() throws Exception {
        assertThrows(RuntimeException.class, () -> {
            final String[] args = new String[]{"-m", "export", "-d", "/tmp/rdf"};
            parser.parse(args);
        });
    }

    @Test
    public void parseInvalid() throws Exception {
        assertThrows(RuntimeException.class, () -> {
            final String[] args = new String[]{"junk"};
            parser.parse(args);
        });
    }

    @Test
    public void parseHelpWithNoOtherArgs() {
        assertThrows(RuntimeException.class, () ->
            parser.parseConfiguration(new String[]{"-h"}));
    }

    @Test
    public void parseHelpWithMinimumValidArgs() {
        assertThrows(RuntimeException.class, () -> {
            final String[] args = ArrayUtils.addAll(MINIMAL_VALID_EXPORT_ARGS, "-h");
            parser.parseConfiguration(args);
        });
    }

    @Test
    public void parseValidUsername() {
        final String[] args = ArrayUtils.addAll(MINIMAL_VALID_EXPORT_ARGS, "-u",  "user:pass");
        final Config config = parser.parseConfiguration(args);
        Assertions.assertEquals("user", config.getUsername());
        Assertions.assertEquals("pass", config.getPassword());
    }

    @Test
    public void parseValidUsernameLong() {
        final String[] args = ArrayUtils.addAll(MINIMAL_VALID_EXPORT_ARGS, "--user", "user:pass");
        final Config config = parser.parseConfiguration(args);
        Assertions.assertEquals("user", config.getUsername());
        Assertions.assertEquals("pass", config.getPassword());
    }

    @Test
    public void parseInvalidUser() {
        assertThrows(RuntimeException.class, () ->
            parser.parseConfiguration(ArrayUtils.addAll(MINIMAL_VALID_EXPORT_ARGS, "--u", "wrong")));
    }

    @Test
    public void testImportMap() {
        final String map = "http://localhost:7777/rest,http://localhost:8888/fcrepo/rest";
        final String[] args = new String[]{"-m", "import",
                                           "-d", "/tmp/rdf",
                                           "-r", "http://localhost:8888/fcrepo/rest/1",
                                           "-M", map};
        final Config config = parser.parseConfiguration(args);
        Assertions.assertEquals("http://localhost:7777/rest", config.getSource().toString());
        Assertions.assertEquals("/rest", config.getSourcePath());
        Assertions.assertEquals("http://localhost:8888/fcrepo/rest", config.getDestination().toString());
        Assertions.assertEquals("/fcrepo/rest", config.getDestinationPath());
    }

    @Test
    public void testImportEmptySource() {
        final String resource = "http://localhost:8080/rest/1";
        final String[] args = new String[]{"-m", "import",
                                           "-d", "/tmp/rdf",
                                           "-r", resource};
        final Config config = parser.parseConfiguration(args);
        Assertions.assertNull(config.getSource());
        Assertions.assertNull(config.getDestination());
    }

    @Test
    public void retrieveConfig() {
        final File configFile = new File("src/test/resources/configs/importexport.yml");
        final File dir = new File("/tmp/rdf");
        final File baseDir = new File(dir, "data");
        final Config config = parser.retrieveConfig(configFile);

        Assertions.assertEquals("http://www.w3.org/ns/ldp#contains", config.getPredicates()[0]);
        Assertions.assertEquals(URI.create("http://localhost:8080/rest/1"), config.getResource());
        Assertions.assertEquals(URI.create("http://localhost:8080/rest/2"), config.getSource());
        Assertions.assertEquals("default", config.getBagProfile());
        Assertions.assertEquals("path/config.yaml", config.getBagConfigPath());
        Assertions.assertEquals(baseDir, config.getBaseDirectory());
        Assertions.assertEquals(dir.getAbsolutePath(), config.getMap().get("dir"));
        Assertions.assertEquals("text/turtle", config.getRdfLanguage());
        Assertions.assertEquals(".ttl", config.getRdfExtension());

        Assertions.assertFalse(config.retrieveExternal());
        Assertions.assertTrue(config.isIncludeBinaries());
        Assertions.assertTrue(config.overwriteTombstones());
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
                                            "-w", "target/serialized-custom.yml",
                                            "-b", "-x", "-i", "-t", "-a", "-V"};
        final Map<String, String> config = parser.parseConfiguration(args).getMap();
        Assertions.assertEquals("import", config.get("mode"));
        Assertions.assertEquals("http://localhost:8686/rest", config.get("resource"));
        Assertions.assertEquals("http://localhost:8686/rest,http://localhost:8080/f4/rest", config.get("map"));
        Assertions.assertEquals(baseDir.getAbsolutePath(), config.get("dir"));
        Assertions.assertEquals("application/ld+json", config.get("rdfLang"));
        Assertions.assertEquals("custom-profile.yml", config.get("bag-profile"));
        Assertions.assertEquals("custom-metadata.yml", config.get("bag-config"));
        Assertions.assertEquals("http://example.org/sample", config.get("predicates"));
        Assertions.assertEquals("true", config.get("binaries"));
        Assertions.assertEquals("true", config.get("external"));
        Assertions.assertEquals("true", config.get("inbound"));
        Assertions.assertEquals("true", config.get("overwriteTombstones"));
        Assertions.assertEquals("true", config.get("auditLog"));
        Assertions.assertEquals("true", config.get("versions"));
        Assertions.assertNull(config.get("writeConfig"));
        Assertions.assertTrue(new File("target/serialized-custom.yml").exists());
    }

    @Test
    public void testSerializedConfigDefault() {
        final File baseDir = new File("/tmp/rdf");
        final String[] args = new String[] {"-m", "import",
                                            "-r", "http://localhost:8080/rest",
                                            "-d", baseDir.getAbsolutePath()};
        final Map<String, String> config = parser.parseConfiguration(args).getMap();
        Assertions.assertEquals("import", config.get("mode"));
        Assertions.assertEquals("http://localhost:8080/rest", config.get("resource"));
        Assertions.assertNull(config.get("map"));
        Assertions.assertEquals(baseDir.getAbsolutePath(), config.get("dir"));
        Assertions.assertEquals("text/turtle", config.get("rdfLang"));
        Assertions.assertNull(config.get("bag-profile"));
        Assertions.assertNull(config.get("bag-config"));
        Assertions.assertEquals("http://www.w3.org/ns/ldp#contains", config.get("predicates"));
        Assertions.assertEquals("false", config.get("binaries"));
        Assertions.assertEquals("false", config.get("external"));
        Assertions.assertEquals("false", config.get("inbound"));
        Assertions.assertEquals("false", config.get("overwriteTombstones"));
        Assertions.assertEquals("false", config.get("auditLog"));
        Assertions.assertEquals("false", config.get("versions"));
        Assertions.assertNull(config.get("writeConfig"));
    }

    @Test
    public void testStreamingImport() {
        assertThrows(RuntimeException.class,
                () -> parser.parseConfiguration(ArrayUtils.addAll(MINIMAL_VALID_IMPORT_ARGS, "--streaming")));
    }

    @Test
    public void testStreamingExportRdfLang() {
        assertThrows(RuntimeException.class, () -> parser.parseConfiguration(
                ArrayUtils.addAll(MINIMAL_VALID_EXPORT_ARGS, "--streaming", "-l", "application/ld+json")));
    }

    /**
     * Test that default RDF language is set to application/n-triples when streaming is enabled and isRdfSet is false
     */
    @Test
    public void testStreamingExport() {
        final Map<String, String> config =
                parser.parseConfiguration(ArrayUtils.addAll(MINIMAL_VALID_EXPORT_ARGS, "--streaming")).getMap();
        Assertions.assertEquals("export", config.get("mode"));
        Assertions.assertEquals("http://localhost:8080/rest/1", config.get("resource"));
        Assertions.assertEquals("/tmp/rdf", config.get("dir"));
        Assertions.assertEquals("application/n-triples", config.get("rdfLang"));
        Assertions.assertEquals("false", config.get("isRdfSet"));
    }

    /**
     * Test that default RDF language is set to application/n-triples when streaming is enabled and isRdfSet is true
     */
    @Test
    public void testStreamingExportSetRdfLang() {
        final Map<String, String> config = parser.parseConfiguration(
                ArrayUtils.addAll(MINIMAL_VALID_EXPORT_ARGS, "--streaming", "-l", "application/n-triples")).getMap();
        Assertions.assertEquals("export", config.get("mode"));
        Assertions.assertEquals("http://localhost:8080/rest/1", config.get("resource"));
        Assertions.assertEquals("/tmp/rdf", config.get("dir"));
        Assertions.assertEquals("application/n-triples", config.get("rdfLang"));
        Assertions.assertEquals("true", config.get("streaming"));
        Assertions.assertEquals("true", config.get("isRdfSet"));
    }

    /**
     * Test that default RDF language is set to application/n-triples when streaming is not enabled and isRdfSet is true
     */
    @Test
    public void testNonStreamingExportRdfSet() {
        final Map<String, String> config = parser.parseConfiguration(
                ArrayUtils.addAll(MINIMAL_VALID_EXPORT_ARGS, "-l", "application/n-triples")).getMap();
        Assertions.assertEquals("export", config.get("mode"));
        Assertions.assertEquals("http://localhost:8080/rest/1", config.get("resource"));
        Assertions.assertEquals("/tmp/rdf", config.get("dir"));
        Assertions.assertEquals("application/n-triples", config.get("rdfLang"));
        Assertions.assertEquals("false", config.get("streaming"));
        Assertions.assertEquals("true", config.get("isRdfSet"));
    }

    /**
     * Test that default RDF language is set to text/turtle when streaming is not enabled and isRdfSet is false
     */
    @Test
    public void testNonStreamingExportRdf() {
        final Map<String, String> config =
                parser.parseConfiguration(ArrayUtils.addAll(MINIMAL_VALID_EXPORT_ARGS)).getMap();
        Assertions.assertEquals("export", config.get("mode"));
        Assertions.assertEquals("http://localhost:8080/rest/1", config.get("resource"));
        Assertions.assertEquals("/tmp/rdf", config.get("dir"));
        Assertions.assertEquals("text/turtle", config.get("rdfLang"));
        Assertions.assertEquals("false", config.get("streaming"));
        Assertions.assertEquals("false", config.get("isRdfSet"));
    }

    @Test
    public void testParseReturnsExporter() {
        final TransferProcess process = parser.parse(MINIMAL_VALID_EXPORT_ARGS);
        Assertions.assertTrue(process instanceof Exporter);
    }

    @Test
    public void testParseReturnsImporter() {
        final TransferProcess process = parser.parse(MINIMAL_VALID_IMPORT_ARGS);
        Assertions.assertTrue(process instanceof Importer);
    }

    @Test
    public void parseLongHelp() {
        assertThrows(RuntimeException.class,
                () -> parser.parseConfiguration(ArrayUtils.addAll(MINIMAL_VALID_EXPORT_ARGS, "--help")));
    }

    @Test
    public void parseInvalidMode() {
        assertThrows(RuntimeException.class, () -> parser.parseConfiguration(
                new String[]{"-m", "sideways", "-d", "/tmp/rdf", "-r", "http://localhost:8080/rest/1"}));
    }

    @Test
    public void parseImportWithoutResource() {
        final RuntimeException e = assertThrows(RuntimeException.class,
                () -> parser.parseConfiguration(new String[]{"-m", "import", "-d", "/tmp/rdf"}));
        Assertions.assertEquals("A resource must be specified when importing", e.getMessage());
    }

    @Test
    public void parseResourceFileWithoutRepositoryRoot() {
        final RuntimeException e = assertThrows(RuntimeException.class, () -> parser.parseConfiguration(
                new String[]{"-m", "export", "-d", "/tmp/rdf", "-f", "/tmp/resources.txt"}));
        Assertions.assertEquals("The repository root must be specified when exporting from a resources file",
                e.getMessage());
    }

    @Test
    public void parseResourceFileAndOptions() {
        final Config config = parser.parseConfiguration(new String[]{"-m", "export", "-d", "/tmp/rdf",
                "-f", "/tmp/resources.txt", "-R", "http://localhost:8080/rest", "-T", "3", "--acls",
                "--membership", "-a", "--skip-tombstones", "--bag-algorithms", "sha1,md5"});
        Assertions.assertEquals(new File("/tmp/resources.txt").toPath(), config.getResourceFile());
        Assertions.assertEquals(URI.create("http://localhost:8080/rest"), config.getRepositoryRoot());
        Assertions.assertEquals(Integer.valueOf(3), config.getThreadCount());
        Assertions.assertTrue(config.isIncludeAcls());
        Assertions.assertTrue(config.includeMembership());
        Assertions.assertTrue(config.isSkipTombstoneErrors());
        Assertions.assertArrayEquals(new String[]{"sha1", "md5"}, config.getBagAlgorithms());
    }

    @Test
    public void parseMapWithSingleValue() {
        assertThrows(RuntimeException.class, () -> parser.parseConfiguration(
                ArrayUtils.addAll(MINIMAL_VALID_IMPORT_ARGS, "-M", "http://localhost:8080/rest")));
    }

    @Test
    public void parseMissingConfigFile() {
        assertThrows(RuntimeException.class,
                () -> parser.parseConfiguration(new String[]{"-c", "/does/not/exist/config.yml"}));
    }

    @Test
    public void parseUnwritableWriteConfig() {
        final RuntimeException e = assertThrows(RuntimeException.class, () -> parser.parseConfiguration(
                ArrayUtils.addAll(MINIMAL_VALID_EXPORT_ARGS, "-w", "/does/not/exist/config.yml")));
        Assertions.assertTrue(e.getMessage().startsWith("Unable to write configuration file"));
    }

    @Test
    public void testConfigFromFileAllKeys() throws ParseException {
        final Map<String, String> vars = new LinkedHashMap<>();
        vars.put("mode", "import");
        vars.put("resource", "http://localhost:8080/rest/1");
        vars.put("map", "http://localhost:8080/rest,http://example.org/rest");
        vars.put("dir", "/tmp/rdf");
        vars.put("rdfLang", "application/ld+json");
        vars.put("binaries", "true");
        vars.put("acls", "true");
        vars.put("external", "true");
        vars.put("inbound", "true");
        vars.put("writeConfig", "/tmp/written.yml");
        vars.put("overwriteTombstones", "true");
        vars.put("legacyMode", "true");
        vars.put("versions", "true");
        vars.put("bag-profile", "DEFAULT");
        vars.put("bag-config", "/tmp/Bag-Config.yml");
        vars.put("bag-algorithms", "sha1,sha256");
        vars.put("bag-serialization", "tar");
        vars.put("predicates", "http://example.org/a,http://example.org/b");
        vars.put("auditLog", "true");
        vars.put("threadCount", "2");
        vars.put("resourceFile", "/tmp/resources.txt");
        vars.put("streaming", "false");
        vars.put("membership", "true");

        final Config config = ArgParser.configFromFile(vars);
        Assertions.assertTrue(config.isImport());
        Assertions.assertEquals(URI.create("http://localhost:8080/rest/1"), config.getResource());
        Assertions.assertEquals(URI.create("http://example.org/rest"), config.getDestination());
        Assertions.assertEquals("application/ld+json", config.getRdfLanguage());
        Assertions.assertTrue(config.isIncludeBinaries());
        Assertions.assertTrue(config.isIncludeAcls());
        Assertions.assertTrue(config.retrieveExternal());
        Assertions.assertTrue(config.retrieveInbound());
        Assertions.assertEquals(new File("/tmp/written.yml"), config.getWriteConfig());
        Assertions.assertTrue(config.overwriteTombstones());
        Assertions.assertTrue(config.isLegacy());
        Assertions.assertTrue(config.includeVersions());
        Assertions.assertEquals("default", config.getBagProfile());
        Assertions.assertEquals("/tmp/bag-config.yml", config.getBagConfigPath());
        Assertions.assertArrayEquals(new String[]{"sha1", "sha256"}, config.getBagAlgorithms());
        Assertions.assertEquals("tar", config.getBagSerialization());
        Assertions.assertArrayEquals(new String[]{"http://example.org/a", "http://example.org/b"},
                config.getPredicates());
        Assertions.assertEquals(Integer.valueOf(2), config.getThreadCount());
        Assertions.assertEquals(new File("/tmp/resources.txt").toPath(), config.getResourceFile());
        Assertions.assertFalse(config.isStreaming());
        Assertions.assertTrue(config.includeMembership());
    }

    @Test
    public void testConfigFromFileInvalidMode() {
        final Map<String, String> vars = new LinkedHashMap<>();
        vars.put("mode", "sideways");
        assertThrows(ParseException.class, () -> ArgParser.configFromFile(vars));
    }

    @Test
    public void testConfigFromFileInvalidMap() {
        final Map<String, String> vars = new LinkedHashMap<>();
        vars.put("map", "http://localhost:8080/rest");
        assertThrows(ParseException.class, () -> ArgParser.configFromFile(vars));
    }
}
