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

import org.fcrepo.importexport.common.Config;

import org.apache.commons.lang3.ArrayUtils;
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
        parser.parseConfiguration(ArrayUtils.addAll(MINIMAL_VALID_EXPORT_ARGS, new String[] {
            "-l", "invalid/language" }));
    }

    @Test
    public void parseBagProfile() throws Exception {
        final Config config = parser.parseConfiguration(ArrayUtils.addAll(MINIMAL_VALID_EXPORT_ARGS,
            new String[] {"-g", "default", "-G", "path/config.yaml" }));
        Assert.assertEquals("default", config.getBagProfile());
        Assert.assertEquals(new File("/tmp/rdf/data"), config.getBaseDirectory());
        Assert.assertEquals("path/config.yaml", config.getBagConfigPath());
    }

    @Test(expected = RuntimeException.class)
    public void parseBagProfileWithNoConfigSpecified() throws Exception {
        parser.parseConfiguration(ArrayUtils.addAll(MINIMAL_VALID_EXPORT_ARGS,
            new String[] {"-g", "default"}));
    }

    @Test(expected = RuntimeException.class)
    public void parseBagConfigWithNoProfileSpecified() throws Exception {
        parser.parseConfiguration(ArrayUtils.addAll(MINIMAL_VALID_EXPORT_ARGS,
            new String[] {"-G", "/path/to/bag-config.yaml"}));
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
        final String[] args = ArrayUtils.addAll(new String[] { "-h" });
        parser.parseConfiguration(args);
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
    public void testImportSource() {
        final String source = "http://localhost:8080/rest/2";
        final String[] args = new String[]{"-m", "import",
                                           "-d", "/tmp/rdf",
                                           "-r", "http://localhost:8080/rest/1",
                                           "-s", source};
        final Config config = parser.parseConfiguration(args);
        Assert.assertEquals(source, config.getSource().toString());
    }

    @Test
    public void testImportEmptySource() {
        final String resource = "http://localhost:8080/rest/1";
        final String[] args = new String[]{"-m", "import",
                                           "-d", "/tmp/rdf",
                                           "-r", resource};
        final Config config = parser.parseConfiguration(args);
        Assert.assertEquals(resource, config.getSource().toString());
    }
}
