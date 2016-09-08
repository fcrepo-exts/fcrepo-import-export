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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static java.lang.System.getProperty;
import static org.fcrepo.importexport.ArgParser.CONFIG_FILE_NAME;

/**
 * @author awoods
 * @since 2016-08-29
 */
public class ArgParserTest {

    private ArgParser parser;

    @Before
    public void setUp() throws Exception {
        parser = new ArgParser();
    }

    @Test
    public void parseValidExport() throws Exception {
        final String[] args = new String[]{"-m", "export",
                                           "-d", "/tmp/rdf",
                                           "-b", "/tmp/bin",
                                           "-x", ".jsonld",
                                           "-l", "application/ld+json",
                                           "-r", "http://localhost:8080/rest/1"};
        final Config config = parser.parseConfiguration(args);
        Assert.assertTrue(config.isExport());
        Assert.assertEquals(new File("/tmp/rdf"), config.getDescriptionDirectory());
        Assert.assertEquals(new File("/tmp/bin"), config.getBinaryDirectory());
        Assert.assertEquals(".jsonld", config.getRdfExtension());
        Assert.assertEquals("application/ld+json", config.getRdfLanguage());
        Assert.assertEquals(new URI("http://localhost:8080/rest/1"), config.getResource());
    }

    @Test
    public void parseMinimalValidExport() throws Exception {
        final String[] args = new String[]{"-m", "export",
                                           "-d", "/tmp/rdf",
                                           "-r", "http://localhost:8080/rest/1"};
        final Config config = parser.parseConfiguration(args);
        Assert.assertTrue(config.isExport());
        Assert.assertEquals(new File("/tmp/rdf"), config.getDescriptionDirectory());
        Assert.assertNull(config.getBinaryDirectory());
        Assert.assertEquals(".ttl", config.getRdfExtension());
        Assert.assertEquals("text/turtle", config.getRdfLanguage());
        Assert.assertEquals(new URI("http://localhost:8080/rest/1"), config.getResource());
    }

    @Test
    public void parseConfigFile() throws IOException {
        // Create test config file
        final File configFile = File.createTempFile("config-test", ".txt");
        final FileWriter writer = new FileWriter(configFile);
        writer.append("-d\n");
        writer.append("/tmp/desc\n");
        writer.append("-m\n");
        writer.append("export\n");
        writer.append("-r\n");
        writer.append("http://localhost:8080/rest/test\n");
        writer.append("-b\n");
        writer.append("/tmp/bin\n");
        writer.flush();

        final String[] args = new String[]{"-c", configFile.getAbsolutePath()};
        final Config config = parser.parseConfiguration(args);
        Assert.assertTrue(config.isExport());
        Assert.assertEquals(new File("/tmp/desc"), config.getDescriptionDirectory());
        Assert.assertEquals(new File("/tmp/bin"), config.getBinaryDirectory());
        Assert.assertEquals(".ttl", config.getRdfExtension());
        Assert.assertEquals("text/turtle", config.getRdfLanguage());
        Assert.assertEquals(URI.create("http://localhost:8080/rest/test"), config.getResource());

        Assert.assertTrue("Config file should be created!",
                new File(getProperty("java.io.tmpdir"), CONFIG_FILE_NAME).exists());
    }

    @Test (expected = RuntimeException.class)
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
}
