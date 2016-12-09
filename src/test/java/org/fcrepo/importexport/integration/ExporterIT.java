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

import static org.apache.http.HttpStatus.SC_CREATED;
import static org.fcrepo.importexport.ArgParser.DEFAULT_RDF_EXT;
import static org.fcrepo.importexport.ArgParser.DEFAULT_RDF_LANG;
import static org.fcrepo.importexport.common.FcrepoConstants.CONTAINS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.File;
import java.net.URI;
import java.util.UUID;

import org.fcrepo.client.FcrepoOperationFailedException;
import org.fcrepo.client.FcrepoResponse;
import org.fcrepo.importexport.common.Config;
import org.fcrepo.importexport.exporter.Exporter;

import org.junit.Test;
import org.slf4j.Logger;

/**
 * @author awoods
 * @since 2016-09-18
 */
public class ExporterIT extends AbstractResourceIT {

    private URI url;

    public ExporterIT() {
        super();
        url = URI.create(serverAddress + UUID.randomUUID());
    }

    @Test
    public void testExport() throws Exception {
        // Create a repository resource
        final FcrepoResponse response = create(url);
        assertEquals(SC_CREATED, response.getStatusCode());
        assertEquals(url, response.getLocation());

        // Run an export process
        final Config config = new Config();
        config.setMode("export");
        config.setBaseDirectory(TARGET_DIR);
        config.setResource(url);
        config.setRdfLanguage(DEFAULT_RDF_LANG);
        config.setUsername(USERNAME);
        config.setPassword(PASSWORD);
        config.setPredicates(new String[]{ CONTAINS.toString() });

        final Exporter exporter = new Exporter(config, clientBuilder);
        exporter.run();

        // Verify
        assertTrue(new File(TARGET_DIR, url.getPath() + DEFAULT_RDF_EXT).exists());
    }

    @Test
    public void testExportCustomPredicate() throws Exception {
        final String[] predicates = new String[]{ "http://example.org/custom" };
        final UUID uuid = UUID.randomUUID();
        final Config config = exportWithCustomPredicates(predicates, uuid);

        // Verify
        final File baseDir = new File(config.getBaseDirectory(), "/fcrepo/rest/" + uuid);
        assertTrue(new File(baseDir, "/res1" + DEFAULT_RDF_EXT).exists());
        assertFalse(new File(baseDir, "/res1/res2" + DEFAULT_RDF_EXT).exists());
        assertTrue(new File(baseDir, "/res3" + DEFAULT_RDF_EXT).exists());
        assertFalse(new File(baseDir, "/res3/res4" + DEFAULT_RDF_EXT).exists());
    }

    @Test
    public void testExportMultiplePredicates() throws Exception {
        final String[] predicates = new String[]{ CONTAINS.toString(), "http://example.org/custom" };
        final UUID uuid = UUID.randomUUID();
        final Config config = exportWithCustomPredicates(predicates, uuid);

        // Verify
        final File baseDir = new File(config.getBaseDirectory(), "/fcrepo/rest/" + uuid);
        assertTrue(new File(baseDir, "/res1" + DEFAULT_RDF_EXT).exists());
        assertTrue(new File(baseDir, "/res1/res2" + DEFAULT_RDF_EXT).exists());
        assertTrue(new File(baseDir, "/res3" + DEFAULT_RDF_EXT).exists());
        assertTrue(new File(baseDir, "/res3/res4" + DEFAULT_RDF_EXT).exists());
    }

    @Test
    public void testExportDefaultPredicate() throws Exception {
        final String[] predicates = new String[]{ CONTAINS.toString() };
        final UUID uuid = UUID.randomUUID();
        final Config config = exportWithCustomPredicates(predicates, uuid);

        // Verify
        final File baseDir = new File(config.getBaseDirectory(), "/fcrepo/rest/" + uuid);
        assertFalse(new File(baseDir, "/res1" + DEFAULT_RDF_EXT).exists());
        assertFalse(new File(baseDir, "/res1/res2" + DEFAULT_RDF_EXT).exists());
        assertTrue(new File(baseDir, "/res3" + DEFAULT_RDF_EXT).exists());
        assertTrue(new File(baseDir, "/res3/res4" + DEFAULT_RDF_EXT).exists());
    }

    private Config exportWithCustomPredicates(final String[] predicates, final UUID uuid)
            throws FcrepoOperationFailedException {
        final String baseURI = serverAddress + uuid;
        final URI res1 = URI.create(baseURI + "/res1");
        final URI res2 = URI.create(baseURI + "/res1/res2");
        final URI res3 = URI.create(baseURI + "/res3");
        final URI res4 = URI.create(baseURI + "/res3/res4");

        create(res1);
        create(res2);
        createTurtle(res3, "<> <http://example.org/custom> <" + res1.toString() + "> .");
        create(res4);

        // export with custom predicates
        final Config config = new Config();
        config.setMode("export");
        config.setBaseDirectory(TARGET_DIR + "/" + uuid);
        config.setResource(res3);
        config.setRdfExtension(DEFAULT_RDF_EXT);
        config.setRdfLanguage(DEFAULT_RDF_LANG);
        config.setUsername(USERNAME);
        config.setPassword(PASSWORD);
        config.setPredicates(predicates);

        new Exporter(config, clientBuilder).run();
        return config;
    }

    @Override
    protected Logger logger() {
        return getLogger(ExporterIT.class);
    }

}
