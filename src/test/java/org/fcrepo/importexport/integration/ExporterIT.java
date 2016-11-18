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

import org.fcrepo.client.FcrepoResponse;
import org.fcrepo.importexport.ArgParser;
import org.fcrepo.importexport.common.Config;
import org.fcrepo.importexport.exporter.Exporter;
import org.junit.Test;
import org.slf4j.Logger;

import java.io.File;
import java.net.URI;
import java.util.UUID;

import static org.apache.http.HttpStatus.SC_CREATED;
import static org.fcrepo.importexport.ArgParser.DEFAULT_RDF_LANG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.slf4j.LoggerFactory.getLogger;

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
        config.setDescriptionDirectory(TARGET_DIR);
        config.setResource(url);
        config.setRdfExtension(ArgParser.getDefaultRdfExtension());
        config.setRdfLanguage(DEFAULT_RDF_LANG);
        config.setUsername(USERNAME);
        config.setPassword(PASSWORD);

        final Exporter exporter = new Exporter(config, clientBuilder);
        exporter.run();

        // Verify
        assertTrue(new File(TARGET_DIR, url.getPath() + ArgParser.getDefaultRdfExtension()).exists());
    }

    protected Logger logger() {
        return getLogger(ExporterIT.class);
    }

}
