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
import org.fcrepo.client.FcrepoOperationFailedException;
import org.fcrepo.importexport.common.Config;
import org.fcrepo.importexport.exporter.Exporter;
import org.fcrepo.importexport.importer.Importer;
import org.junit.Test;
import org.slf4j.Logger;

import java.io.File;
import java.net.URI;
import java.util.UUID;

import org.apache.jena.rdf.model.Model;

import static org.apache.http.HttpStatus.SC_CREATED;
import static org.fcrepo.importexport.ArgParser.DEFAULT_RDF_EXT;
import static org.fcrepo.importexport.ArgParser.DEFAULT_RDF_LANG;
import static org.fcrepo.importexport.common.FcrepoConstants.CONTAINER;
import static org.fcrepo.importexport.common.FcrepoConstants.RDF_TYPE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author escowles
 * @since 2016-12-05
 */
public class RoundtripIT extends AbstractResourceIT {


    public RoundtripIT() {
        super();
    }

    @Test
    public void testRoundtripMinimal() throws Exception {
        final URI source = URI.create(serverAddress + UUID.randomUUID());
        final FcrepoResponse response = create(source);
        assertEquals(SC_CREATED, response.getStatusCode());
        assertEquals(source, response.getLocation());
        create(URI.create(source.toString() + "/res1"));

        final URI destination = roundtrip(source);

        final Model model = getAsModel(URI.create(destination.toString() + "/res1"));
        assertTrue(model.contains(null, RDF_TYPE, CONTAINER));
    }

    private URI roundtrip(final URI source) throws FcrepoOperationFailedException {
        final String uuid = UUID.randomUUID().toString();

        // setup config for export, then export resources
        final Config config = new Config();
        config.setMode("export");
        config.setDescriptionDirectory(TARGET_DIR + File.separator + uuid);
        config.setResource(source);
        config.setRdfExtension(DEFAULT_RDF_EXT);
        config.setRdfLanguage(DEFAULT_RDF_LANG);
        config.setUsername(USERNAME);
        config.setPassword(PASSWORD);
        new Exporter(config, clientBuilder).run();

        // setup config for import to a new base URL, then perform import
        final URI destination = URI.create(serverAddress + uuid);
        create(destination);
        config.setMode("import");
        config.setSource(source);
        config.setResource(destination);
        new Importer(config, clientBuilder).run();

        // return the newly-created destination
        return destination;
    }

    protected Logger logger() {
        return getLogger(RoundtripIT.class);
    }
}
