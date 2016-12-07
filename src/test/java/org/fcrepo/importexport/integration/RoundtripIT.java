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

        final URI destination = URI.create(serverAddress + UUID.randomUUID());
        roundtrip(source, destination);

        final Model model = getAsModel(URI.create(destination.toString() + "/res1"));
        assertTrue(model.contains(null, RDF_TYPE, CONTAINER));
    }

    @Test
    public void testRoundtripLinked() throws Exception {
        final String baseURI = serverAddress + UUID.randomUUID();
        final URI src1 = URI.create(baseURI + "/res1");
        final URI src2 = URI.create(baseURI + "/res2");
        create(src1);

        final String turtle = "<> <" + DC_TITLE + "> \"metadata test\" ; "
            + "<" + DC_RELATION + "> <" + src1.toString() + "> ; "
            + "<" + DC_DATE + "> <#date1> . "
            + "<#date1> a <" + EDM_TIMESPAN + "> ; "
            + "<" + SKOS_PREFLABEL + "> \"The last 20 seconds of 2013\" ; "
            + "<" + EDM_BEGIN + "> \"2013-12-31T23:59:39Z\"^^" + XSD_DATETIME + " ; "
            + "<" + EDM_END + "> \"2013-12-31T23:59:59Z\"^^" + XSD_DATETIME + " . ";
        createTurtle(src2, turtle);

        final String dstURI = serverAddress + UUID.randomUUID();
        final URI dst1 = URI.create(dstURI + "/res1");
        final URI dst2 = URI.create(dstURI + "/res2");
        roundtrip(src2, dst2);

        assertTrue(exists(dst1));
        final Model model = getAsModel(dst2);
        assertTrue(model.contains(null, RDF_TYPE, CONTAINER));
        assertTrue(model.contains(null, RDF_TYPE, CONTAINER));
    }

    private void roundtrip(final URI source, final URI destination) throws FcrepoOperationFailedException {
        // setup config for export, then export resources
        final Config config = new Config();
        config.setMode("export");
        config.setBaseDirectory(TARGET_DIR + File.separator + UUID.randomUUID());
        config.setResource(source);
        config.setRdfExtension(DEFAULT_RDF_EXT);
        config.setRdfLanguage(DEFAULT_RDF_LANG);
        config.setUsername(USERNAME);
        config.setPassword(PASSWORD);
        new Exporter(config, clientBuilder).run();

        // setup config for import to a new base URL, then perform import
        create(destination);
        config.setMode("import");
        config.setSource(source);
        config.setResource(destination);
        new Importer(config, clientBuilder).run();
    }

    protected Logger logger() {
        return getLogger(RoundtripIT.class);
    }
}
