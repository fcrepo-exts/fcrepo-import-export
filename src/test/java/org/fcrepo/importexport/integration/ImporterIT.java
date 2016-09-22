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

import static org.apache.jena.rdf.model.ModelFactory.createDefaultModel;
import static org.apache.jena.rdf.model.ResourceFactory.createResource;

import org.apache.commons.io.IOUtils;
import org.apache.jena.rdf.model.Model;
import org.fcrepo.client.FcrepoClient;
import org.fcrepo.client.FcrepoOperationFailedException;
import org.fcrepo.client.FcrepoResponse;
import org.fcrepo.importexport.common.Config;
import org.fcrepo.importexport.exporter.Exporter;
import org.fcrepo.importexport.importer.Importer;
import org.junit.Test;
import org.slf4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.UUID;

import static org.apache.http.HttpStatus.SC_CREATED;
import static org.apache.http.HttpStatus.SC_GONE;
import static org.apache.http.HttpStatus.SC_NO_CONTENT;
import static org.fcrepo.importexport.ArgParser.DEFAULT_RDF_EXT;
import static org.fcrepo.importexport.ArgParser.DEFAULT_RDF_LANG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author awoods
 * @since 2016-09-18
 */
public class ImporterIT extends AbstractResourceIT {

    private FcrepoClient client;

    public ImporterIT() {
        super();
        client = clientBuilder.build();
    }

    @Test
    public void testImport() throws FcrepoOperationFailedException, IOException {
        final String exportPath = TARGET_DIR + "/" + UUID.randomUUID() + "/testPeartreeAmbiguity";
        final String parentTitle = "parent";
        final String childTitle = "child";
        final String binaryText = "binary";

        final URI parent = URI.create(serverAddress + UUID.randomUUID());
        final URI child = URI.create(parent.toString() + "/child");
        final URI binary = URI.create(child + "/binary");
        assertEquals(SC_CREATED, create(parent).getStatusCode());
        assertEquals(SC_CREATED, create(child).getStatusCode());
        assertEquals(SC_NO_CONTENT, client.patch(parent).body(insertTitle(parentTitle)).perform().getStatusCode());
        assertEquals(SC_NO_CONTENT, client.patch(child).body(insertTitle(childTitle)).perform().getStatusCode());
        assertEquals(SC_CREATED, client.put(binary).body(new ByteArrayInputStream(binaryText.getBytes("UTF-8")),
                "text/plain").perform().getStatusCode());

        // Run an export process
        final Config config = new Config();
        config.setMode("export");
        config.setDescriptionDirectory(exportPath);
        config.setBinaryDirectory(exportPath);
        config.setRdfExtension(DEFAULT_RDF_EXT);
        config.setRdfLanguage(DEFAULT_RDF_LANG);
        config.setResource(parent.toString());
        config.setUsername(USERNAME);
        config.setPassword(PASSWORD);

        final Exporter exporter = new Exporter(config, clientBuilder);
        exporter.run();

        // Verify
        resourceExists(parent);

        // Remove the resources
        client.delete(parent).perform();
        final FcrepoResponse getResponse = client.get(parent).perform();
        assertEquals("Resource should have been deleted!", SC_GONE, getResponse.getStatusCode());
        assertEquals("Failed to delete the tombstone!", SC_NO_CONTENT,
                client.delete(getResponse.getLinkHeaders("hasTombstone").get(0)).perform().getStatusCode());

        // Run the import process
        config.setMode("import");

        final Importer importer = new Importer(config, clientBuilder);
        importer.run();

        // Verify
        assertHasTitle(parent, parentTitle);
        assertHasTitle(child, childTitle);
        assertEquals("Binary should have been imported!",
                binaryText, IOUtils.toString(client.get(binary).perform().getBody(), "UTF-8"));
    }

    @Test
    public void testReferences() throws Exception {
        final URI sourceURI = URI.create("http://localhost:8080/fcrepo/rest");
        final URI linkFrom = URI.create(serverAddress + "linkFrom");
        final URI linkTo = URI.create(serverAddress + "linkTo");
        final String referencePath = TARGET_DIR + "/test-classes/sample/reference";
        System.out.println("Importing from " + referencePath);

        final Config config = new Config();
        config.setMode("import");
        config.setBinaryDirectory(referencePath);
        config.setDescriptionDirectory(referencePath);
        config.setRdfExtension(DEFAULT_RDF_EXT);
        config.setRdfLanguage(DEFAULT_RDF_LANG);
        config.setResource(serverAddress);
        config.setSource(sourceURI.toString());
        config.setUsername(USERNAME);
        config.setPassword(PASSWORD);

        // run import
        final Importer importer = new Importer(config, clientBuilder);
        importer.run();

        // verify the resources exist and link to each other
        assertTrue(resourceExists(linkFrom));
        assertTrue(resourceExists(linkTo));
        assertTrue(resourceLinksTo(linkTo, linkFrom));
        assertTrue(resourceLinksTo(linkFrom, linkTo));
    }

    @Test
    public void testImportContainer() throws Exception {
        // import test-indirect

        final URI sourceURI = URI.create("http://localhost:8080/fcrepo/rest");
        final URI parentURI = URI.create(serverAddress + "indirect/1");
        final URI memberURI = URI.create(serverAddress + "indirect/2");
        final String indirectPath = TARGET_DIR + "/test-classes/sample/indirect";

        final Config config = new Config();
        config.setMode("import");
        config.setDescriptionDirectory(indirectPath);
        config.setRdfExtension(DEFAULT_RDF_EXT);
        config.setRdfLanguage(DEFAULT_RDF_LANG);
        config.setResource(serverAddress);
        config.setSource(sourceURI.toString());
        config.setUsername(USERNAME);
        config.setPassword(PASSWORD);

        // run import
        final Importer importer = new Importer(config, clientBuilder);
        importer.run();

        // verify one title and one hasMember
        final FcrepoResponse response = clientBuilder.build().get(parentURI).accept("text/plain").perform();
        final String triples = IOUtils.toString(response.getBody());
        final String memberTriple = "<" + parentURI + "> <http://pcdm.org/models#hasMember> <" + memberURI + "> .";
        final String titleTriple = "<" + parentURI + "> <http://purl.org/dc/terms/title> "
                + "\"foo\"^^<http://www.w3.org/2001/XMLSchema#string> .";
        assertEquals(1, count(triples, titleTriple));
        assertEquals(1, count(triples, memberTriple));
    }

    private int count(final String triples, final String triple) {
        int count = 0;
        final String[] arr = triples.split("\\n");
        for (int i = 0; i < arr.length; i++) {
            if (triple.equals(arr[i])) {
                count++;
            }
        }
        return count;
    }

    private boolean resourceExists(final URI uri) throws FcrepoOperationFailedException {
        final FcrepoResponse response = clientBuilder.build().head(uri).perform();
        return response.getStatusCode() == 200;
    }

    private boolean resourceLinksTo(final URI linkFrom, final URI linkTo) throws FcrepoOperationFailedException {
        final FcrepoResponse response = clientBuilder.build().get(linkFrom).perform();
        final Model model = createDefaultModel().read(response.getBody(), null, "text/turtle");
        return model.contains(createResource(linkFrom.toString()), null, createResource(linkTo.toString()));
    }

    protected Logger logger() {
        return getLogger(ImporterIT.class);
    }

}
