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

import java.io.File;
import java.io.FileInputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.apache.commons.io.IOUtils;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
import org.fcrepo.client.FcrepoClient;
import org.fcrepo.client.FcrepoResponse;
import org.fcrepo.client.FcrepoOperationFailedException;
import org.fcrepo.importexport.common.Config;
import org.fcrepo.importexport.exporter.Exporter;
import org.fcrepo.importexport.importer.Importer;
import org.junit.Test;
import org.slf4j.Logger;

import static org.apache.http.HttpStatus.SC_CREATED;
import static org.apache.jena.datatypes.xsd.XSDDatatype.XSDdateTime;
import static org.apache.jena.datatypes.xsd.XSDDatatype.XSDlong;
import static org.apache.jena.rdf.model.ResourceFactory.createLangLiteral;
import static org.apache.jena.rdf.model.ResourceFactory.createResource;
import static org.apache.jena.rdf.model.ResourceFactory.createProperty;
import static org.apache.jena.rdf.model.ResourceFactory.createTypedLiteral;
import static org.apache.jena.riot.RDFDataMgr.loadModel;
import static org.fcrepo.importexport.common.Config.DEFAULT_RDF_EXT;
import static org.fcrepo.importexport.common.Config.DEFAULT_RDF_LANG;
import static org.fcrepo.importexport.common.FcrepoConstants.CONTAINER;
import static org.fcrepo.importexport.common.FcrepoConstants.CONTAINS;
import static org.fcrepo.importexport.common.FcrepoConstants.NON_RDF_SOURCE;
import static org.fcrepo.importexport.common.FcrepoConstants.RDF_TYPE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author escowles
 * @since 2016-12-07
 */
public class RoundtripIT extends AbstractResourceIT {

    private FcrepoClient client;

    public RoundtripIT() {
        super();
        client = clientBuilder.build();
    }

    @Test
    public void testRoundtripMinimal() throws Exception {
        final URI uri = URI.create(serverAddress + UUID.randomUUID());
        final FcrepoResponse response = create(uri);
        assertEquals(SC_CREATED, response.getStatusCode());
        assertEquals(uri, response.getLocation());
        create(URI.create(uri.toString() + "/res1"));

        roundtrip(uri, true);

        final Model model = getAsModel(URI.create(uri.toString() + "/res1"));
        assertTrue(model.contains(null, RDF_TYPE, CONTAINER));
    }

    @Test
    public void testRoundtripMetadata() throws Exception {
        final UUID uuid = UUID.randomUUID();
        final String baseURI = serverAddress + uuid;
        final URI res1 = URI.create(baseURI + "/res1");
        final URI res2 = URI.create(baseURI + "/res2");

        final Resource subject = createResource(res2.toString());
        final Resource date1 = createResource(res2.toString() + "#date1");

        create(res1);

        final String turtle = "<> a <" + PCDM_OBJECT + "> ; "
            + "<" + DC_TITLE + "> \"metadata test\" ; "
            + "<" + DC_TITLE + "> \"metadata test\"@en ; "
            + "<" + DC_RELATION + "> <" + res1.toString() + "> ; "
            + "<" + DC_DATE + "> <#date1> . "
            + "<#date1> a <" + EDM_TIMESPAN + "> ; "
            + "<" + SKOS_PREFLABEL + "> \"The last 20 seconds of 2013\" ; "
            + "<" + EDM_BEGIN + "> \"2013-12-31T23:59:39Z\"^^<" + XSD_DATETIME + "> ; "
            + "<" + EDM_END + "> \"2013-12-31T23:59:59Z\"^^<" + XSD_DATETIME + "> . ";
        createTurtle(res2, turtle);

        final Config config = roundtrip(URI.create(baseURI), true);

        // verify that files exist and contain expected content
        final File exportDir = config.getBaseDirectory();
        final File res2File = new File(exportDir, "fcrepo/rest/" + uuid + "/res2" + config.getRdfExtension());

        assertTrue(res2File.exists() && res2File.isFile());
        final Model res2Model = loadModel(res2File.getAbsolutePath());
        assertTrue(res2Model.contains(subject, RDF_TYPE, CONTAINER));
        assertTrue(res2Model.contains(subject, RDF_TYPE, createResource(PCDM_OBJECT)));
        assertTrue(res2Model.contains(subject, createProperty(DC_TITLE), "metadata test"));
        assertTrue(res2Model.contains(subject, createProperty(DC_TITLE), createLangLiteral("metadata test","en")));
        assertTrue(res2Model.contains(subject, createProperty(DC_RELATION), createResource(res1.toString())));
        assertTrue(res2Model.contains(subject, createProperty(DC_DATE), date1));
        assertTrue(res2Model.contains(date1, RDF_TYPE, createResource(EDM_TIMESPAN)));
        assertTrue(res2Model.contains(date1, createProperty(EDM_BEGIN), dateLiteral("2013-12-31T23:59:39Z")));
        assertTrue(res2Model.contains(date1, createProperty(EDM_END), dateLiteral("2013-12-31T23:59:59Z")));

        // verify metadata in the repository
        assertTrue(exists(res1));
        final Model model = getAsModel(res2);
        assertTrue(model.contains(subject, RDF_TYPE, CONTAINER));
        assertTrue(model.contains(subject, RDF_TYPE, createResource(PCDM_OBJECT)));
        assertTrue(model.contains(subject, createProperty(DC_TITLE), "metadata test"));
        assertTrue(model.contains(subject, createProperty(DC_TITLE), createLangLiteral("metadata test","en")));
        assertTrue(model.contains(subject, createProperty(DC_RELATION), createResource(res1.toString())));
        assertTrue(model.contains(subject, createProperty(DC_DATE), date1));
        assertTrue(model.contains(date1, RDF_TYPE, createResource(EDM_TIMESPAN)));
        assertTrue(model.contains(date1, createProperty(EDM_BEGIN), dateLiteral("2013-12-31T23:59:39Z")));
        assertTrue(model.contains(date1, createProperty(EDM_END), dateLiteral("2013-12-31T23:59:59Z")));
    }

    @Test
    public void testRoundtripDirectContainer() throws Exception {
        final String baseURI = serverAddress + UUID.randomUUID();
        final URI res1 = URI.create(baseURI + "/res1");
        final URI parts = URI.create(baseURI + "/res1/parts");
        final URI part1 = URI.create(baseURI + "/res1/parts/part1");


        final String partsTurtle = "<> a <" + LDP_DIRECT_CONTAINER + "> ; "
            + "<" + LDP_HAS_MEMBER_RELATION + "> <" + DCTERMS_HAS_PART + "> ; "
            + "<" + LDP_MEMBERSHIP_RESOURCE + "> <" +  res1.toString() + "> .";

        create(res1);
        createTurtle(parts, partsTurtle);
        create(part1);

        roundtrip(URI.create(baseURI), true);

        final Resource parent = createResource(res1.toString());
        final Resource container = createResource(parts.toString());
        final Resource member = createResource(part1.toString());

        final Model model1 = getAsModel(res1);
        assertTrue(model1.contains(parent, createProperty(DCTERMS_HAS_PART), member));

        final Model model2 = getAsModel(parts);
        assertTrue(model2.contains(container, RDF_TYPE, createResource(LDP_DIRECT_CONTAINER)));
        assertTrue(model2.contains(container, createProperty(LDP_HAS_MEMBER_RELATION),
                createResource(DCTERMS_HAS_PART)));
        assertTrue(model2.contains(container, createProperty(LDP_MEMBERSHIP_RESOURCE), parent));

        // make sure membership triples were generated by the container
        client.delete(part1).perform();
        final Model model3 = getAsModel(res1);
        assertFalse(model3.contains(parent, createProperty(DCTERMS_HAS_PART), member));
    }

    @Test
    public void testRoundtripIndirectContainer() throws Exception {
        final String baseURI = serverAddress + UUID.randomUUID();
        final URI res1 = URI.create(baseURI + "/res1");
        final URI res2 = URI.create(baseURI + "/res2");
        final URI parts = URI.create(baseURI + "/res2/parts");
        final URI proxy = URI.create(baseURI + "/res2/parts/proxy1");

        final String partsTurtle = "<> a <" + LDP_INDIRECT_CONTAINER + "> ; "
            + "<" + LDP_HAS_MEMBER_RELATION + "> <" + DCTERMS_HAS_PART + "> ; "
            + "<" + LDP_MEMBERSHIP_RESOURCE + "> <" +  res2.toString() + "> ; "
            + "<" + LDP_INSERTED_CONTENT_RELATION + "> <" + ORE_PROXY_FOR + "> .";

        final String proxyTurtle = "<> a <" + ORE_PROXY + "> ; "
            + "<" + ORE_PROXY_FOR + "> <" + res1.toString() + "> . ";

        create(res1);
        create(res2);
        createTurtle(parts, partsTurtle);
        createTurtle(proxy, proxyTurtle);

        roundtrip(URI.create(baseURI), true);

        final Resource member = createResource(res1.toString());
        final Resource parent = createResource(res2.toString());
        final Resource container = createResource(parts.toString());

        final Model model1 = getAsModel(res2);
        assertTrue(model1.contains(parent, createProperty(DCTERMS_HAS_PART), member));

        final Model model2 = getAsModel(parts);
        assertTrue(model2.contains(container, RDF_TYPE, createResource(LDP_INDIRECT_CONTAINER)));
        assertTrue(model2.contains(container, createProperty(LDP_HAS_MEMBER_RELATION),
                createResource(DCTERMS_HAS_PART)));
        assertTrue(model2.contains(container, createProperty(LDP_MEMBERSHIP_RESOURCE), parent));
        assertTrue(model2.contains(container, createProperty(LDP_INSERTED_CONTENT_RELATION),
                createResource(ORE_PROXY_FOR)));

        // make sure membership triples were generated by the container
        client.delete(proxy).perform();
        final Model model3 = getAsModel(res2);
        assertFalse(model3.contains(parent, createProperty(DCTERMS_HAS_PART), member));
    }

    @Test
    public void testRoundtripBinaries() throws Exception {
        final UUID uuid = UUID.randomUUID();
        final String baseURI = serverAddress + uuid;
        final URI res1 = URI.create(baseURI);
        final URI file1 = URI.create(baseURI + "/file1");

        final Resource container = createResource(res1.toString());
        final Resource binary = createResource(file1.toString());

        final String file1patch = "insert data { "
            + "<" + file1.toString() + "> <" + SKOS_PREFLABEL + "> \"original version\" . }";

        create(res1);
        final FcrepoResponse resp = createBody(file1, "this is some content", "text/plain");
        final URI file1desc = resp.getLinkHeaders("describedby").get(0);
        patch(file1desc, file1patch);

        final Config config = roundtrip(URI.create(baseURI), true);

        // verify that files exist and contain expected content
        final File exportDir = config.getBaseDirectory();
        final File containerFile = new File(exportDir, "fcrepo/rest/" + uuid + config.getRdfExtension());
        final File binaryFile = new File(exportDir, "fcrepo/rest/" + uuid + "/file1.binary");
        final File descFile = new File(exportDir, "fcrepo/rest/" + uuid + "/file1/fcr%3Ametadata"
                + config.getRdfExtension());

        assertTrue(containerFile.exists() && containerFile.isFile());
        final Model contModel = loadModel(containerFile.getAbsolutePath());
        assertTrue(contModel.contains(container, RDF_TYPE, CONTAINER));

        assertTrue(binaryFile.exists() && binaryFile.isFile());
        assertEquals("this is some content", IOUtils.toString(new FileInputStream(binaryFile)));
        assertEquals(20L, binaryFile.length());

        assertTrue(descFile.exists() && descFile.isFile());
        final Model descModel = loadModel(descFile.getAbsolutePath());
        assertTrue(descModel.contains(binary, RDF_TYPE, NON_RDF_SOURCE));
        assertTrue(descModel.contains(binary, createProperty(PREMIS_SIZE), longLiteral("20")));

        // verify that the resources exist in the repository
        assertTrue(exists(res1));
        assertTrue(exists(file1));

        final Model model = getAsModel(file1desc);
        assertTrue(model.contains(binary, RDF_TYPE, createResource(LDP_NON_RDF_SOURCE)));
        assertTrue(model.contains(binary, createProperty(SKOS_PREFLABEL), "original version"));
        assertTrue(model.contains(binary, createProperty(PREMIS_SIZE), longLiteral("20")));
        assertTrue(model.contains(binary, createProperty(PREMIS_DIGEST),
                createResource("urn:sha1:5ec1a3cb71c75c52cf23934b137985bd2499bd85")));
    }

    @Test
    public void testRoundtripExcludedBinaries() throws Exception {
        final UUID uuid = UUID.randomUUID();
        final UUID uuidBinary = UUID.randomUUID();
        final String baseURI = serverAddress + uuid;
        final URI res1 = URI.create(baseURI);
        final URI file1 = URI.create(serverAddress + uuidBinary);

        final Resource container = createResource(res1.toString());

        final String file1patch = "insert data { "
                + "<" + file1.toString() + "> <" + SKOS_PREFLABEL + "> \"original version\" . }";

        final String fileMemberPatch = "insert data { "
                + "<" + res1.toString() + "> <" + PCDM_HAS_MEMBER + "> <" + file1.toString() + "> . }";

        create(res1);
        final FcrepoResponse resp = createBody(file1, "this is some content", "text/plain");
        final URI file1desc = resp.getLinkHeaders("describedby").get(0);
        patch(file1desc, file1patch);
        patch(res1, fileMemberPatch);

        assertTrue(exists(res1));
        assertTrue(exists(file1));

        final Config config = roundtrip(res1, Collections.singletonList(file1), true, false);

        // verify that files exist and contain expected content
        final File exportDir = config.getBaseDirectory();
        final File containerFile = new File(exportDir, "fcrepo/rest/" + uuid + config.getRdfExtension());
        final File binaryFile = new File(exportDir, "fcrepo/rest/" + uuidBinary + "/file1.binary");
        final File descFile = new File(exportDir, "fcrepo/rest/" + uuidBinary + "/file1/fcr%3Ametadata"
                + config.getRdfExtension());

        assertTrue(containerFile.exists() && containerFile.isFile());
        final Model contModel = loadModel(containerFile.getAbsolutePath());

        assertTrue(contModel.contains(container, RDF_TYPE, CONTAINER));

        // verify that the files are not exported on disk
        assertFalse(binaryFile.exists());
        assertFalse(descFile.exists());

        // verify that the resource exists in the repository
        assertTrue(exists(res1));
        // verify that the file doesn't exist in the repository
        assertFalse(exists(file1));
    }

    @Test
    public void testRoundtripExternal() throws Exception {
        final UUID uuid = UUID.randomUUID();
        final String baseURI = serverAddress + uuid;
        final URI res1 = URI.create(baseURI);
        final URI file1 = URI.create(baseURI + "/file1");

        final Resource container = createResource(res1.toString());
        final Resource binary = createResource(file1.toString());

        final String file1patch = "insert data { "
            + "<" + file1.toString() + "> <" + SKOS_PREFLABEL + "> \"original version\" . }";

        create(res1);
        final URI externalURI = URI.create("http://www.example.com/file1");
        final String externalContent = "message/external-body;access-type=URL;url=\"" + externalURI + "\"";
        final FcrepoResponse resp = createBody(file1, "", externalContent);
        final URI file1desc = resp.getLinkHeaders("describedby").get(0);
        patch(file1desc, file1patch);

        final Config config = roundtrip(URI.create(baseURI), true);

        // verify that files exist and contain expected content
        final File exportDir = config.getBaseDirectory();
        final File containerFile = new File(exportDir, "fcrepo/rest/" + uuid + config.getRdfExtension());
        final File binaryFile = new File(exportDir, "fcrepo/rest/" + uuid + "/file1.external");
        final File descFile = new File(exportDir, "fcrepo/rest/" + uuid + "/file1/fcr%3Ametadata"
                + config.getRdfExtension());

        assertTrue(containerFile.exists() && containerFile.isFile());
        final Model contModel = loadModel(containerFile.getAbsolutePath());
        assertTrue(contModel.contains(container, RDF_TYPE, CONTAINER));

        assertTrue(binaryFile.exists() && binaryFile.isFile());
        assertEquals("", IOUtils.toString(new FileInputStream(binaryFile)));
        assertEquals(0L, binaryFile.length());

        assertTrue(descFile.exists() && descFile.isFile());
        final Model descModel = loadModel(descFile.getAbsolutePath());
        assertTrue(descModel.contains(binary, RDF_TYPE, NON_RDF_SOURCE));
        assertTrue(descModel.contains(binary, createProperty(PREMIS_SIZE), longLiteral("0")));
        assertTrue(descModel.contains(binary, createProperty(EBU_HAS_MIME_TYPE), externalContent));

        // verify that the resources exist in the repository
        assertTrue(exists(res1));
        assertTrue(exists(file1));
        final FcrepoResponse redirResp = client.get(file1).disableRedirects().perform();
        assertEquals(307, redirResp.getStatusCode());
        assertEquals(externalURI, redirResp.getLocation());

        final Model model = getAsModel(file1desc);
        assertTrue(model.contains(binary, RDF_TYPE, createResource(LDP_NON_RDF_SOURCE)));
        assertTrue(model.contains(binary, createProperty(SKOS_PREFLABEL), "original version"));
        assertTrue(model.contains(binary, createProperty(PREMIS_SIZE), longLiteral("0")));
        assertTrue(model.contains(binary, createProperty(PREMIS_DIGEST),
                createResource("urn:sha1:da39a3ee5e6b4b0d3255bfef95601890afd80709")));
    }

    @Test
    public void testRoundtripOverwrite() throws Exception {
        final URI uri = URI.create(serverAddress + UUID.randomUUID());
        final FileInputStream stream = new FileInputStream("src/test/resources/test.ttl");
        final URI parentURI = URI.create(uri.toString() + "/res1");
        final URI childURI = URI.create(parentURI.toString() + "/child1");
        final URI fileURI = URI.create(parentURI.toString() + "/file1");
        final File fileContent = new File("src/test/resources/binary.txt");

        final FcrepoResponse response = createBody(uri, stream, "text/turtle");
        assertEquals(SC_CREATED, response.getStatusCode());
        assertEquals(uri, response.getLocation());
        create(parentURI);
        create(childURI);
        createBody(fileURI, new FileInputStream(fileContent), "text/plain");

        roundtrip(uri, false);

        // verify that the resources have been created
        final Model model = getAsModel(uri);
        assertTrue(model.contains(createResource(uri.toString()), createProperty(DC_TITLE), "this is a title"));
        assertTrue(exists(parentURI));
        assertTrue(exists(childURI));
        assertTrue(exists(fileURI));
        assertEquals("this is some content\n", getAsString(fileURI));
    }

    @Test
    public void testRoundtripOverwriteBinary() throws Exception {
        final URI fileURI = URI.create(serverAddress + UUID.randomUUID());
        final File fileContent = new File("src/test/resources/binary.txt");

        final FcrepoResponse response = createBody(fileURI, new FileInputStream(fileContent), "text/plain");
        assertEquals(SC_CREATED, response.getStatusCode());
        assertEquals(fileURI, response.getLocation());

        roundtrip(fileURI, false);

        // verify that the resources have been created
        assertTrue(exists(fileURI));
        assertEquals("this is some content\n", getAsString(fileURI));
    }

    private Literal dateLiteral(final String dateString) {
        return createTypedLiteral(dateString, XSDdateTime);
    }

    private Literal longLiteral(final String longString) {
        return createTypedLiteral(longString, XSDlong);
    }

    private Config roundtrip(final URI uri, final boolean reset) throws FcrepoOperationFailedException {
        return roundtrip(uri, new ArrayList<URI>(), true, true);
    }

    private Config roundtrip(final URI uri, final List<URI> relatedResources,
            final boolean reset, final boolean includeBinary)
            throws FcrepoOperationFailedException {
        // export resources
        final Config config = new Config();
        config.setMode("export");
        config.setBaseDirectory(TARGET_DIR + File.separator + UUID.randomUUID());
        config.setIncludeBinaries(includeBinary);
        config.setResource(uri);
        config.setPredicates(new String[]{ CONTAINS.toString() });
        config.setRdfExtension(DEFAULT_RDF_EXT);
        config.setRdfLanguage(DEFAULT_RDF_LANG);
        config.setUsername(USERNAME);
        config.setPassword(PASSWORD);
        new Exporter(config, clientBuilder).run();

        // delete container and optionally remove tombstone
        if (reset) {
            removeAndReset(uri);
            for (final URI rel : relatedResources) {
                removeAndReset(rel);
            }
        } else {
            remove(uri);
            for (final URI rel : relatedResources) {
                remove(rel);
            }
        }

        // setup config for import to a new base URL, then perform import
        config.setMode("import");
        if (!reset) {
            config.setOverwriteTombstones(true);
        }
        new Importer(config, clientBuilder).run();

        return config;
    }

    @Override
    protected Logger logger() {
        return getLogger(RoundtripIT.class);
    }
}
