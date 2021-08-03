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

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
import org.fcrepo.client.FcrepoOperationFailedException;
import org.fcrepo.client.FcrepoResponse;
import org.fcrepo.importexport.common.Config;
import org.fcrepo.importexport.exporter.Exporter;
import org.junit.Test;
import org.slf4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.apache.commons.io.FileUtils.readFileToString;
import static org.apache.http.HttpStatus.SC_CREATED;
import static org.apache.jena.rdf.model.ResourceFactory.createProperty;
import static org.apache.jena.rdf.model.ResourceFactory.createResource;
import static org.apache.jena.rdf.model.ResourceFactory.createStringLiteral;
import static org.apache.jena.riot.RDFDataMgr.loadModel;
import static org.fcrepo.importexport.common.Config.DEFAULT_RDF_EXT;
import static org.fcrepo.importexport.common.Config.DEFAULT_RDF_LANG;
import static org.fcrepo.importexport.common.FcrepoConstants.CONTAINER;
import static org.fcrepo.importexport.common.FcrepoConstants.CONTAINS;
import static org.fcrepo.importexport.common.FcrepoConstants.EXTERNAL_RESOURCE_EXTENSION;
import static org.fcrepo.importexport.common.FcrepoConstants.HAS_MIME_TYPE;
import static org.fcrepo.importexport.common.FcrepoConstants.RDF_TYPE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
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
    public void testExportBogusResource() throws Exception {
        final Config config = new Config();
        config.setMode("export");
        config.setBaseDirectory(TARGET_DIR);
        config.setResource(url);
        config.setRepositoryRoot(serverAddress);
        config.setRdfLanguage(DEFAULT_RDF_LANG);
        config.setUsername(USERNAME);
        config.setPassword(PASSWORD);
        config.setPredicates(new String[]{ CONTAINS.toString() });

        final Exporter exporter = new Exporter(config, clientBuilder);
        exporter.run();

        // Verify
        assertFalse(new File(TARGET_DIR, url.getPath() + DEFAULT_RDF_EXT).exists());

        // The failed resource should be logged to the remaining log
        final File[] files = new File(TARGET_DIR).listFiles((dir, name) -> name.startsWith("remaining_"));
        Arrays.sort(files, Comparator.comparingLong(File::lastModified).reversed());

        assertTrue(files.length > 0);

        final String contents = FileUtils.readFileToString(files[0], StandardCharsets.UTF_8);

        assertEquals(url + "\n", contents);
    }

    @Test
    public void exportFromResourcesFile() throws Exception {
        // Create a repository resource
        final FcrepoResponse response = create(url);
        assertEquals(SC_CREATED, response.getStatusCode());
        assertEquals(url, response.getLocation());

        final URI url2 = URI.create(serverAddress + UUID.randomUUID());

        final FcrepoResponse response2 = create(url2);
        assertEquals(SC_CREATED, response2.getStatusCode());
        assertEquals(url2, response2.getLocation());

        final File resourcesFile = new File(TARGET_DIR, UUID.randomUUID().toString());

        FileUtils.writeLines(resourcesFile, Arrays.asList(serverAddress, url.toString(), url2.toString()));

        // Run an export process
        final Config config = new Config();
        config.setMode("export");
        config.setBaseDirectory(TARGET_DIR);
        config.setRepositoryRoot(serverAddress);
        config.setResourceFile(resourcesFile.toPath());
        config.setRdfLanguage(DEFAULT_RDF_LANG);
        config.setUsername(USERNAME);
        config.setPassword(PASSWORD);
        config.setPredicates(new String[]{ CONTAINS.toString() });

        final Exporter exporter = new Exporter(config, clientBuilder);
        exporter.run();

        // Verify
        assertTrue(new File(TARGET_DIR, url.getPath() + DEFAULT_RDF_EXT).exists());
        assertTrue(new File(TARGET_DIR, url2.getPath() + DEFAULT_RDF_EXT).exists());
    }

    @Test
    public void testExportExcludedBinaries() throws Exception {
        final UUID uuid = UUID.randomUUID();
        final String baseURI = serverAddress + uuid;
        final URI res1 = URI.create(baseURI);
        final URI file1 = URI.create(baseURI + "/file1");

        final Resource container = createResource(res1.toString());

        final String file1patch = "insert data { "
                + "<" + file1.toString() + "> <" + SKOS_PREFLABEL + "> \"original version\" . }";

        create(res1);
        final FcrepoResponse resp = createBody(file1, "this is some content", "text/plain");
        final URI file1desc = resp.getLinkHeaders("describedby").get(0);
        patch(file1desc, file1patch);

        assertTrue(exists(res1));
        assertTrue(exists(file1));

        // Run an export process
        final Config config = new Config();
        config.setMode("export");
        config.setBaseDirectory(TARGET_DIR);
        config.setResource(res1);
        config.setIncludeBinaries(false);
        config.setRdfLanguage(DEFAULT_RDF_LANG);
        config.setUsername(USERNAME);
        config.setPassword(PASSWORD);
        config.setPredicates(new String[] { CONTAINS.toString() });

        final Exporter exporter = new Exporter(config, clientBuilder);
        exporter.run();

        // verify that files exist and contain expected content
        final File exportDir = config.getBaseDirectory();
        final File containerFile = new File(exportDir, ROOT_PATH + uuid + config.getRdfExtension());
        final File binaryFile = new File(exportDir, ROOT_PATH + uuid + "/file1.binary");
        final File descFile = new File(exportDir, ROOT_PATH + uuid + "/file1/fcr%3Ametadata"
                + config.getRdfExtension());

        assertTrue(containerFile.exists() && containerFile.isFile());
        final Model contModel = loadModel(containerFile.getAbsolutePath());

        assertTrue(contModel.contains(container, RDF_TYPE, CONTAINER));
        // verify that references binaries are excluded
        assertFalse(contModel.contains(container, null, createResource(file1.toString())));

        // verify that the files for binaries are not on disk
        assertFalse(binaryFile.exists());
        assertFalse(descFile.exists());
    }

    @Test
    public void testExportRetrieveExternal() throws Exception {
        // Create an external content resource pointing at another repository resource
        final URI binaryURI = URI.create(serverAddress + UUID.randomUUID());
        createBody(binaryURI, "content", "text/plain");
        final Map<String,String> headers = new HashMap<>();
        headers.put("Link", "<" + binaryURI.toString() + ">;" +
                "      rel=\"http://fedora.info/definitions/fcrepo#ExternalContent\"; " +
                "      handling=\"redirect\"; " +
                "      type=\"text/plain\"");

        createBody(url, new ByteArrayInputStream("".getBytes()), "text/plain", headers);

        // Run an export process
        final Config config = new Config();
        config.setMode("export");
        config.setBaseDirectory(TARGET_DIR);
        config.setResource(url);
        config.setIncludeBinaries(true);
        config.setRetrieveExternal(true);
        config.setUsername(USERNAME);
        config.setPassword(PASSWORD);

        final Exporter exporter = new Exporter(config, clientBuilder);
        exporter.run();

        // Verify
        final File externalFile = new File(TARGET_DIR, url.getPath() + EXTERNAL_RESOURCE_EXTENSION);
        assertTrue(externalFile.exists());
        assertEquals("content", readFileToString(externalFile, "UTF-8"));
    }

    @Test
    public void testExportRetrieveExternalDisabledWithRedirect() throws Exception {
        // Create an external content resource pointing at another repository resource
        final URI binaryURI = URI.create(serverAddress + UUID.randomUUID());
        createBody(binaryURI, "content", "text/plain");
        final Map<String,String> headers = new HashMap<>();
        headers.put("Link", "<" + binaryURI.toString() + ">;" +
                "      rel=\"http://fedora.info/definitions/fcrepo#ExternalContent\"; " +
                "      handling=\"redirect\"; " +
                "      type=\"text/plain\"");

        createBody(url, new ByteArrayInputStream("".getBytes()), "text/plain", headers);

        // Run an export process
        final Config config = new Config();
        config.setMode("export");
        config.setBaseDirectory(TARGET_DIR);
        config.setResource(url);
        config.setIncludeBinaries(true);
        config.setRetrieveExternal(false);
        config.setUsername(USERNAME);
        config.setPassword(PASSWORD);

        final Exporter exporter = new Exporter(config, clientBuilder);
        exporter.run();

        // Verify
        final File externalFile = new File(TARGET_DIR, url.getPath() + EXTERNAL_RESOURCE_EXTENSION);
        assertTrue(externalFile.exists());
        assertEquals("File length should be 0", 0, externalFile.length());
    }

    @Test
    public void testExportRetrieveExternalDisabledWithProxy() throws Exception {
        // Create an external content resource pointing at a local file resource
        final File file = File.createTempFile("test",".txt");
        file.deleteOnExit();
        final String fileStr = "file://" + file.toPath().toAbsolutePath().toString();
        logger().debug("External File Resource = {}", fileStr);
        final Map<String,String> headers = new HashMap<>();
        headers.put("Link", "<" + fileStr + ">;" +
                "rel=\"http://fedora.info/definitions/fcrepo#ExternalContent\"; " +
                "handling=\"proxy\"; type=\"text/plain\"");
        final FcrepoResponse proxiedResourceResponse = createBody(url, new ByteArrayInputStream("".getBytes()),
                "text/plain", headers);
        assertEquals(SC_CREATED, proxiedResourceResponse.getStatusCode());
        // Run an export process
        final Config config = new Config();
        config.setMode("export");
        config.setBaseDirectory(TARGET_DIR);
        config.setResource(url);
        config.setIncludeBinaries(true);
        config.setRetrieveExternal(false);
        config.setUsername(USERNAME);
        config.setPassword(PASSWORD);

        final Exporter exporter = new Exporter(config, clientBuilder);
        try {
            exporter.run();
        } catch(Exception ex) {
            logger().error(ex.getMessage(), ex);
            fail("Exporter failed unexpectedly: " + ex.getMessage());
        }

        // Verify
        final File externalFile = new File(TARGET_DIR, url.getPath() + EXTERNAL_RESOURCE_EXTENSION);
        assertTrue("External file should exist: " + externalFile.getAbsoluteFile(), externalFile.exists());
        assertEquals("File length should be 0", 0, externalFile.length());
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

    @Test
    public void testExportVersions() throws Exception {
        final UUID uuid = UUID.randomUUID();
        final String baseURI = serverAddress + uuid;
        final URI res1 = URI.create(baseURI + "/res1");
        final URI res2 = URI.create(baseURI + "/res1/res2");
        final URI binRes = URI.create(baseURI + "/res1/file");

        final String rfc1123Date = "Wed, 13 Mar 2019 17:58:45 GMT";
        final String versionLabel = "20190313175845";


        create(res1);
        create(res2);
        createBody(binRes, "binary", "text/plain");
        createMemento(res1, rfc1123Date);
        createMemento(res2, rfc1123Date);
        createMemento(binRes, rfc1123Date);

        final Config config = new Config();
        config.setMode("export");
        config.setBaseDirectory(TARGET_DIR + "/" + uuid);
        config.setResource(res1);
        config.setRdfExtension(DEFAULT_RDF_EXT);
        config.setRdfLanguage(DEFAULT_RDF_LANG);
        config.setUsername(USERNAME);
        config.setPassword(PASSWORD);
        config.setIncludeVersions(true);
        config.setIncludeBinaries(true);

        final Exporter exporter = new Exporter(config, clientBuilder);
        exporter.run();

        final File baseDir = new File(config.getBaseDirectory(), "/fcrepo/rest/" + uuid);
        assertTrue(new File(baseDir, "/res1" + DEFAULT_RDF_EXT).exists());
        assertTrue(new File(baseDir, "/res1/res2" + DEFAULT_RDF_EXT).exists());
        assertTrue(new File(baseDir, "/res1/file.binary").exists());
        assertTrue(new File(baseDir, "/res1/file/fcr%3Ametadata" + DEFAULT_RDF_EXT).exists());
        assertTrue(new File(baseDir, "/res1/fcr%3Aversions" + DEFAULT_RDF_EXT).exists());
        assertTrue(new File(baseDir, "/res1/fcr%3Aversions/" + versionLabel + DEFAULT_RDF_EXT).exists());
        assertTrue(new File(baseDir, "/res1/res2/fcr%3Aversions/" + versionLabel + DEFAULT_RDF_EXT).exists());
        assertTrue(new File(baseDir, "/res1/res2/fcr%3Aversions/" + versionLabel + DEFAULT_RDF_EXT + ".headers")
            .exists());
        assertTrue(new File(baseDir, "/res1/file/fcr%3Aversions/" + versionLabel + ".binary").exists());
        assertTrue(new File(baseDir, "/res1/file/fcr%3Aversions/" + versionLabel + ".binary.headers").exists());
    }


    @Test
    public void testExportAcl() throws Exception {
        final UUID uuid = UUID.randomUUID();
        final String baseURI = serverAddress + uuid;
        final URI res1 = URI.create(baseURI + "/res1");
        final URI res1Acl = URI.create(baseURI + "/res1/fcr:acl");

        create(res1);
        create(res1Acl);

        final Config config = new Config();
        config.setMode("export");
        config.setBaseDirectory(TARGET_DIR + "/" + uuid);
        config.setResource(res1);
        config.setRdfExtension(DEFAULT_RDF_EXT);
        config.setRdfLanguage(DEFAULT_RDF_LANG);
        config.setUsername(USERNAME);
        config.setPassword(PASSWORD);
        config.setIncludeAcls(true);
        config.setIncludeVersions(false);
        config.setIncludeBinaries(false);

        final Exporter exporter = new Exporter(config, clientBuilder);
        exporter.run();

        final File baseDir = new File(config.getBaseDirectory(), "/fcrepo/rest/" + uuid);
        assertTrue(new File(baseDir, "/res1" + DEFAULT_RDF_EXT).exists());
        assertTrue(new File(baseDir, "/res1/fcr%3Aacl" + DEFAULT_RDF_EXT).exists());
    }
    private void createMemento(final URI uri, final String rfc1123Date) throws FcrepoOperationFailedException {
        final InputStream body = clientBuilder.build().get(uri).accept(DEFAULT_RDF_LANG).perform().getBody();
        final String timeMap = uri.toString() + "/fcr:versions";
        final FcrepoResponse response = clientBuilder.build().createMemento(URI.create(timeMap), rfc1123Date)
            .body(body, DEFAULT_RDF_LANG).perform();
        try {
            final String mementoResponse = IOUtils.toString(response.getBody(), "UTF-8");
            logger().info("Create memento:  status={}; location={}; url = {}; body={}",
                response.getStatusCode(), response.getHeaderValue("Location"), timeMap, mementoResponse);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
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

    @Test
    public void testExportCustomPredicatesInbound() throws Exception {
        final UUID uuid = UUID.randomUUID();
        final String baseURI = serverAddress + uuid;
        final URI col1 = URI.create(baseURI + "/col1");
        final URI obj1 = URI.create(baseURI + "/obj1");
        final URI obj2 = URI.create(baseURI + "/obj2");
        final URI other = URI.create(baseURI + "/other");

        create(col1);
        createTurtle(obj1, "<> <http://example.org/custom> <" + col1.toString() + "> .");
        createTurtle(obj2, "<> <http://example.org/custom> <" + col1.toString() + "> .");
        createTurtle(other, "<> <http://example.org/other> <" + col1.toString() + "> .");

        // export with custom predicates
        final Config config = new Config();
        config.setMode("export");
        config.setBaseDirectory(TARGET_DIR + "/" + uuid);
        config.setResource(col1);
        config.setRdfExtension(DEFAULT_RDF_EXT);
        config.setRdfLanguage(DEFAULT_RDF_LANG);
        config.setUsername(USERNAME);
        config.setPassword(PASSWORD);
        config.setPredicates(new String[] { "http://example.org/custom" });
        config.setRetrieveInbound(true);

        new Exporter(config, clientBuilder).run();

        // the inbound objects should be written to disk
        final File baseDir = new File(config.getBaseDirectory(), "/fcrepo/rest/" + uuid);
        assertTrue(new File(baseDir, "/obj1" + DEFAULT_RDF_EXT).exists());
        assertTrue(new File(baseDir, "/obj2" + DEFAULT_RDF_EXT).exists());

        // the inbound links should not be written to disk
        final File collectionFile = new File(baseDir, "/col1" + DEFAULT_RDF_EXT);
        assertTrue(collectionFile.exists());
        final Model collectionModel = loadModel(collectionFile.getAbsolutePath());
        assertFalse(collectionModel.contains(
                createResource(obj1.toString()),
                createProperty("http://example.org/custom"),
                createResource(col1.toString())));
        assertFalse(collectionModel.contains(
                createResource(other.toString()),
                createProperty("http://example.org/other"),
                createResource(col1.toString())));
    }

    @Test
    public void testExportInboundFiles() throws Exception {
        final UUID uuid = UUID.randomUUID();
        final String baseURI = serverAddress + uuid;
        final URI fileURI = URI.create(baseURI + "/file9");
        createBody(fileURI, "This is some file content", "text/plain");

        // export with inbound references
        final Config config = new Config();
        config.setMode("export");
        config.setBaseDirectory(TARGET_DIR + "/" + uuid);
        config.setResource(baseURI);
        config.setUsername(USERNAME);
        config.setPassword(PASSWORD);
        config.setIncludeBinaries(true);
        config.setRetrieveInbound(true);

        new Exporter(config, clientBuilder).run();

        final File baseDir = new File(config.getBaseDirectory(), "/fcrepo/rest/" + uuid);
        final File file = new File(baseDir, "file9.binary");
        assertTrue(file.exists());
        final File fileDesc = new File(baseDir, "file9/fcr%3Ametadata" + DEFAULT_RDF_EXT);
        assertTrue(fileDesc.exists());
        final Model fileModel = loadModel(fileDesc.getAbsolutePath());
        assertTrue(fileModel.contains(
                createResource(fileURI.toString()),
                HAS_MIME_TYPE,
                createStringLiteral("text/plain")));
    }

    @Test
    public void exportDirectContainerWithoutMembership() throws Exception {
        final UUID uuid = UUID.randomUUID();
        final String baseURI = serverAddress + uuid;
        final URI res1 = URI.create(baseURI + "/res1");
        final URI parts = URI.create(baseURI + "/res1/parts");
        final URI part1 = URI.create(baseURI + "/res1/parts/part1");

        final String partsTurtle = " <> <" + LDP_HAS_MEMBER_RELATION + "> <" + DCTERMS_HAS_PART + "> ; "
                + "<" + LDP_MEMBERSHIP_RESOURCE + "> <" +  res1.toString() + "> .";

        create(res1);

        final Map<String, String> headers = new HashMap<>();
        headers.put("Link", "<" + LDP_DIRECT_CONTAINER + ">; rel=\"type\"");

        createTurtle(parts, partsTurtle, headers);
        create(part1);

        final Config config = new Config();
        config.setMode("export");
        config.setBaseDirectory(TARGET_DIR + "/" + uuid);
        config.setResource(baseURI);
        config.setIncludeMembership(false);
        config.setUsername(USERNAME);
        config.setPassword(PASSWORD);

        new Exporter(config, clientBuilder).run();

        final File baseDir = new File(config.getBaseDirectory(), "/fcrepo/rest/" + uuid);
        final File file = new File(baseDir, "res1" + DEFAULT_RDF_EXT);
        assertTrue(file.exists());
        final Model model = loadModel(file.getAbsolutePath());
        assertFalse(model.contains(createResource(res1.toString()),
                createProperty(DCTERMS_HAS_PART),
                createResource(part1.toString())));
    }

    @Override
    protected Logger logger() {
        return getLogger(ExporterIT.class);
    }

}
