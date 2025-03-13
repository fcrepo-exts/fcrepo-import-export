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
package org.fcrepo.importexport.exporter;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.IOUtils;
import org.apache.jena.vocabulary.DC;
import org.duraspace.bagit.BagItDigest;
import org.duraspace.bagit.profile.BagProfile;
import org.fcrepo.client.FcrepoClient;
import org.fcrepo.client.FcrepoOperationFailedException;
import org.fcrepo.client.FcrepoResponse;
import org.fcrepo.client.HeadBuilder;
import org.fcrepo.importexport.common.AuthenticationRequiredRuntimeException;
import org.fcrepo.importexport.common.Config;
import org.fcrepo.importexport.test.util.ResponseMocker;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.commons.io.FileUtils.readLines;
import static org.duraspace.bagit.profile.BagProfileConstants.BAGIT_PROFILE_IDENTIFIER;
import static org.fcrepo.importexport.common.FcrepoConstants.BINARY_EXTENSION;
import static org.fcrepo.importexport.common.FcrepoConstants.CONTAINS;
import static org.fcrepo.importexport.common.FcrepoConstants.DESCRIBEDBY;
import static org.fcrepo.importexport.common.FcrepoConstants.EXTERNAL_RESOURCE_EXTENSION;
import static org.fcrepo.importexport.common.FcrepoConstants.HEADERS_EXTENSION;
import static org.fcrepo.importexport.common.FcrepoConstants.NON_RDF_DESCRIPTION;
import static org.fcrepo.importexport.common.FcrepoConstants.RDF_SOURCE;
import static org.fcrepo.importexport.common.FcrepoConstants.RDF_TYPE;
import static org.fcrepo.importexport.common.FcrepoConstants.REPOSITORY_NAMESPACE;
import static org.fcrepo.importexport.common.FcrepoConstants.REPOSITORY_ROOT;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the StreamExporter class.
 */
public class ExportStreamTest extends ExportTestBase {

    private Config config;

    private String id;

    /**
     * The resources to be exported.
     * rootResource (in ExportTestBase) is the repository root.
     * resource (in ExportTestBase) is a container.
     * resource2 is a binary.
     * resource3 is a description of resource2.
     * resource4 is a container, often used as an alternate description of resource2.
     */
    private URI resource2;
    private URI resource3;
    private URI resource4;

    private StreamExporterWrapper exporter;
    private StreamTripleHandlerWrapper handler;

    public ExportStreamTest() throws URISyntaxException {
        super();
        exportDirectory = new File("target/stream-export").getAbsolutePath();
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        id = UUID.randomUUID().toString();
        resource = URI.create("http://localhost:8080/rest/" + id);

        config = new Config();
        config.setStreaming(true);
        config.setRdfLanguage("application/n-triples");
        config.setBaseDirectory(exportDirectory);
        config.setResource(resource);
        final String rootBody = "<" + rootResource + "> <" + CONTAINS + "> <" + resource + "> ." +
                "<" + rootResource + "> <" + RDF_TYPE + "> <" + REPOSITORY_ROOT + "> .";
        mockResponse(rootResource, containerLinks, new ArrayList<>(), rootBody);

        resource2 = URI.create("http://localhost:8080/rest/file1");
        resource3 = URI.create("http://localhost:8080/rest/file1/fcr:metadata");
        resource4 = URI.create("http://localhost:8080/rest/alt_description");

        describedbyLinks.add(resource3);
        describedbyLinks.add(resource4);

        mockResponse(resource, containerLinks, emptyList(), "<" + resource + "> <" + RDF_TYPE +
                "> <" + RDF_SOURCE + "> .\n <" + resource + "> <" + CONTAINS + "> <" + resource2 + "> .");
        mockResponse(resource2, binaryLinks, describedbyLinks,null,  "binary", "image/tiff");
        mockResponse(resource3, descriptionLinks, new ArrayList<>(), "<" + resource3 + "> <" + RDF_TYPE +
                "> <" + NON_RDF_DESCRIPTION + "> .");
        mockResponse(resource4, containerLinks, new ArrayList<>(), "<" + resource4 + "> <" + RDF_TYPE +
                "> <" + RDF_SOURCE + "> .");

        reconfigureExporter();
    }

    /**
     * Reconfigure the exporter and handler to use the current configuration.
     */
    private void reconfigureExporter() {
        exporter = new StreamExporterWrapper(config, clientBuilder);
        handler = new StreamTripleHandlerWrapper(config, exporter, client);
        exporter.setHandler(handler);
    }

    @Test
    public void testContent() throws Exception {
        final String content = "<" + resource + "> <" + DC.title + "> \"Title\" .\n" +
                "<" + resource + "> <" + DC.creator + "> \"Creator\" .\n";
        mockResponse(resource, descriptionLinks, describedbyLinks, content);
        exporter.run();
        assertTrue(handler.wroteFile(exportDirectory + "/rest/" + id + ".nt"));
    }

    @Test
    public void testBinaryExport() throws Exception {
        config.setIncludeBinaries(true);
        resource2 = URI.create("http://localhost:8080/rest/" + id + "/fcr:metadata");
        final String alternateID = UUID.randomUUID().toString();
        resource3 = URI.create("http://localhost:8080/rest/" + alternateID);

        final String content = "<" + resource  + "> <" + DC.title + "> \"Title\" .\n" +
                "<" + resource + "> <" + DC.creator + "> \"Creator\" .\n" +
                "<" + resource + "> <" + DESCRIBEDBY.getURI() + "> <" + resource2 + "> .\n";
        describedbyLinks.add(resource2);
        describedbyLinks.add(resource3);
        mockResponse(resource, binaryLinks, describedbyLinks, "Some binary content");
        mockResponse(resource2, containerLinks, emptyList(), content);
        final String alternate = "<" + resource + "> <" + DC.title + "> \"Alternate Title\" .\n" +
                "<" + resource + "> <" + DC.creator + "> \"Alternate Creator\" .\n";
        mockResponse(resource3, containerLinks, emptyList(), alternate);
        exporter.run();
        assertTrue(exporter.wroteFile(exportDirectory + "/rest/" + id + BINARY_EXTENSION));
        assertTrue(exporter.wroteFile(exportDirectory + "/rest/" + id + BINARY_EXTENSION + ".headers"));
        assertTrue(handler.wroteFile(exportDirectory + "/rest/" + id + "/fcr%3Ametadata.nt"));
        assertTrue(exporter.wroteFile(exportDirectory + "/rest/" + id + "/fcr%3Ametadata.nt.headers"));
        assertTrue(handler.wroteFile(exportDirectory + "/rest/" + alternateID + ".nt"));
        assertTrue(exporter.wroteFile(exportDirectory + "/rest/" + alternateID + ".nt.headers"));
    }

    @Test
    public void testExportBinaryAndDescription() throws Exception, FcrepoOperationFailedException {
        config.setIncludeBinaries(true);
        config.setResource(resource2);

        exporter.run();
        assertTrue(exporter.wroteFile(exportDirectory + "/rest/file1" + BINARY_EXTENSION));
        assertTrue(handler.wroteFile(exportDirectory + "/rest/file1/fcr%3Ametadata.nt"));
        assertTrue(handler.wroteFile(exportDirectory + "/rest/alt_description.nt"));
        assertTrue(exporter.wroteFile(exportDirectory + "/rest/file1" + BINARY_EXTENSION + HEADERS_EXTENSION));
        assertTrue(exporter.wroteFile(exportDirectory + "/rest/file1/fcr%3Ametadata.nt" + HEADERS_EXTENSION));
        assertTrue(exporter.wroteFile(exportDirectory + "/rest/alt_description.nt" + HEADERS_EXTENSION));
    }

    @Test
    public void testExportBag() throws Exception {
        config.setIncludeBinaries(true);
        config.setRetrieveExternal(true);
        config.setResource(resource2);
        config.setBagProfile("default");
        config.setBagConfigPath("src/test/resources/configs/bagit-config.yml");

        reconfigureExporter();

        exporter.run();
        assertTrue(exporter.wroteFile(exportDirectory + "/data/rest/file1" + BINARY_EXTENSION));
        assertTrue(handler.wroteFile(exportDirectory + "/data/rest/file1/fcr%3Ametadata.nt"));
        assertTrue(handler.wroteFile(exportDirectory + "/data/rest/alt_description.nt"));

        final File baginfo = new File(exportDirectory + "/bag-info.txt");
        assertTrue(baginfo.exists());
        final List<String> baginfoLines = readLines(baginfo, UTF_8);
        assertTrue(baginfoLines.contains("Bag-Size: 311 bytes"));
        assertTrue(baginfoLines.contains("Payload-Oxum: 311.3"));
        assertTrue(baginfoLines.contains("Source-Organization: My University"));

        // verify all manifests are written and contain entries for the exported files
        final String manifestFiles = ".*alt_description\\.nt|.*file1\\.binary|.*fcr%3Ametadata\\.nt";
        final File sha1Manifest = new File(exportDirectory + "/manifest-sha1.txt");
        assertTrue(sha1Manifest.exists());
        assertTrue(Files.lines(sha1Manifest.toPath()).allMatch(string -> string.matches(manifestFiles)));

        // verify all tag files are written to the tag manifest (checksum + expected name)
        final String tagFiles = ".*bagit\\.txt|.*bag-info\\.txt|.*aptrust-info\\.txt|.*manifest-sha1\\.txt";
        final File sha1TagManifest = new File(exportDirectory + "/tagmanifest-sha1.txt");
        assertTrue(sha1TagManifest.exists());
        assertTrue(Files.lines(sha1TagManifest.toPath()).allMatch(string -> string.matches(tagFiles)));
    }

    @Test
    public void testExportApTrustBag() throws Exception {
        createAptrustBagConfig();
        config.setBagAlgorithms(new String[]{BagItDigest.SHA256.bagitName()});
        config.setBagSerialization("tar");
        config.setBagConfigPath("src/test/resources/configs/bagit-config.yml");

        reconfigureExporter();

        exporter.run();

        assertTrue(Files.exists(Paths.get(exportDirectory, "manifest-md5.txt")));
        assertTrue(Files.exists(Paths.get(exportDirectory, "manifest-sha256.txt")));
        assertTrue(exporter.wroteFile(exportDirectory + "/data/rest/file1" + BINARY_EXTENSION));
        assertTrue(handler.wroteFile(exportDirectory + "/data/rest/file1/fcr%3Ametadata.nt"));
        assertTrue(handler.wroteFile(exportDirectory + "/data/rest/alt_description.nt"));

        assertTrue(Files.exists(Paths.get(exportDirectory + ".tar")));
        tearDownFiles.add(new File(exportDirectory + ".tar"));

        // instead of extracting the tarball, search for the aptrust-info.txt and load it if found
        List<String> aptrustInfoLines = emptyList();
        try (InputStream is = Files.newInputStream(Paths.get(exportDirectory + ".tar"));
             TarArchiveInputStream tais = new TarArchiveInputStream(is)) {
            TarArchiveEntry entry;
            while ((entry = tais.getNextTarEntry()) != null) {
                if (entry.getName().equalsIgnoreCase("stream-export/aptrust-info.txt")) {
                    aptrustInfoLines = IOUtils.readLines(tais, Charset.defaultCharset());
                    break;
                }
            }
        }

        // And check the aptrust-info.txt was found (isNotEmpty) and expected entries exist
        assertFalse(aptrustInfoLines.isEmpty());
        assertTrue(aptrustInfoLines.contains("Access: Restricted"));
        assertTrue(aptrustInfoLines.contains("Title: My Title"));
        assertTrue(aptrustInfoLines.contains("Storage-Option: Standard"));

    }

    @Test
    public void testExportApTrustBagValidationError() {
        createAptrustBagConfig();
        config.setBagConfigPath("src/test/resources/configs/bagit-config-missing-access.yml");

        assertThrows(RuntimeException.class, () -> new StreamExporterWrapper(config, clientBuilder));
    }

    @Test
    public void testExportApTrustBagInvalidUserAlgorithm() {
        createAptrustBagConfig();
        config.setBagAlgorithms(new String[]{BagItDigest.SHA1.bagitName()});
        config.setBagSerialization("tar");
        config.setBagConfigPath("src/test/resources/configs/bagit-config.yml");

        assertThrows(RuntimeException.class, () -> new StreamExporterWrapper(config, clientBuilder));
    }

    @Test
    public void testExportBeyondTheRepositoryBag() throws IOException {
        final BagProfile profile = new BagProfile(BagProfile.BuiltIn.BEYOND_THE_REPOSITORY);
        final String bagConfigPath = "src/test/resources/configs/bagit-config-no-aptrust.yml";
        final String bagProfileId = BAGIT_PROFILE_IDENTIFIER + ": " + profile.getIdentifier();

        config.setResource(resource2);
        config.setIncludeBinaries(true);
        config.setRetrieveExternal(true);
        config.setBagProfile("beyondtherepository");
        config.setBagConfigPath(bagConfigPath);

        reconfigureExporter();

        exporter.run();
        assertTrue(exporter.wroteFile(exportDirectory + "/data/rest/file1" + BINARY_EXTENSION));
        assertTrue(handler.wroteFile(exportDirectory + "/data/rest/file1/fcr%3Ametadata.nt"));
        assertTrue(handler.wroteFile(exportDirectory + "/data/rest/alt_description.nt"));

        final File bagInfo = new File(exportDirectory + "/bag-info.txt");
        assertTrue(bagInfo.exists());
        final List<String> bagInfoLines = readLines(bagInfo, UTF_8);
        assertTrue(bagInfoLines.contains("Bag-Size: 311 bytes"));
        assertTrue(bagInfoLines.contains("Payload-Oxum: 311.3"));
        assertTrue(bagInfoLines.contains("Source-Organization: My University"));
        assertTrue(bagInfoLines.contains(bagProfileId));
    }

    @Test
    public void testExportBeyondTheRepositoryBagValidationError() {
        config.setIncludeBinaries(true);
        config.setResource(resource2);
        config.setBagProfile("beyondtherepository");
        config.setBagConfigPath("src/test/resources/configs/bagit-config-missing-source-org.yml");

        assertThrows(RuntimeException.class, () -> new StreamExporterWrapper(config, clientBuilder));
    }

    @Test
    public void testExportNoBinaryAndDescription() throws Exception, FcrepoOperationFailedException {
        config.setIncludeBinaries(false);
        config.setResource(resource2);

        exporter.run();
        assertFalse(exporter.wroteFile(exportDirectory + "/rest/file1" + BINARY_EXTENSION));
        assertFalse(handler.wroteFile(exportDirectory + "/rest/file1/fcr%3Ametadata.nt"));
        assertFalse(handler.wroteFile(exportDirectory + "/rest/alt_description.nt"));
    }

    @Test
    public void testExternalContent() throws Exception {
        config.setIncludeBinaries(true);
        config.setResource(resource2);

        final HeadBuilder headBuilder = mock(HeadBuilder.class);
        final FcrepoResponse headResponse = mock(FcrepoResponse.class);
        when(client.head(eq(resource2))).thenReturn(headBuilder);
        when(headBuilder.disableRedirects()).thenReturn(headBuilder);
        when(headBuilder.perform()).thenReturn(headResponse);
        when(headResponse.getUrl()).thenReturn(resource3);
        when(headResponse.getLinkHeaders(eq("describedby"))).thenReturn(describedbyLinks);
        when(headResponse.getStatusCode()).thenReturn(307);
        when(headResponse.getLinkHeaders(eq("type"))).thenReturn(binaryLinks);
        when(headResponse.getHeaderValue("Content-Location")).thenReturn("http://www.example.com/file");

        exporter.run();

        final String externalResourceFile = exportDirectory + "/rest/file1" + EXTERNAL_RESOURCE_EXTENSION;
        assertTrue(exporter.wroteFile(externalResourceFile));
        assertTrue(new File(externalResourceFile).exists());
        assertTrue(handler.wroteFile(exportDirectory + "/rest/file1/fcr%3Ametadata.nt"));
    }

    @Test
    public void testExportContainer() throws Exception {
        config.setIncludeBinaries(true);
        config.setResource(resource);

        exporter.run();
        assertTrue(handler.wroteFile(exportDirectory + "/rest/" + id + ".nt"));
    }

    @Test
    public void testExportAcl() throws Exception {
        config.setIncludeAcls(true);
        config.setIncludeBinaries(true);
        config.setResource(resource);

        final URI resourceAcl = URI.create(resource.toString() + "/fcr:acl");

        mockResponse(resource, containerLinks, new ArrayList<>(), resourceAcl,"<" + resource
                + "> <" + RDF_TYPE + "> <" + REPOSITORY_NAMESPACE + "RepositoryRoot> ."
                , null);
        mockResponse(resourceAcl, containerLinks, emptyList(), "<" + resourceAcl + "> <" + RDF_TYPE + "> <" +
                RDF_SOURCE + "> .");

        exporter.run();

        assertTrue(handler.wroteFile(exportDirectory + "/rest/" + id + ".nt"));
        assertTrue(handler.wroteFile(exportDirectory + "/rest/" + id + "/fcr%3Aacl.nt"));
    }

    @Test
    public void testExportAclRecursive() throws Exception {
        config.setIncludeAcls(true);
        config.setIncludeBinaries(true);
        config.setResource(resource);

        final URI resourceAcl = URI.create(resource.toString() + "/fcr:acl");
        final URI resource2 = URI.create(resource.toString() + "/2");
        final URI resource3 = URI.create(resource2 + "/fcr%3Ametadata");

        mockResponse(resource, containerLinks, new ArrayList<>(), resourceAcl,"<" + resource
                        + "> <" + RDF_TYPE + "> <" + REPOSITORY_NAMESPACE + "RepositoryRoot> .\n"
                        + "<" + resource + "> <" + CONTAINS + "> <" + resource2 + "> ."
                , null);
        mockResponse(resourceAcl, containerLinks, emptyList(), "<" + resourceAcl + "> <" + RDF_TYPE + "> <" +
                RDF_SOURCE + "> .");
        mockResponse(resource2, binaryLinks, singletonList(resource3), "binary");
        mockResponse(resource3, containerLinks, emptyList(), "<" + resource3 + "> <" + RDF_TYPE + "> <" +
                RDF_SOURCE + "> .");

        exporter.run();

        final String first_resource_path = exportDirectory + "/rest/" + id;
        assertTrue(handler.wroteFile(first_resource_path + ".nt"));
        assertTrue(handler.wroteFile(exportDirectory + "/rest/" + id + "/fcr%3Aacl.nt"));
        assertTrue(exporter.wroteFile(first_resource_path + "/2" + BINARY_EXTENSION));
        assertTrue(exporter.wroteFile(first_resource_path + "/2" + BINARY_EXTENSION + ".headers"));
        assertTrue(handler.wroteFile(first_resource_path + "/2/fcr%3Ametadata.nt"));
    }

    @Test
    public void testUnauthenticatedExportWhenAuthorizationIsRequired() throws Exception {
        config.setIncludeBinaries(true);
        config.setResource(resource);

        ResponseMocker.mockHeadResponseError(client, resource, 401);

        assertThrows(AuthenticationRequiredRuntimeException.class, () -> exporter.run());
    }

    @Test
    public void testMetadataOnlyDoesNotExportBinaries() throws Exception {
        config.setResource(resource);

        exporter.run();

        assertTrue(handler.wroteFile(exportDirectory + "/rest/" + id + ".nt"));
        assertFalse(exporter.wroteFile(exportDirectory + "/rest/file1" + BINARY_EXTENSION));
    }

    @Test
    public void testMetadataOnlyExportsContainers() throws Exception {
        config.setResource(resource);

        exporter.run();
        assertTrue(handler.wroteFile(exportDirectory + "/rest/" + id + ".nt"));
    }

    @Test
    public void testRecursive() throws Exception {
        final URI resource6 = URI.create("http://localhost:8080/rest/" + id + "/2");
        mockResponse(resource, containerLinks, emptyList(), "<" + resource + "> <" + RDF_TYPE + "> <" + RDF_SOURCE + "> .\n" +
                "<" + resource + "> <" + CONTAINS + "> <" + resource6 + "> .");
        mockResponse(resource6, containerLinks, emptyList(), "<" + resource6 + "> <" + RDF_TYPE + "> <" + RDF_SOURCE + "> .");
        config.setResource(resource);

        exporter.run();
        assertTrue(handler.wroteFile(exportDirectory + "/rest/" + id + ".nt"));
        assertTrue(handler.wroteFile(exportDirectory + "/rest/" + id + "/2.nt"));
    }

    private void createAptrustBagConfig() {
        config.setMode("export");
        config.setBaseDirectory(exportDirectory);
        config.setIncludeBinaries(true);
        config.setPredicates(predicates);
        config.setRdfLanguage("application/n-triples");
        config.setResource(resource2);
        config.setBagProfile("aptrust");
    }
}

/**
 * A wrapper around the Exporter class to allow for testing of the StreamTripleHandler.
 */
class StreamExporterWrapper extends Exporter {
    private final List<String> writtenFiles = new ArrayList<>();

    StreamExporterWrapper(
            final Config config,
            final FcrepoClient.FcrepoClientBuilder clientBuilder
    ) {
        super(config, clientBuilder);
    }

    @Override
    void writeResponse(final URI uri, final InputStream in, final List<URI> describedby, final File file)
            throws IOException, FcrepoOperationFailedException {
        super.writeResponse(uri, in, describedby, file);
        writtenFiles.add(file.getAbsolutePath());
    }

    @Override
    void writeHeadersFile(final FcrepoResponse response, final File file) throws IOException {
        super.writeHeadersFile(response, file);
        writtenFiles.add(file.getAbsolutePath());

    }

    void setHandler(final StreamTripleHandler handler) {
        this.streamTripleHandler = handler;
    }

    boolean wroteFile(final String file) {
        return writtenFiles.contains(file);
    }

}

/**
 * A wrapper around the StreamTripleHandler class to allow for validating the files written.
 */
class StreamTripleHandlerWrapper extends StreamTripleHandler {

    private final List<String> files = new ArrayList<>();

    StreamTripleHandlerWrapper(
            final Config config,
            final Exporter transferProcess,
            final FcrepoClient client
    ) {
        super(config, transferProcess, client);
    }

    @Override
    public void finish() {
        // Need to make a copy of the file before calling super.finish() because the file reference is set to null.
        // The file is only written if the outputstream exists in finish().
        final File file = this.file;
        super.finish();
        if (file.exists()) {
            files.add(file.getAbsolutePath());
        }
    }

    public boolean wroteFile(final String filename) {
        return files.contains(filename);
    }
}
