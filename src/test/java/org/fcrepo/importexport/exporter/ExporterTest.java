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
import org.duraspace.bagit.BagItDigest;
import org.duraspace.bagit.profile.BagProfile;
import org.fcrepo.client.FcrepoClient;
import org.fcrepo.client.FcrepoOperationFailedException;
import org.fcrepo.client.FcrepoResponse;
import org.fcrepo.client.HeadBuilder;
import org.fcrepo.importexport.common.AuthenticationRequiredRuntimeException;
import org.fcrepo.importexport.common.Config;
import org.fcrepo.importexport.common.TombstoneFoundException;
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
import java.util.Collections;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.commons.io.FileUtils.readLines;
import static org.duraspace.bagit.profile.BagProfileConstants.BAGIT_PROFILE_IDENTIFIER;
import static org.fcrepo.importexport.common.FcrepoConstants.BINARY_EXTENSION;
import static org.fcrepo.importexport.common.FcrepoConstants.CONTAINS;
import static org.fcrepo.importexport.common.FcrepoConstants.EXTERNAL_RESOURCE_EXTENSION;
import static org.fcrepo.importexport.common.FcrepoConstants.HEADERS_EXTENSION;
import static org.fcrepo.importexport.common.FcrepoConstants.REPOSITORY_NAMESPACE;
import static org.fcrepo.importexport.common.FcrepoConstants.REPOSITORY_ROOT;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author escowles
 * @since 2016-08-30
 */
public class ExporterTest extends ExportTestBase {

    private FcrepoResponse headResponse;
    private URI resourceAcl;
    private URI resource2;
    private URI resource3;
    private URI resource4;
    private URI resource5;

    public ExporterTest() throws URISyntaxException {
        super();
        exportDirectory = "target/export";
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();

        headResponse = mock(FcrepoResponse.class);
        resource = new URI("http://localhost:8080/rest/1");
        resourceAcl = new URI("http://localhost:8080/rest/1/fcr:acl");

        resource2 = new URI("http://localhost:8080/rest/1/2");
        resource3 = new URI("http://localhost:8080/rest/file1");
        resource4 = new URI("http://localhost:8080/rest/file1/fcr:metadata");
        resource5 = new URI("http://localhost:8080/rest/alt_description");

        describedbyLinks.add(resource4);
        describedbyLinks.add(resource5);

        mockResponse(resource, containerLinks, new ArrayList<>(), resourceAcl,"{\"@id\":\"" + resource.toString()
                + "\",\"@type\":[\"" + REPOSITORY_NAMESPACE + "RepositoryRoot\"],\""
                + CONTAINS.getURI() + "\":[{\"@id\":\"" + resource2.toString() + "\"}]}", null);
        mockResponse(resourceAcl, containerLinks, new ArrayList<>(), "{}");

        mockResponse(resource2, containerLinks, new ArrayList<>(), "{\"@id\":\"" + resource2.toString() + "\"}");

        mockResponse(resource3, binaryLinks, describedbyLinks, "binary");
        mockResponse(resource4, descriptionLinks, new ArrayList<>(), "{\"@id\":\"" + resource4.toString() + "\"}");
        mockResponse(resource5, containerLinks, new ArrayList<>(), "{\"@id\":\"" + resource5.toString() + "\"}");
        mockResponse(rootResource, containerLinks, new ArrayList<>(), "{\"@id\":\"" + rootResource.toString()
                + "\",\"@type\":[\"" + REPOSITORY_ROOT.getURI() + "\"]}");
    }

    @Test
    public void testExportBinaryAndDescription() throws Exception, FcrepoOperationFailedException {
        final String basedir = exportDirectory + "/1";
        final Config binaryArgs = new Config();
        binaryArgs.setMode("export");
        binaryArgs.setBaseDirectory(basedir);
        binaryArgs.setIncludeBinaries(true);
        binaryArgs.setPredicates(predicates);
        binaryArgs.setRdfLanguage("application/ld+json");
        binaryArgs.setResource(resource3);

        when(headResponse.getLinkHeaders(eq("type"))).thenReturn(binaryLinks);
        when(headResponse.getLinkHeaders(eq("describedby"))).thenReturn(describedbyLinks);

        final ExporterWrapper exporter = new ExporterWrapper(binaryArgs, clientBuilder);
        exporter.run();
        assertTrue(exporter.wroteFile(new File(basedir + "/rest/file1" + BINARY_EXTENSION)));
        assertTrue(exporter.wroteFile(new File(basedir + "/rest/file1/fcr%3Ametadata.jsonld")));
        assertTrue(exporter.wroteFile(new File(basedir + "/rest/alt_description.jsonld")));
        assertTrue(exporter.wroteFile(new File(basedir + "/rest/file1" + BINARY_EXTENSION +
            HEADERS_EXTENSION)));
        assertTrue(exporter.wroteFile(new File(basedir + "/rest/file1/fcr%3Ametadata.jsonld" +
            HEADERS_EXTENSION)));
        assertTrue(exporter.wroteFile(new File(basedir + "/rest/alt_description.jsonld" +
            HEADERS_EXTENSION)));
    }

    @Test
    public void testExportBag() throws Exception {
        final String basedir = exportDirectory + "/2";
        final Config bagArgs = new Config();
        bagArgs.setMode("export");
        bagArgs.setBaseDirectory(basedir);
        bagArgs.setIncludeBinaries(true);
        bagArgs.setRetrieveExternal(true);
        bagArgs.setPredicates(predicates);
        bagArgs.setRdfLanguage("application/ld+json");
        bagArgs.setResource(resource3);
        bagArgs.setBagProfile("default");
        bagArgs.setBagConfigPath("src/test/resources/configs/bagit-config.yml");

        when(headResponse.getLinkHeaders(eq("type"))).thenReturn(binaryLinks);
        when(headResponse.getContentType()).thenReturn("image/tiff");
        when(headResponse.getLinkHeaders(eq("describedby"))).thenReturn(describedbyLinks);

        final ExporterWrapper exporter = new ExporterWrapper(bagArgs, clientBuilder);
        exporter.run();
        assertTrue(exporter.wroteFile(new File(basedir + "/data/rest/file1" + BINARY_EXTENSION)));
        assertTrue(exporter.wroteFile(new File(basedir + "/data/rest/file1/fcr%3Ametadata.jsonld")));
        assertTrue(exporter.wroteFile(new File(basedir + "/data/rest/alt_description.jsonld")));

        final File baginfo = new File(basedir + "/bag-info.txt");
        assertTrue(baginfo.exists());
        final List<String> baginfoLines = readLines(baginfo, UTF_8);
        assertTrue(baginfoLines.contains("Bag-Size: 113 bytes"));
        assertTrue(baginfoLines.contains("Payload-Oxum: 113.3"));
        assertTrue(baginfoLines.contains("Source-Organization: My University"));

        // verify all manifests are written and contain entries for the exported files
        final String manifestFiles = ".*alt_description\\.jsonld|.*file1\\.binary|.*fcr%3Ametadata\\.jsonld";
        final File sha1Manifest = new File(basedir + "/manifest-sha1.txt");
        assertTrue(sha1Manifest.exists());
        assertTrue(Files.lines(sha1Manifest.toPath()).allMatch(string -> string.matches(manifestFiles)));

        // verify all tag files are written to the tag manifest (checksum + expected name)
        final String tagFiles = ".*bagit\\.txt|.*bag-info\\.txt|.*aptrust-info\\.txt|.*manifest-sha1\\.txt";
        final File sha1TagManifest = new File(basedir + "/tagmanifest-sha1.txt");
        assertTrue(sha1TagManifest.exists());
        assertTrue(Files.lines(sha1TagManifest.toPath()).allMatch(string -> string.matches(tagFiles)));
    }

    @Test
    public void testExportApTrustBag() throws Exception {
        final Config bagArgs = createAptrustBagConfig();
        bagArgs.setBagAlgorithms(new String[]{BagItDigest.SHA256.bagitName()});
        bagArgs.setBagSerialization("tar");
        bagArgs.setBagConfigPath("src/test/resources/configs/bagit-config.yml");

        final ExporterWrapper exporter = new ExporterWrapper(bagArgs, clientBuilder);
        when(headResponse.getLinkHeaders(eq("type"))).thenReturn(binaryLinks);
        when(headResponse.getLinkHeaders(eq("describedby"))).thenReturn(describedbyLinks);
        when(headResponse.getContentType()).thenReturn("image/tiff");
        exporter.run();
        assertTrue(Files.exists(Paths.get(exportDirectory, "manifest-md5.txt")));
        assertTrue(Files.exists(Paths.get(exportDirectory, "manifest-sha256.txt")));
        assertTrue(exporter.wroteFile(new File(exportDirectory + "/data/rest/file1" + BINARY_EXTENSION)));
        assertTrue(exporter.wroteFile(new File(exportDirectory + "/data/rest/file1/fcr%3Ametadata.jsonld")));
        assertTrue(exporter.wroteFile(new File(exportDirectory + "/data/rest/alt_description.jsonld")));

        assertTrue(Files.exists(Paths.get(exportDirectory + ".tar")));
        tearDownFiles.add(new File(exportDirectory + ".tar"));

        // instead of extracting the tarball, search for the aptrust-info.txt and load it if found
        List<String> aptrustInfoLines = Collections.emptyList();
        try (InputStream is = Files.newInputStream(Paths.get(exportDirectory + ".tar"));
            TarArchiveInputStream tais = new TarArchiveInputStream(is)) {
            TarArchiveEntry entry;
            while ((entry = tais.getNextTarEntry()) != null) {
                if (entry.getName().equalsIgnoreCase("export/aptrust-info.txt")) {
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

    @Test(expected = Exception.class)
    public void testExportApTrustBagValidationError() {
        final Config bagArgs = createAptrustBagConfig();
        bagArgs.setBagConfigPath("src/test/resources/configs/bagit-config-missing-access.yml");
        final ExporterWrapper exporter = new ExporterWrapper(bagArgs, clientBuilder);
        exporter.run();
    }

    @Test(expected = Exception.class)
    public void testExportApTrustBagInvalidUserAlgorithm() {
        final Config bagArgs = createAptrustBagConfig();
        bagArgs.setBagAlgorithms(new String[]{BagItDigest.SHA1.bagitName()});
        bagArgs.setBagSerialization("tar");
        bagArgs.setBagConfigPath("src/test/resources/configs/bagit-config.yml");
        final ExporterWrapper exporter = new ExporterWrapper(bagArgs, clientBuilder);
        exporter.run();
    }

    /**
     * @return
     */
    private Config createAptrustBagConfig() {
        final Config bagArgs = new Config();
        bagArgs.setMode("export");
        bagArgs.setBaseDirectory(exportDirectory);
        bagArgs.setIncludeBinaries(true);
        bagArgs.setPredicates(predicates);
        bagArgs.setRdfLanguage("application/ld+json");
        bagArgs.setResource(resource3);
        bagArgs.setBagProfile("aptrust");
        return bagArgs;
    }

    @Test
    public void testExportBeyondTheRepositoryBag() throws IOException {
        final BagProfile profile = new BagProfile(BagProfile.BuiltIn.BEYOND_THE_REPOSITORY);
        final String bagConfigPath = "src/test/resources/configs/bagit-config-no-aptrust.yml";
        final String bagProfileId = BAGIT_PROFILE_IDENTIFIER + ": " + profile.getIdentifier();

        final Config config = new Config();
        config.setMode("export");
        config.setBaseDirectory(exportDirectory);
        config.setIncludeBinaries(true);
        config.setRetrieveExternal(true);
        config.setPredicates(predicates);
        config.setRdfLanguage("application/ld+json");
        config.setResource(resource3);
        config.setBagProfile("beyondtherepository");
        config.setBagConfigPath(bagConfigPath);

        final ExporterWrapper exporter = new ExporterWrapper(config, clientBuilder);
        when(headResponse.getLinkHeaders(eq("type"))).thenReturn(binaryLinks);
        when(headResponse.getLinkHeaders(eq("describedby"))).thenReturn(describedbyLinks);
        when(headResponse.getContentType()).thenReturn("image/tiff");
        exporter.run();
        assertTrue(exporter.wroteFile(new File(exportDirectory + "/data/rest/file1" + BINARY_EXTENSION)));
        assertTrue(exporter.wroteFile(new File(exportDirectory + "/data/rest/file1/fcr%3Ametadata.jsonld")));
        assertTrue(exporter.wroteFile(new File(exportDirectory + "/data/rest/alt_description.jsonld")));

        final File bagInfo = new File(exportDirectory + "/bag-info.txt");
        assertTrue(bagInfo.exists());
        final List<String> bagInfoLines = readLines(bagInfo, UTF_8);
        assertTrue(bagInfoLines.contains("Bag-Size: 113 bytes"));
        assertTrue(bagInfoLines.contains("Payload-Oxum: 113.3"));
        assertTrue(bagInfoLines.contains("Source-Organization: My University"));
        assertTrue(bagInfoLines.contains(bagProfileId));
    }

    @Test(expected = Exception.class)
    public void testExportBeyondTheRepositoryBagValidationError() {
        final Config config = new Config();
        config.setMode("export");
        config.setBaseDirectory(exportDirectory);
        config.setIncludeBinaries(true);
        config.setPredicates(predicates);
        config.setRdfLanguage("application/ld+json");
        config.setResource(resource3);
        config.setBagProfile("beyondtherepository");
        config.setBagConfigPath("src/test/resources/configs/bagit-config-missing-source-org.yml");

        final ExporterWrapper exporter = new ExporterWrapper(config, clientBuilder);
        when(headResponse.getLinkHeaders(eq("type"))).thenReturn(binaryLinks);
        when(headResponse.getLinkHeaders(eq("describedby"))).thenReturn(describedbyLinks);
        when(headResponse.getContentType()).thenReturn("image/tiff");
        exporter.run();
    }

    @Test
    public void testExportNoBinaryAndDescription() throws Exception, FcrepoOperationFailedException {
        final String basedir = exportDirectory + "/3";
        final Config noBinaryArgs = new Config();
        noBinaryArgs.setMode("export");
        noBinaryArgs.setBaseDirectory(basedir);
        noBinaryArgs.setIncludeBinaries(false);
        noBinaryArgs.setPredicates(predicates);
        noBinaryArgs.setRdfLanguage("application/ld+json");
        noBinaryArgs.setResource(resource3);

        final ExporterWrapper exporter = new ExporterWrapper(noBinaryArgs, clientBuilder);
        when(headResponse.getContentType()).thenReturn("image/tiff");
        exporter.run();
        assertFalse(exporter.wroteFile(new File(basedir + "/rest/file1" + BINARY_EXTENSION)));
        assertFalse(exporter.wroteFile(new File(basedir + "/rest/file1/fcr%3Ametadata.jsonld")));
        assertFalse(exporter.wroteFile(new File(basedir + "/rest/alt_description.jsonld")));
    }

    @Test
    public void testExternalContent() throws Exception {
        final String basedir = exportDirectory + "/4";
        final Config binaryArgs = new Config();
        binaryArgs.setMode("export");
        binaryArgs.setBaseDirectory(basedir);
        binaryArgs.setIncludeBinaries(true);
        binaryArgs.setPredicates(predicates);
        binaryArgs.setRdfLanguage("application/ld+json");
        binaryArgs.setResource(resource3);

        final ExporterWrapper exporter = new ExporterWrapper(binaryArgs, clientBuilder);

        final HeadBuilder headBuilder = mock(HeadBuilder.class);
        final FcrepoResponse headResponse = mock(FcrepoResponse.class);
        when(client.head(eq(resource3))).thenReturn(headBuilder);
        when(headBuilder.disableRedirects()).thenReturn(headBuilder);
        when(headBuilder.perform()).thenReturn(headResponse);
        when(headResponse.getUrl()).thenReturn(resource3);
        when(headResponse.getLinkHeaders(eq("describedby"))).thenReturn(describedbyLinks);
        when(headResponse.getStatusCode()).thenReturn(307);
        when(headResponse.getLinkHeaders(eq("type"))).thenReturn(binaryLinks);
        when(headResponse.getHeaderValue("Content-Location")).thenReturn("http://www.example.com/file");
        exporter.run();
        final File externalResourceFile = new File(basedir + "/rest/file1" + EXTERNAL_RESOURCE_EXTENSION);
        assertTrue(exporter.wroteFile(externalResourceFile));
        assertTrue(externalResourceFile.exists());
        assertTrue(exporter.wroteFile(new File(basedir + "/rest/file1/fcr%3Ametadata.jsonld")));
    }

    @Test
    public void testExportContainer() throws Exception {
        final String basedir = exportDirectory + "/5";
        final Config args = new Config();
        args.setMode("export");
        args.setBaseDirectory(basedir);
        args.setIncludeBinaries(true);
        args.setPredicates(predicates);
        args.setRdfLanguage("application/ld+json");
        args.setResource(resource);

        final ExporterWrapper exporter = new ExporterWrapper(args, clientBuilder);
        when(headResponse.getLinkHeaders(isA(String.class))).thenReturn(containerLinks);
        exporter.run();
        assertTrue(exporter.wroteFile(new File(basedir + "/rest/1.jsonld")));
    }

    @Test
    public void testExportAcl() throws Exception {
        final String basedir = exportDirectory + "/5";
        final Config args = new Config();
        args.setMode("export");
        args.setBaseDirectory(basedir);
        args.setIncludeAcls(true);
        args.setIncludeBinaries(true);
        args.setPredicates(predicates);
        args.setRdfLanguage("application/ld+json");
        args.setResource(resource);

        final ExporterWrapper exporter = new ExporterWrapper(args, clientBuilder);

        exporter.run();
        assertTrue(exporter.wroteFile(new File(basedir + "/rest/1.jsonld")));
        assertTrue(exporter.wroteFile(new File(basedir + "/rest/1/fcr%3Aacl.jsonld")));
    }

    @Test (expected = AuthenticationRequiredRuntimeException.class)
    public void testUnauthenticatedExportWhenAuthorizationIsRequired() throws Exception {
        final Config args = new Config();
        args.setMode("export");
        args.setBaseDirectory(exportDirectory + "/6");
        args.setIncludeBinaries(true);
        args.setPredicates(predicates);
        args.setRdfLanguage("application/ld+json");
        args.setResource(resource);

        ResponseMocker.mockHeadResponseError(client, resource, 401);
        final ExporterWrapper exporter = new ExporterWrapper(args, clientBuilder);
        exporter.run();
    }

    @Test
    public void testMetadataOnlyDoesNotExportBinaries() throws Exception {
        final String basedir = exportDirectory + "/7";
        final Config metadataArgs = new Config();
        metadataArgs.setMode("export");
        metadataArgs.setBaseDirectory(basedir);
        metadataArgs.setPredicates(predicates);
        metadataArgs.setRdfLanguage("application/ld+json");
        metadataArgs.setResource(resource);

        final ExporterWrapper exporter = new ExporterWrapper(metadataArgs, clientBuilder);
        exporter.run();
        assertFalse(exporter.wroteFile(new File(basedir + "/rest/1")));
    }

    @Test
    public void testMetadataOnlyExportsContainers() throws Exception {
        final String basedir = exportDirectory + "/8";
        final Config metadataArgs = new Config();
        metadataArgs.setMode("export");
        metadataArgs.setBaseDirectory(basedir);
        metadataArgs.setPredicates(predicates);
        metadataArgs.setRdfLanguage("application/ld+json");
        metadataArgs.setResource(resource);

        final ExporterWrapper exporter = new ExporterWrapper(metadataArgs, clientBuilder);
        when(headResponse.getLinkHeaders(isA(String.class))).thenReturn(containerLinks);
        exporter.run();
        assertTrue(exporter.wroteFile(new File(basedir + "/rest/1.jsonld")));
    }

    @Test
    public void testRecursive() throws Exception {
        final String basedir = exportDirectory + "/9";
        final Config args = new Config();
        args.setMode("export");
        args.setBaseDirectory(basedir);
        args.setIncludeBinaries(true);
        args.setPredicates(predicates);
        args.setRdfLanguage("application/ld+json");
        args.setResource(resource);

        final ExporterWrapper exporter = new ExporterWrapper(args, clientBuilder);
        when(headResponse.getLinkHeaders(isA(String.class))).thenReturn(containerLinks);
        exporter.run();
        assertTrue(exporter.wroteFile(new File(basedir + "/rest/1.jsonld")));
        assertTrue(exporter.wroteFile(new File(basedir + "/rest/1/2.jsonld")));
    }

    @Test
    public void testExportBagCustomTags() throws Exception, FcrepoOperationFailedException {
        final String basedir = exportDirectory + "/10";
        final Config bagArgs = new Config();
        bagArgs.setMode("export");
        bagArgs.setBaseDirectory(basedir);
        bagArgs.setIncludeBinaries(true);
        bagArgs.setPredicates(predicates);
        bagArgs.setRdfLanguage("application/ld+json");
        bagArgs.setResource(resource3);
        bagArgs.setBagProfile("default");
        bagArgs.setBagConfigPath("src/test/resources/configs/bagit-config-custom-tagfile.yml");

        final ExporterWrapper exporter = new ExporterWrapper(bagArgs, clientBuilder);
        when(headResponse.getContentType()).thenReturn("image/tiff");
        exporter.run();

        final File customTags = new File(basedir + "/foo-info.txt");
        assertTrue(customTags.exists());
        final List<String> customLines = readLines(customTags, UTF_8);
        assertTrue(customLines.contains("Foo: Bar"));
        assertTrue(customLines.contains("Baz: Quux"));
    }

    @Test(expected = TombstoneFoundException.class)
    public void testExportTombsone() throws Exception {
        final String basedir = exportDirectory + "/11";
        final Config args = new Config();
        args.setMode("export");
        args.setBaseDirectory(basedir);
        args.setIncludeBinaries(false);
        args.setPredicates(predicates);
        args.setRdfLanguage("application/ld+json");
        args.setResource(resource);

        ResponseMocker.mockHeadResponseError(client, resource, 410);
        final ExporterWrapper exporter = new ExporterWrapper(args, clientBuilder);
        exporter.run();
    }

    @Test
    public void testExportTombstoneNested() throws Exception {
        final String basedir = exportDirectory + "/12";
        final Config args = new Config();
        args.setMode("export");
        args.setBaseDirectory(basedir);
        args.setIncludeBinaries(false);
        args.setPredicates(predicates);
        args.setRdfLanguage("application/ld+json");
        args.setResource(resource);

        ResponseMocker.mockHeadResponseError(client, resource2, 410);
        final ExporterWrapper exporter = new ExporterWrapper(args, clientBuilder);
        exporter.run();
        assertFalse(exporter.wroteFile(new File(basedir + "/rest/1.jsonld")));
    }

    @Test
    public void testExportTombstoneNestedWithSkip() throws Exception {
        final String basedir = exportDirectory + "/13";
        final Config args = new Config();
        args.setMode("export");
        args.setBaseDirectory(basedir);
        args.setIncludeBinaries(false);
        args.setPredicates(predicates);
        args.setRdfLanguage("application/ld+json");
        args.setResource(resource);
        args.setSkipTombstoneErrors(true);

        ResponseMocker.mockHeadResponseError(client, resource2, 410);

        final ExporterWrapper exporter = new ExporterWrapper(args, clientBuilder);
        exporter.run();
        assertTrue(exporter.wroteFile(new File(basedir + "/rest/1.jsonld")));
        assertFalse(exporter.wroteFile(new File(basedir + "/rest/1/2.jsonld")));
    }
}

class ExporterWrapper extends Exporter {
    private List<File> writtenFiles = new ArrayList<>();

    ExporterWrapper(final Config config, final FcrepoClient.FcrepoClientBuilder clientBuilder) {
        super(config, clientBuilder);
    }
    @Override
    void writeResponse(final URI uri, final InputStream in, final List<URI> describedby, final File file)
            throws IOException, FcrepoOperationFailedException {
        super.writeResponse(uri, in, describedby, file);
        writtenFiles.add(file);
    }

    @Override
    void writeHeadersFile(final FcrepoResponse response, final File file) throws IOException {
        super.writeHeadersFile(response, file);
        writtenFiles.add(file);

    }

    boolean wroteFile(final File file) {
        return writtenFiles.contains(file);
    }
}
