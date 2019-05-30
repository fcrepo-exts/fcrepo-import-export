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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.commons.io.FileUtils.readLines;
import static org.fcrepo.importexport.common.FcrepoConstants.BINARY_EXTENSION;
import static org.fcrepo.importexport.common.FcrepoConstants.CONTAINER;
import static org.fcrepo.importexport.common.FcrepoConstants.CONTAINS;
import static org.fcrepo.importexport.common.FcrepoConstants.EXTERNAL_RESOURCE_EXTENSION;
import static org.fcrepo.importexport.common.FcrepoConstants.NON_RDF_SOURCE;
import static org.fcrepo.importexport.common.FcrepoConstants.RDF_SOURCE;
import static org.fcrepo.importexport.common.FcrepoConstants.REPOSITORY_NAMESPACE;
import static org.fcrepo.importexport.common.FcrepoConstants.REPOSITORY_ROOT;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.fcrepo.client.FcrepoClient;
import org.fcrepo.client.FcrepoOperationFailedException;
import org.fcrepo.client.FcrepoResponse;
import org.fcrepo.client.HeadBuilder;
import org.fcrepo.importexport.common.AuthenticationRequiredRuntimeException;
import org.fcrepo.importexport.common.Config;
import org.fcrepo.importexport.test.util.ResponseMocker;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author escowles
 * @since 2016-08-30
 */
public class ExporterTest {

    private FcrepoClient client;
    private FcrepoClient.FcrepoClientBuilder clientBuilder;
    private FcrepoResponse headResponse;
    private List<URI> binaryLinks;
    private List<URI> containerLinks;
    private List<URI> descriptionLinks;
    private List<URI> describedbyLinks;
    private URI rootResource;
    private URI resource;
    private URI resourceAcl;
    private URI resource2;
    private URI resource3;
    private URI resource4;
    private URI resource5;
    private String exportDirectory = "target/export";
    private String[] predicates = new String[]{ CONTAINS.toString() };

    @Before
    public void setUp() throws Exception {
        clientBuilder = mock(FcrepoClient.FcrepoClientBuilder.class);
        client = mock(FcrepoClient.class);
        when(clientBuilder.build()).thenReturn(client);

        headResponse = mock(FcrepoResponse.class);
        rootResource = new URI("http://localhost:8080/rest");
        resource = new URI("http://localhost:8080/rest/1");
        resourceAcl = new URI("http://localhost:8080/rest/1/fcr:acl");

        resource2 = new URI("http://localhost:8080/rest/1/2");
        resource3 = new URI("http://localhost:8080/rest/file1");
        resource4 = new URI("http://localhost:8080/rest/file1/fcr:metadata");
        resource5 = new URI("http://localhost:8080/rest/alt_description");

        binaryLinks = Arrays.asList(new URI(NON_RDF_SOURCE.getURI()));
        containerLinks = Arrays.asList(new URI(CONTAINER.getURI()));
        descriptionLinks = Arrays.asList(new URI(RDF_SOURCE.getURI()));
        describedbyLinks = Arrays.asList(new URI(resource4.toString()), new URI(resource5.toString()));

        mockResponse(resource, containerLinks, new ArrayList<>(), resourceAcl,"{\"@id\":\"" + resource.toString()
                + "\",\"@type\":[\"" + REPOSITORY_NAMESPACE + "RepositoryRoot\"],\""
                + CONTAINS.getURI() + "\":[{\"@id\":\"" + resource2.toString() + "\"}]}");
        mockResponse(resourceAcl, containerLinks, new ArrayList<>(), "{}");

        mockResponse(resource2, containerLinks, new ArrayList<>(), "{\"@id\":\"" + resource2.toString() + "\"}");

        mockResponse(resource3, binaryLinks, describedbyLinks, "binary");
        mockResponse(resource4, descriptionLinks, new ArrayList<>(), "{\"@id\":\"" + resource4.toString() + "\"}");
        mockResponse(resource5, containerLinks, new ArrayList<>(), "{\"@id\":\"" + resource5.toString() + "\"}");
        mockResponse(rootResource, containerLinks, new ArrayList<>(), "{\"@id\":\"" + rootResource.toString()
                + "\",\"@type\":[\"" + REPOSITORY_ROOT.getURI() + "\"]}");
    }

    @After
    public void tearDown() {
        try {
            FileUtils.deleteDirectory(new File(exportDirectory));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void mockResponse(final URI uri, final List<URI> typeLinks, final List<URI> describedbyLinks,
            final String body) throws FcrepoOperationFailedException {
        mockResponse(uri, typeLinks, describedbyLinks, null, body);

    }

    private void mockResponse(final URI uri, final List<URI> typeLinks, final List<URI> describedbyLinks,
                              final URI aclLink, final String body) throws FcrepoOperationFailedException {
        ResponseMocker.mockHeadResponse(client, uri, typeLinks, describedbyLinks, null, aclLink);
        ResponseMocker.mockGetResponse(client, uri, typeLinks, describedbyLinks,  null, aclLink, body);

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
        Assert.assertTrue(exporter.wroteFile(new File(basedir + "/rest/file1" + BINARY_EXTENSION)));
        Assert.assertTrue(exporter.wroteFile(new File(basedir + "/rest/file1/fcr%3Ametadata.jsonld")));
        Assert.assertTrue(exporter.wroteFile(new File(basedir + "/rest/alt_description.jsonld")));
    }

    @Test
    public void testExportBag() throws Exception, FcrepoOperationFailedException {
        final String basedir = exportDirectory + "/2";
        final Config bagArgs = new Config();
        bagArgs.setMode("export");
        bagArgs.setBaseDirectory(basedir);
        bagArgs.setIncludeBinaries(true);
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
        Assert.assertTrue(exporter.wroteFile(new File(basedir + "/data/rest/file1" + BINARY_EXTENSION)));
        Assert.assertTrue(exporter.wroteFile(new File(basedir + "/data/rest/file1/fcr%3Ametadata.jsonld")));
        Assert.assertTrue(exporter.wroteFile(new File(basedir + "/data/rest/alt_description.jsonld")));

        final File baginfo = new File(basedir + "/bag-info.txt");
        Assert.assertTrue(baginfo.exists());
        final List<String> baginfoLines = readLines(baginfo, UTF_8);
        Assert.assertTrue(baginfoLines.contains("Bag-Size: 113 bytes"));
        Assert.assertTrue(baginfoLines.contains("Payload-Oxum: 113.3"));
        Assert.assertTrue(baginfoLines.contains("Source-Organization: My University"));
        Assert.assertTrue(new File(basedir + "/tagmanifest-sha1.txt").exists());
    }

    @Test
    public void testExportApTrustBag() throws Exception, FcrepoOperationFailedException {
        final Config bagArgs = createAptrustBagConfig();
        bagArgs.setBagConfigPath("src/test/resources/configs/bagit-config.yml");

        final ExporterWrapper exporter = new ExporterWrapper(bagArgs, clientBuilder);
        when(headResponse.getLinkHeaders(eq("type"))).thenReturn(binaryLinks);
        when(headResponse.getLinkHeaders(eq("describedby"))).thenReturn(describedbyLinks);
        when(headResponse.getContentType()).thenReturn("image/tiff");
        exporter.run();
        Assert.assertTrue(exporter.wroteFile(new File(exportDirectory + "/data/rest/file1" + BINARY_EXTENSION)));
        Assert.assertTrue(exporter.wroteFile(new File(exportDirectory + "/data/rest/file1/fcr%3Ametadata.jsonld")));
        Assert.assertTrue(exporter.wroteFile(new File(exportDirectory + "/data/rest/alt_description.jsonld")));

        final File baginfo = new File(exportDirectory + "/aptrust-info.txt");
        Assert.assertTrue(baginfo.exists());
        final List<String> baginfoLines = readLines(baginfo, UTF_8);
        Assert.assertTrue(baginfoLines.contains("Access: Restricted"));
        Assert.assertTrue(baginfoLines.contains("Title: My Title"));
    }

    @Test(expected = Exception.class)
    public void testExportApTrustBagValidationError() throws Exception, FcrepoOperationFailedException {
        final Config bagArgs = createAptrustBagConfig();
        bagArgs.setBagConfigPath("src/test/resources/configs/bagit-config-missing-access.yml");
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
        Assert.assertFalse(exporter.wroteFile(new File(basedir + "/rest/file1" + BINARY_EXTENSION)));
        Assert.assertFalse(exporter.wroteFile(new File(basedir + "/rest/file1/fcr%3Ametadata.jsonld")));
        Assert.assertFalse(exporter.wroteFile(new File(basedir + "/rest/alt_description.jsonld")));
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
        Assert.assertTrue(exporter.wroteFile(externalResourceFile));
        Assert.assertTrue(externalResourceFile.exists());
        Assert.assertTrue(exporter.wroteFile(new File(basedir + "/rest/file1/fcr%3Ametadata.jsonld")));
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
        Assert.assertTrue(exporter.wroteFile(new File(basedir + "/rest/1.jsonld")));
    }

    @Test
    public void testExportAcl() throws Exception {
        final String basedir = exportDirectory + "/5";
        final Config args = new Config();
        args.setMode("export");
        args.setBaseDirectory(basedir);
        args.setIncludeBinaries(true);
        args.setPredicates(predicates);
        args.setRdfLanguage("application/ld+json");
        args.setResource(resource);

        final ExporterWrapper exporter = new ExporterWrapper(args, clientBuilder);

        exporter.run();
        Assert.assertTrue(exporter.wroteFile(new File(basedir + "/rest/1.jsonld")));
        Assert.assertTrue(exporter.wroteFile(new File(basedir + "/rest/1/fcr%3Aacl.jsonld")));

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
        Assert.assertFalse(exporter.wroteFile(new File(basedir + "/rest/1")));
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
        Assert.assertTrue(exporter.wroteFile(new File(basedir + "/rest/1.jsonld")));
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
        Assert.assertTrue(exporter.wroteFile(new File(basedir + "/rest/1.jsonld")));
        Assert.assertTrue(exporter.wroteFile(new File(basedir + "/rest/1/2.jsonld")));
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
        Assert.assertTrue(customTags.exists());
        final List<String> customLines = readLines(customTags, UTF_8);
        Assert.assertTrue(customLines.contains("Foo: Bar"));
        Assert.assertTrue(customLines.contains("Baz: Quux"));
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
    boolean wroteFile(final File file) {
        return writtenFiles.contains(file);
    }
}
