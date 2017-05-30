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
package org.fcrepo.importexport.importer;

import static org.apache.jena.rdf.model.ModelFactory.createDefaultModel;
import static org.fcrepo.importexport.common.FcrepoConstants.LAST_MODIFIED_DATE;
import static org.fcrepo.importexport.common.FcrepoConstants.REPOSITORY_NAMESPACE;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.List;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.Model;
import org.fcrepo.client.FcrepoClient;
import org.fcrepo.client.FcrepoOperationFailedException;
import org.fcrepo.client.FcrepoResponse;
import org.fcrepo.client.GetBuilder;
import org.fcrepo.client.HeadBuilder;
import org.fcrepo.client.PutBuilder;
import org.fcrepo.importexport.common.AuthenticationRequiredRuntimeException;
import org.fcrepo.importexport.common.Config;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

/**
 * @author escowles
 * @since 2016-09-06
 */
public class VersionImporterTest {

    private FcrepoClient client;
    private FcrepoClient.FcrepoClientBuilder clientBuilder;
    private Config binaryArgs;
    private Config noBinaryArgs;
    private Config externalResourceArgs;
    private Config containerArgs;
    private Config pairtreeArgs;
    private Config bagItArgs;
    private URI binaryURI;
    private URI binaryDescriptionURI;
    private URI externalResourceURI;
    private URI externalResourceDescriptionURI;
    private URI containerURI;
    private URI pairtreeURI;
    private URI finalContainerURI;
    private File binaryFilesDir;
    private File externalFilesDir;

    private FcrepoResponse conResponse;
    private PutBuilder binBuilder;
    private PutBuilder externalResourceBuilder;
    private GetBuilder getBuilder;
    private HeadBuilder headBuilder;
    private PutBuilder putBuilder;

    @Before
    public void setUp() throws Exception {
        binaryURI  = new URI("http://example.org:9999/rest/bin1");
        binaryDescriptionURI = new URI("http://example.org:9999/rest/bin1/fcr:metadata");
        externalResourceURI  = new URI("http://example.org:9999/rest/ext1");
        externalResourceDescriptionURI = new URI("http://example.org:9999/rest/ext1/fcr:metadata");
        containerURI = new URI("http://example.org:9999/rest/con1");

        binaryFilesDir = new File("src/test/resources/sample/binary");
        binaryArgs = new Config();
        binaryArgs.setMode("import");
        binaryArgs.setBaseDirectory("src/test/resources/sample/binary");
        binaryArgs.setIncludeBinaries(true);
        binaryArgs.setRdfLanguage("application/ld+json");
        binaryArgs.setResource(new URI("http://example.org:9999/rest"));
        binaryArgs.setMap(new String[]{"http://localhost:8080/rest", "http://example.org:9999/rest"});

        noBinaryArgs = new Config();
        noBinaryArgs.setMode("import");
        noBinaryArgs.setBaseDirectory("src/test/resources/sample/binary");
        noBinaryArgs.setIncludeBinaries(false);
        noBinaryArgs.setRdfLanguage("application/ld+json");
        noBinaryArgs.setResource(new URI("http://example.org:9999/rest"));
        noBinaryArgs.setMap(new String[]{"http://localhost:8080/rest", "http://example.org:9999/rest"});

        externalFilesDir = new File("src/test/resources/sample/external");
        externalResourceArgs = new Config();
        externalResourceArgs.setMode("import");
        externalResourceArgs.setBaseDirectory("src/test/resources/sample/external");
        externalResourceArgs.setIncludeBinaries(true);
        externalResourceArgs.setRdfExtension(".jsonld");
        externalResourceArgs.setRdfLanguage("application/ld+json");
        externalResourceArgs.setResource(new URI("http://example.org:9999/rest"));
        externalResourceArgs.setMap(new String[]{"http://localhost:8080/rest", "http://example.org:9999/rest"});

        containerArgs = new Config();
        containerArgs.setMode("import");
        containerArgs.setBaseDirectory("src/test/resources/sample/container");
        containerArgs.setRdfLanguage("application/ld+json");
        containerArgs.setResource(new URI("http://example.org:9999/rest/con1"));
        containerArgs.setMap(new String[]{"http://localhost:8080/rest", "http://example.org:9999/rest"});

        pairtreeArgs = new Config();
        pairtreeArgs.setMode("import");
        pairtreeArgs.setBaseDirectory("src/test/resources/sample/pairtree");
        pairtreeArgs.setRdfLanguage("application/ld+json");
        pairtreeArgs.setResource(new URI("http://example.org:9999/rest"));
        pairtreeArgs.setMap(new String[]{"http://localhost:8080/rest", "http://example.org:9999/rest"});

        bagItArgs = new Config();
        bagItArgs.setMode("import");
        bagItArgs.setBaseDirectory("src/test/resources/sample/bagcorrupted");
        bagItArgs.setIncludeBinaries(true);
        bagItArgs.setRdfLanguage("application/ld+json");
        bagItArgs.setResource(new URI("http://example.org:9999/rest"));
        bagItArgs.setMap(new String[] { "http://localhost:8080/rest", "http://example.org:9999/rest" });
        bagItArgs.setBagProfile("default");
        bagItArgs.setUsername("tester");

        pairtreeURI = new URI("http://example.org:9999/rest/ab");
        finalContainerURI = new URI("http://example.org:9999/rest/ab/abc123");

        final List<URI> binLinks = Arrays.asList(binaryDescriptionURI);
        final List<URI> externalResourceLinks = Arrays.asList(externalResourceDescriptionURI);

        // mocks
        clientBuilder = mock(FcrepoClient.FcrepoClientBuilder.class);
        client = mock(FcrepoClient.class);
        when(clientBuilder.build()).thenReturn(client);

        // mock head interactions
        headBuilder = mock(HeadBuilder.class);
        final FcrepoResponse headResponse = mock(FcrepoResponse.class);
        when(client.head(isA(URI.class))).thenReturn(headBuilder);
        when(headBuilder.disableRedirects()).thenReturn(headBuilder);
        when(headBuilder.perform()).thenReturn(headResponse);
        when(headResponse.getStatusCode()).thenReturn(200);

        // mock get container/description interactions
        getBuilder = mock(GetBuilder.class);
        final FcrepoResponse getResponse = mock(FcrepoResponse.class);
        when(client.get(isA(URI.class))).thenReturn(getBuilder);
        when(getBuilder.accept(isA(String.class))).thenReturn(getBuilder);
        when(getBuilder.disableRedirects()).thenReturn(getBuilder);
        when(getBuilder.perform()).thenReturn(getResponse);
        when(getResponse.getStatusCode()).thenReturn(200);
        when(getResponse.getBody()).thenReturn(
                new ByteArrayInputStream(("{\"@type\":[\"" + REPOSITORY_NAMESPACE + "RepositoryRoot\"]}").getBytes()))
                .thenReturn(new ByteArrayInputStream("{}".getBytes()));

        // mock binary interactions
        binBuilder = mock(PutBuilder.class);
        final FcrepoResponse binResponse = mock(FcrepoResponse.class);
        when(client.put(eq(binaryURI))).thenReturn(binBuilder);
        when(binBuilder.body(isA(InputStream.class), isA(String.class))).thenReturn(binBuilder);
        when(binBuilder.digest(isA(String.class))).thenReturn(binBuilder);
        when(binBuilder.perform()).thenReturn(binResponse);
        when(binResponse.getStatusCode()).thenReturn(201);
        when(binResponse.getLinkHeaders(eq("describedby"))).thenReturn(binLinks);

        // mock external resource interactions
        externalResourceBuilder = mock(PutBuilder.class);
        final FcrepoResponse externalResourceResponse = mock(FcrepoResponse.class);
        when(client.put(eq(externalResourceURI))).thenReturn(externalResourceBuilder);
        when(externalResourceBuilder.body(isA(InputStream.class), isA(String.class)))
                .thenReturn(externalResourceBuilder);
        when(externalResourceBuilder.perform()).thenReturn(externalResourceResponse);
        when(externalResourceResponse.getStatusCode()).thenReturn(201);
        when(externalResourceResponse.getLinkHeaders(eq("describedby"))).thenReturn(externalResourceLinks);


        // mock container/description interactions
        putBuilder = mock(PutBuilder.class);
        conResponse = mock(FcrepoResponse.class);
        when(client.put(eq(containerURI))).thenReturn(putBuilder);
        when(client.put(eq(pairtreeURI))).thenReturn(putBuilder);
        when(client.put(eq(finalContainerURI))).thenReturn(putBuilder);
        when(client.put(eq(binaryDescriptionURI))).thenReturn(putBuilder);
        when(client.put(eq(externalResourceDescriptionURI))).thenReturn(putBuilder);
        when(putBuilder.body(isA(InputStream.class), isA(String.class))).thenReturn(putBuilder);
        when(putBuilder.preferLenient()).thenReturn(putBuilder);
        when(putBuilder.perform()).thenReturn(conResponse);
        when(conResponse.getStatusCode()).thenReturn(201);
        when(conResponse.getLinkHeaders(eq("describedby"))).thenReturn(binLinks);
    }

    @Test
    public void testImportBinary() throws Exception {
        final VersionImporter importer = new VersionImporter(binaryArgs, clientBuilder);
        importer.run();
        verify(client).put(binaryURI);
        verify(binBuilder).digest(eq("2a6d6229e30f667c60d406f7bf44d834e52d11b7"));
        verify(binBuilder).body(isA(InputStream.class), eq("application/x-www-form-urlencoded"));
        verify(client).put(binaryDescriptionURI);
    }

    @Test
    public void testImportExternalResource() throws Exception {
        final VersionImporter importer = new VersionImporter(externalResourceArgs, clientBuilder);
        importer.run();
        verify(client).put(externalResourceURI);
        verify(externalResourceBuilder).body(isA(InputStream.class), eq("message/external-body"));
        verify(client).put(externalResourceDescriptionURI);
    }

    @Test
    public void testImportWithoutBinaries() throws Exception {
        final VersionImporter importer = new VersionImporter(noBinaryArgs, clientBuilder);
        importer.run();
        verify(client, never()).put(binaryURI);
        verify(binBuilder, never()).body(isA(InputStream.class), eq("application/x-www-form-urlencoded"));
        verify(client, never()).put(binaryDescriptionURI);
    }

    @Test
    public void testImportContainer() throws Exception {
        final VersionImporter importer = new VersionImporter(containerArgs, clientBuilder);
        importer.run();
        verify(client).put(containerURI);
    }

    @Test
    public void testLegacyModeStripsLastModified() throws Exception {
        containerArgs.setLegacy(true);
        final ArgumentCaptor<InputStream> streamCapture = ArgumentCaptor.forClass(InputStream.class);
        final VersionImporter importer = new VersionImporter(containerArgs, clientBuilder);
        importer.run();

        verify(putBuilder).body(streamCapture.capture(), isA(String.class));
        final Model model = createDefaultModel();
        model.read(streamCapture.getValue(), "", "JSON-LD");
        final Graph graph = model.getGraph();
        assertFalse(graph.contains(Node.ANY, LAST_MODIFIED_DATE.asNode(), Node.ANY));
    }

    @Test
    public void testDefaultModeRetainsLastModified() throws Exception {
        final ArgumentCaptor<InputStream> streamCapture = ArgumentCaptor.forClass(InputStream.class);
        final VersionImporter importer = new VersionImporter(containerArgs, clientBuilder);
        importer.run();

        verify(putBuilder).body(streamCapture.capture(), isA(String.class));
        final Model model = createDefaultModel();
        model.read(streamCapture.getValue(), "", "JSON-LD");
        final Graph graph = model.getGraph();
        assertTrue(graph.contains(Node.ANY, LAST_MODIFIED_DATE.asNode(), Node.ANY));
    }

    @Test (expected = AuthenticationRequiredRuntimeException.class)
    public void testUnauthenticatedImportWhenAuthorizationIsRequired() throws FcrepoOperationFailedException {
        when(conResponse.getStatusCode()).thenReturn(401);
        final VersionImporter importer = new VersionImporter(containerArgs, clientBuilder);
        importer.run();
    }

    @Test
    public void testSkipPairtree() throws Exception {
        final VersionImporter importer = new VersionImporter(pairtreeArgs, clientBuilder);
        importer.run();
        verify(client).put(finalContainerURI);
        verify(client, never()).put(pairtreeURI);
    }

    @Test
    public void testImportBagVerifyBinaryDigest() throws Exception {
        final URI badBinURI = new URI("http://example.org:9999/rest/bad_bin1");

        // mock bad binary interactions
        final PutBuilder badBinBuilder = mock(PutBuilder.class);
        final FcrepoResponse badBinResponse = mock(FcrepoResponse.class);
        when(client.put(isA(URI.class))).thenReturn(badBinBuilder);
        when(badBinBuilder.body(isA(InputStream.class), isA(String.class))).thenReturn(badBinBuilder);
        when(badBinBuilder.digest(isA(String.class))).thenReturn(badBinBuilder);
        when(badBinBuilder.perform()).thenReturn(badBinResponse);
        when(badBinResponse.getStatusCode()).thenReturn(409);
        when(badBinResponse.getBody()).thenReturn(new ByteArrayInputStream("Checksum Mismatch".getBytes()));

        final VersionImporter importer = new VersionImporter(bagItArgs, clientBuilder);
        importer.run();

        verify(client).put(badBinURI);

        // verify that the checksum from the manifest-sha1 file is used
        verify(badBinBuilder).digest(eq("c537ab534deef7493140106c2151eccf2a219b8e"));
    }
}
