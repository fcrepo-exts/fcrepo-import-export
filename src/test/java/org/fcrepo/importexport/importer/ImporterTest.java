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

import org.fcrepo.client.FcrepoClient;
import org.fcrepo.client.FcrepoOperationFailedException;
import org.fcrepo.client.FcrepoResponse;
import org.fcrepo.client.GetBuilder;
import org.fcrepo.client.PutBuilder;
import org.fcrepo.importexport.common.AuthenticationRequiredRuntimeException;
import org.fcrepo.importexport.common.Config;

import org.junit.Before;
import org.junit.Test;

/**
 * @author escowles
 * @since 2016-09-06
 */
public class ImporterTest {

    private FcrepoClient client;
    private FcrepoClient.FcrepoClientBuilder clientBuilder;
    private Config binaryArgs;
    private Config noBinaryArgs;
    private Config externalResourceArgs;
    private Config containerArgs;
    private Config pairtreeArgs;
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

        pairtreeURI = new URI("http://example.org:9999/rest/ab");
        finalContainerURI = new URI("http://example.org:9999/rest/ab/abc123");

        final List<URI> binLinks = Arrays.asList(binaryDescriptionURI);
        final List<URI> externalResourceLinks = Arrays.asList(externalResourceDescriptionURI);

        // mocks
        clientBuilder = mock(FcrepoClient.FcrepoClientBuilder.class);
        client = mock(FcrepoClient.class);
        when(clientBuilder.build()).thenReturn(client);

        // mock get container/description interactions
        getBuilder = mock(GetBuilder.class);
        final FcrepoResponse getResponse = mock(FcrepoResponse.class);
        when(client.get(isA(URI.class))).thenReturn(getBuilder);
        when(getBuilder.accept(isA(String.class))).thenReturn(getBuilder);
        when(getBuilder.disableRedirects()).thenReturn(getBuilder);
        when(getBuilder.perform()).thenReturn(getResponse);
        when(getResponse.getStatusCode()).thenReturn(200);
        when(getResponse.getBody()).thenReturn(new ByteArrayInputStream("{}".getBytes()));

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
        final PutBuilder conBuilder = mock(PutBuilder.class);
        conResponse = mock(FcrepoResponse.class);
        when(client.put(eq(containerURI))).thenReturn(conBuilder);
        when(client.put(eq(pairtreeURI))).thenReturn(conBuilder);
        when(client.put(eq(finalContainerURI))).thenReturn(conBuilder);
        when(client.put(eq(binaryDescriptionURI))).thenReturn(conBuilder);
        when(client.put(eq(externalResourceDescriptionURI))).thenReturn(conBuilder);
        when(conBuilder.body(isA(InputStream.class), isA(String.class))).thenReturn(conBuilder);
        when(conBuilder.preferLenient()).thenReturn(conBuilder);
        when(conBuilder.perform()).thenReturn(conResponse);
        when(conResponse.getStatusCode()).thenReturn(201);
        when(conResponse.getLinkHeaders(eq("describedby"))).thenReturn(binLinks);
    }

    @Test
    public void testImportBinary() throws Exception {
        final Importer importer = new Importer(binaryArgs, clientBuilder);
        importer.run();
        verify(client).put(binaryURI);
        verify(binBuilder).digest(eq("2a6d6229e30f667c60d406f7bf44d834e52d11b7"));
        verify(binBuilder).body(isA(InputStream.class), eq("application/x-www-form-urlencoded"));
        verify(client).put(binaryDescriptionURI);
    }

    @Test
    public void testImportExternalResource() throws Exception {
        final Importer importer = new Importer(externalResourceArgs, clientBuilder);
        importer.run();
        verify(client).put(externalResourceURI);
        verify(externalResourceBuilder).body(isA(InputStream.class), eq("message/external-body"));
        verify(client).put(externalResourceDescriptionURI);
    }

    @Test
    public void testImportWithoutBinaries() throws Exception {
        final Importer importer = new Importer(noBinaryArgs, clientBuilder);
        importer.run();
        verify(client, never()).put(binaryURI);
        verify(binBuilder, never()).body(isA(InputStream.class), eq("application/x-www-form-urlencoded"));
        verify(client, never()).put(binaryDescriptionURI);
    }

    @Test
    public void testImportContainer() throws Exception {
        final Importer importer = new Importer(containerArgs, clientBuilder);
        importer.run();
        verify(client).put(containerURI);
    }

    @Test (expected = AuthenticationRequiredRuntimeException.class)
    public void testUnauthenticatedImportWhenAuthorizationIsRequired() throws FcrepoOperationFailedException {
        when(conResponse.getStatusCode()).thenReturn(401);
        final Importer importer = new Importer(containerArgs, clientBuilder);
        importer.run();
    }

    @Test
    public void testSkipPairtree() throws Exception {
        final Importer importer = new Importer(pairtreeArgs, clientBuilder);
        importer.run();
        verify(client).put(finalContainerURI);
        verify(client, never()).put(pairtreeURI);
    }
}
