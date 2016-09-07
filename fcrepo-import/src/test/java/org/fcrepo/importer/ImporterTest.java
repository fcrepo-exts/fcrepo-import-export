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
package org.fcrepo.importer;

import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.util.List;
import java.util.Arrays;

import org.fcrepo.client.FcrepoClient;
import org.fcrepo.client.FcrepoResponse;
import org.fcrepo.client.PutBuilder;
import org.fcrepo.importexport.Config;
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
    private Config containerArgs;
    private URI binaryURI;
    private URI binaryDescriptionURI;
    private URI containerURI;

    @Before
    public void setUp() throws Exception {
        binaryURI  = new URI("http://localhost:8080/rest/bin1");
        binaryDescriptionURI = new URI("http://localhost:8080/rest/bin1/fcr:metadata");
        containerURI = new URI("http://localhost:8080/rest/con1");
        binaryArgs = new Config();
        binaryArgs.setMode("import");
        binaryArgs.setDescriptionDirectory(new File("src/test/resources/sample/binary/rdf"));
        binaryArgs.setBinaryDirectory(new File("src/test/resources/sample/binarytarget/bin"));
        binaryArgs.setRdfExtension(".jsonld");
        binaryArgs.setRdfLanguage("application/ld+json");
        binaryArgs.setResource(new URI("http://localhost:8080/rest"));

        containerArgs = new Config();
        containerArgs.setMode("import");
        containerArgs.setDescriptionDirectory(new File("src/test/resources/sample/container"));
        containerArgs.setRdfExtension(".jsonld");
        containerArgs.setRdfLanguage("application/ld+json");
        containerArgs.setResource(new URI("http://localhost:8080/rest"));

        final List<URI> binLinks = (List<URI>)Arrays.asList(binaryDescriptionURI);

        // mocks
        clientBuilder = mock(FcrepoClient.FcrepoClientBuilder.class);
        client = mock(FcrepoClient.class);
        when(clientBuilder.build()).thenReturn(client);

        // mock binary interactions
        final PutBuilder binBuilder = mock(PutBuilder.class);
        final FcrepoResponse binResponse = mock(FcrepoResponse.class);
        when(client.put(eq(binaryURI))).thenReturn(binBuilder);
        when(binBuilder.body(isA(File.class), isA(String.class))).thenReturn(binBuilder);
        when(binBuilder.perform()).thenReturn(binResponse);
        when(binResponse.getStatusCode()).thenReturn(201);
        when(binResponse.getLinkHeaders(eq("describedby"))).thenReturn(binLinks);

        // mock container/description interactions
        final PutBuilder conBuilder = mock(PutBuilder.class);
        final FcrepoResponse conResponse = mock(FcrepoResponse.class);
        when(client.put(eq(containerURI))).thenReturn(conBuilder);
        when(client.put(eq(binaryDescriptionURI))).thenReturn(conBuilder);
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
        verify(client).put(binaryDescriptionURI);
    }

    @Test
    public void testImportContainer() throws Exception {
        final Importer importer = new Importer(containerArgs, clientBuilder);
        importer.run();
        verify(client).put(containerURI);
    }
}
