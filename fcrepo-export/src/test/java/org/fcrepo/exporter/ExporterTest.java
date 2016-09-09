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
package org.fcrepo.exporter;

import static org.fcrepo.importexport.FcrepoConstants.BINARY_EXTENSION;
import static org.fcrepo.importexport.FcrepoConstants.CONTAINER;
import static org.fcrepo.importexport.FcrepoConstants.CONTAINS;
import static org.fcrepo.importexport.FcrepoConstants.NON_RDF_SOURCE;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.fcrepo.client.FcrepoClient;
import org.fcrepo.client.FcrepoOperationFailedException;
import org.fcrepo.client.FcrepoResponse;
import org.fcrepo.client.GetBuilder;
import org.fcrepo.client.HeadBuilder;
import org.fcrepo.importexport.AuthenticationRequiredRuntimeException;
import org.fcrepo.importexport.Config;
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
    private List<URI> describedbyLinks;
    private URI resource;
    private URI resource2;
    private URI resource3;
    private URI resource4;
    private URI resource5;
    private Config args;
    private Config binaryArgs;
    private Config metadataArgs;

    @Before
    public void setUp() throws Exception {
        clientBuilder = mock(FcrepoClient.FcrepoClientBuilder.class);
        client = mock(FcrepoClient.class);
        when(clientBuilder.build()).thenReturn(client);

        headResponse = mock(FcrepoResponse.class);
        resource = new URI("http://localhost:8080/rest/1");
        resource2 = new URI("http://localhost:8080/rest/1/2");
        resource3 = new URI("http://localhost:8080/rest/file1");
        resource4 = new URI("http://localhost:8080/rest/file1/fcr:metadata");
        resource5 = new URI("http://localhost:8080/rest/alt_description");

        args = new Config();
        args.setMode("export");
        args.setDescriptionDirectory(new File("target/rdf"));
        args.setBinaryDirectory(new File("target/bin"));
        args.setRdfExtension(".jsonld");
        args.setRdfLanguage("application/ld+json");
        args.setResource(resource);

        binaryArgs = new Config();
        binaryArgs.setMode("export");
        binaryArgs.setDescriptionDirectory(new File("target/rdf"));
        binaryArgs.setBinaryDirectory(new File("target/bin"));
        binaryArgs.setRdfExtension(".jsonld");
        binaryArgs.setRdfLanguage("application/ld+json");
        binaryArgs.setResource(resource3);

        metadataArgs = new Config();
        metadataArgs.setMode("export");
        metadataArgs.setDescriptionDirectory(new File("target/rdf"));
        metadataArgs.setRdfExtension(".jsonld");
        metadataArgs.setRdfLanguage("application/ld+json");
        metadataArgs.setResource(resource);

        binaryLinks = Arrays.asList(new URI(NON_RDF_SOURCE.getURI()));
        containerLinks = Arrays.asList(new URI(CONTAINER.getURI()));
        describedbyLinks = Arrays.asList(new URI(resource4.toString()), new URI(resource5.toString()));

        mockResponse(resource, new ArrayList<>(), "{\"@id\":\"" + resource.toString() + "\",\""
                + CONTAINS.getURI() + "\":[{\"@id\":\"" + resource2.toString() + "\"}]}");
        mockResponse(resource2, new ArrayList<>(), "{\"@id\":\"" + resource2.toString() + "\"}");
        mockResponse(resource3, describedbyLinks, "binary");
        mockResponse(resource4, new ArrayList<>(), "{\"@id\":\"" + resource4.toString() + "\"}");
        mockResponse(resource5, new ArrayList<>(), "{\"@id\":\"" + resource5.toString() + "\"}");

        final HeadBuilder headBuilder = mock(HeadBuilder.class);
        when(client.head(isA(URI.class))).thenReturn(headBuilder);
        when(headBuilder.disableRedirects()).thenReturn(headBuilder);
        when(headBuilder.perform()).thenReturn(headResponse);
        when(headResponse.getStatusCode()).thenReturn(200);
    }

    private void mockResponse(final URI uri, final List<URI> describedbyLinks, final String body)
            throws FcrepoOperationFailedException {
        final GetBuilder getBuilder = mock(GetBuilder.class);
        final FcrepoResponse getResponse = mock(FcrepoResponse.class);
        when(client.get(eq(uri))).thenReturn(getBuilder);
        when(getBuilder.accept(isA(String.class))).thenReturn(getBuilder);
        when(getBuilder.disableRedirects()).thenReturn(getBuilder);
        when(getBuilder.perform()).thenReturn(getResponse);
        when(getResponse.getBody()).thenReturn(new ByteArrayInputStream(body.getBytes()));
        when(getResponse.getUrl()).thenReturn(uri);
        when(getResponse.getLinkHeaders(eq("describedby"))).thenReturn(describedbyLinks);
        when(getResponse.getStatusCode()).thenReturn(200);
    }

    @Test
    public void testExportBinaryAndDescription() throws Exception, FcrepoOperationFailedException {
        final ExporterWrapper exporter = new ExporterWrapper(binaryArgs, clientBuilder);
        when(headResponse.getLinkHeaders(eq("type"))).thenReturn(binaryLinks);
        when(headResponse.getContentType()).thenReturn("image/tiff");
        exporter.run();
        Assert.assertTrue(exporter.wroteFile(new File("target/bin/rest/file1" + BINARY_EXTENSION)));
        Assert.assertTrue(exporter.wroteFile(new File("target/rdf/rest/file1/fcr%3Ametadata.jsonld")));
        Assert.assertTrue(exporter.wroteFile(new File("target/rdf/rest/alt_description.jsonld")));
    }

    @Test
    public void textExternalContent() throws Exception {
        final ExporterWrapper exporter = new ExporterWrapper(binaryArgs, clientBuilder);
        when(headResponse.getLinkHeaders(eq("type"))).thenReturn(binaryLinks);
        when(headResponse.getLinkHeaders(eq("describedby"))).thenReturn(describedbyLinks);
        when(headResponse.getStatusCode()).thenReturn(307);
        when(headResponse.getContentType())
            .thenReturn("message/external-body;access-type=URL;url=\"http://www.example.com/file\"");
        exporter.run();
        Assert.assertTrue(exporter.wroteFile(new File("target/bin/rest/file1" + BINARY_EXTENSION)));
        Assert.assertTrue(exporter.wroteFile(new File("target/rdf/rest/file1/fcr%3Ametadata.jsonld")));
    }


    @Test
    public void testExportContainer() throws Exception {
        final ExporterWrapper exporter = new ExporterWrapper(args, clientBuilder);
        when(headResponse.getLinkHeaders(isA(String.class))).thenReturn(containerLinks);
        exporter.run();
        Assert.assertTrue(exporter.wroteFile(new File("target/rdf/rest/1.jsonld")));
    }

    @Test (expected = AuthenticationRequiredRuntimeException.class)
    public void testUnauthenticatedExportWhenAuthorizationIsRequired() {
        when(headResponse.getStatusCode()).thenReturn(401);
        final ExporterWrapper exporter = new ExporterWrapper(args, clientBuilder);
        exporter.run();
    }

    @Test
    public void testMetadataOnlyDoesNotExportBinaries() throws Exception {
        final ExporterWrapper exporter = new ExporterWrapper(metadataArgs, clientBuilder);
        when(headResponse.getLinkHeaders(isA(String.class))).thenReturn(binaryLinks);
        exporter.run();
        Assert.assertFalse(exporter.wroteFile(new File("/target/bin/rest/1")));
    }

    @Test
    public void testMetadataOnlyExportsContainers() throws Exception {
        final ExporterWrapper exporter = new ExporterWrapper(metadataArgs, clientBuilder);
        when(headResponse.getLinkHeaders(isA(String.class))).thenReturn(containerLinks);
        exporter.run();
        Assert.assertTrue(exporter.wroteFile(new File("target/rdf/rest/1.jsonld")));
    }

    @Test
    public void testRecursive() throws Exception {
        final ExporterWrapper exporter = new ExporterWrapper(args, clientBuilder);
        when(headResponse.getLinkHeaders(isA(String.class))).thenReturn(containerLinks);
        exporter.run();
        Assert.assertTrue(exporter.wroteFile(new File("target/rdf/rest/1.jsonld")));
        Assert.assertTrue(exporter.wroteFile(new File("target/rdf/rest/1/2.jsonld")));
    }
}

class ExporterWrapper extends Exporter {
    private List<File> writtenFiles = new ArrayList<>();

    ExporterWrapper(final Config config, final FcrepoClient.FcrepoClientBuilder clientBuilder) {
        super(config, clientBuilder);
    }
    void writeResponse(final FcrepoResponse response, final File file)
            throws IOException, FcrepoOperationFailedException {
        super.writeResponse(response, file);
        writtenFiles.add(file);
    }
    boolean wroteFile(final File file) {
        return writtenFiles.contains(file);
    }
}
