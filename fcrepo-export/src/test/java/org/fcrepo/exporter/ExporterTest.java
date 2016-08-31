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

import static org.fcrepo.kernel.api.RdfLexicon.CONTAINER;
import static org.fcrepo.kernel.api.RdfLexicon.CONTAINS;
import static org.fcrepo.kernel.api.RdfLexicon.NON_RDF_SOURCE;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;

import org.fcrepo.client.FcrepoClient;
import org.fcrepo.client.FcrepoOperationFailedException;
import org.fcrepo.client.FcrepoResponse;
import org.fcrepo.client.HeadBuilder;
import org.fcrepo.client.GetBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author escowles
 * @since 2016-08-30
 */
public class ExporterTest {

    private FcrepoClient client;
    private FcrepoResponse headResponse;
    private List<URI> binaryLinks;
    private List<URI> containerLinks;
    private URI resource;
    private URI resource2;
    private String[] args;
    private String[] metadataArgs;

    @Before
    public void setUp() throws Exception {
        client = mock(FcrepoClient.class);
        headResponse = mock(FcrepoResponse.class);
        resource = new URI("http://localhost:8080/rest/1");
        resource2 = new URI("http://localhost:8080/rest/1/2");
        args = new String[]{"-m", "export",
                            "-d", "target/rdf",
                            "-b", "target/bin",
                            "-x", ".jsonld",
                            "-l", "application/ld+json",
                            "-r", resource.toString()};

        metadataArgs = new String[]{"-m", "export",
                                    "-d", "target/rdf",
                                    "-x", ".jsonld",
                                    "-l", "application/ld+json",
                                    "-r", resource.toString()};

        binaryLinks = (List<URI>)Arrays.asList(new URI(NON_RDF_SOURCE.getURI()));
        containerLinks = (List<URI>)Arrays.asList(new URI(CONTAINER.getURI()));

        mockResponse(resource, "{\"@id\":\"" + resource.toString() + "\",\"" + CONTAINS.getURI()
                + "\":[{\"@id\":\"" + resource2.toString() + "\"}]}");
        mockResponse(resource2, "{\"@id\":\"" + resource2.toString() + "\"}");

        final HeadBuilder headBuilder = mock(HeadBuilder.class);
        when(client.head(isA(URI.class))).thenReturn(headBuilder);
        when(headBuilder.perform()).thenReturn(headResponse);
    }

    private void mockResponse(final URI uri, final String body) throws FcrepoOperationFailedException {
        final GetBuilder getBuilder = mock(GetBuilder.class);
        final FcrepoResponse getResponse = mock(FcrepoResponse.class);
        when(client.get(eq(uri))).thenReturn(getBuilder);
        when(getBuilder.accept(isA(String.class))).thenReturn(getBuilder);
        when(getBuilder.perform()).thenReturn(getResponse);
        when(getResponse.getBody()).thenReturn(new ByteArrayInputStream(body.getBytes()));
    }

    @Test
    public void testExportBinary() throws Exception {
        final ExporterWrapper exporter = new ExporterWrapper(new ArgParser().parseConfiguration(args), client);
        when(headResponse.getLinkHeaders(isA(String.class))).thenReturn(binaryLinks);
        exporter.run();
        Assert.assertTrue(exporter.wroteFile(new File("target/bin/rest/1")));
    }

    @Test
    public void testExportContainer() throws Exception {
        final ExporterWrapper exporter = new ExporterWrapper(new ArgParser().parseConfiguration(args), client);
        when(headResponse.getLinkHeaders(isA(String.class))).thenReturn(containerLinks);
        exporter.run();
        Assert.assertTrue(exporter.wroteFile(new File("target/rdf/rest/1.jsonld")));
    }

    @Test
    public void testMetadataOnlyDoesNotExportBinaries() throws Exception {
        final ExporterWrapper exporter = new ExporterWrapper(new ArgParser().parseConfiguration(metadataArgs), client);
        when(headResponse.getLinkHeaders(isA(String.class))).thenReturn(binaryLinks);
        exporter.run();
        Assert.assertFalse(exporter.wroteFile(new File("/target/bin/rest/1")));
    }

    @Test
    public void testMetadataOnlyExportsContainers() throws Exception {
        final ExporterWrapper exporter = new ExporterWrapper(new ArgParser().parseConfiguration(metadataArgs), client);
        when(headResponse.getLinkHeaders(isA(String.class))).thenReturn(containerLinks);
        exporter.run();
        Assert.assertTrue(exporter.wroteFile(new File("target/rdf/rest/1.jsonld")));
    }

    @Test
    public void testRecursive() throws Exception {
        final ExporterWrapper exporter = new ExporterWrapper(new ArgParser().parseConfiguration(args), client);
        when(headResponse.getLinkHeaders(isA(String.class))).thenReturn(containerLinks);
        exporter.run();
        Assert.assertTrue(exporter.wroteFile(new File("target/rdf/rest/1.jsonld")));
        Assert.assertTrue(exporter.wroteFile(new File("target/rdf/rest/1/2.jsonld")));
    }
}

class ExporterWrapper extends Exporter {
    private List<File> writtenFiles = new ArrayList<>();

    ExporterWrapper(final Config config, final FcrepoClient client) {
        super(config);
        this.client = client;
    }
    void writeResponse(final FcrepoResponse response, final File file) throws IOException {
        super.writeResponse(response, file);
        writtenFiles.add(file);
    }
    boolean wroteFile(final File file) {
        return writtenFiles.contains(file);
    }
}
