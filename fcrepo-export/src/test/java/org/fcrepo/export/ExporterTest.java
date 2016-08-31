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
package org.fcrepo.export;

import static org.fcrepo.kernel.api.RdfLexicon.CONTAINER;
import static org.fcrepo.kernel.api.RdfLexicon.NON_RDF_SOURCE;
import static org.mockito.Mockito.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;

import java.io.File;
import java.net.URI;
import java.util.List;
import java.util.Arrays;

import org.fcrepo.client.FcrepoClient;
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

    private ExporterWrapper exporter;
    private FcrepoClient client;
    private FcrepoResponse getResponse;
    private FcrepoResponse headResponse;
    private List<URI> binaryLinks;
    private List<URI> containerLinks;
    private URI resource;

    @Before
    public void setUp() throws Exception {
        client = mock(FcrepoClient.class);
        getResponse = mock(FcrepoResponse.class);
        headResponse = mock(FcrepoResponse.class);
        resource = new URI("http://localhost:8080/rest/1");
        final String[] args = new String[]{"-m", "export",
                                           "-d", "/tmp/rdf",
                                           "-b", "/tmp/bin",
                                           "-x", ".jsonld",
                                           "-l", "application/ld+json",
                                           "-r", resource.toString()};
        exporter = new ExporterWrapper(new ArgParser().parse(args), client);

        binaryLinks = (List<URI>)Arrays.asList(new URI(NON_RDF_SOURCE.getURI()));
        containerLinks = (List<URI>)Arrays.asList(new URI(CONTAINER.getURI()));

        final GetBuilder getBuilder = mock(GetBuilder.class);
        when(client.get(isA(URI.class))).thenReturn(getBuilder);
        when(getBuilder.accept(isA(String.class))).thenReturn(getBuilder);
        when(getBuilder.perform()).thenReturn(getResponse);

        final HeadBuilder headBuilder = mock(HeadBuilder.class);
        when(client.head(isA(URI.class))).thenReturn(headBuilder);
        when(headBuilder.perform()).thenReturn(headResponse);
    }

    @Test
    public void testExportBinary() throws Exception {
        when(headResponse.getLinkHeaders(isA(String.class))).thenReturn(binaryLinks);
        exporter.run();
        Assert.assertEquals(new File("/tmp/bin/rest/1"), exporter.writtenFile);
    }

    @Test
    public void testExportContainer() throws Exception {
        when(headResponse.getLinkHeaders(isA(String.class))).thenReturn(containerLinks);
        exporter.run();
        Assert.assertEquals(new File("/tmp/rdf/rest/1.jsonld"), exporter.writtenFile);
    }
}

class ExporterWrapper extends Exporter {
    public File writtenFile = null;

    ExporterWrapper(final Config config, final FcrepoClient client) {
        super(config);
        this.client = client;
    }
    void writeResponse(final FcrepoResponse response, final File file) {
        writtenFile = file;
    }
}
