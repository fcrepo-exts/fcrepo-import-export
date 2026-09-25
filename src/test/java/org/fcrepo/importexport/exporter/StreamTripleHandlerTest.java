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

import static org.apache.jena.graph.NodeFactory.createLiteral;
import static org.apache.jena.graph.NodeFactory.createURI;
import static org.fcrepo.importexport.common.FcrepoConstants.CONTAINER;
import static org.fcrepo.importexport.common.FcrepoConstants.CONTAINS;
import static org.fcrepo.importexport.common.FcrepoConstants.NON_RDF_SOURCE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.Quad;
import org.fcrepo.client.FcrepoClient;
import org.fcrepo.client.FcrepoOperationFailedException;
import org.fcrepo.client.FcrepoResponse;
import org.fcrepo.client.HeadBuilder;
import org.fcrepo.importexport.common.Config;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * @author dfield
 */
public class StreamTripleHandlerTest {

    private static final URI RESOURCE = URI.create("http://localhost:8080/rest/parent");
    private static final Node RESOURCE_NODE = createURI(RESOURCE.toString());
    private static final URI CHILD = URI.create("http://localhost:8080/rest/parent/child");
    private static final Node TITLE = createURI("http://purl.org/dc/elements/1.1/title");

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    private Config config;
    private Exporter exporter;
    private FcrepoClient client;
    private HeadBuilder headBuilder;
    private FcrepoResponse headResponse;
    private File file;

    @Before
    public void setUp() throws Exception {
        config = new Config();
        config.setRdfLanguage("application/n-triples");
        config.setIncludeBinaries(true);
        exporter = mock(Exporter.class);
        client = mock(FcrepoClient.class);
        headBuilder = mock(HeadBuilder.class);
        headResponse = mock(FcrepoResponse.class);
        when(client.head(any(URI.class))).thenReturn(headBuilder);
        when(headBuilder.disableRedirects()).thenReturn(headBuilder);
        when(headBuilder.perform()).thenReturn(headResponse);
        when(headResponse.getStatusCode()).thenReturn(200);
        file = new File(tmp.getRoot(), "nested/dir/parent.nt");
    }

    private StreamTripleHandler handler() {
        return new StreamTripleHandler(config, exporter, client).setResource(URI.create(RESOURCE + "/"))
                .setFile(file);
    }

    private String output() throws IOException {
        return new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);
    }

    @Test
    public void testWritesTriplesAndExportsChildren() throws Exception {
        final StreamTripleHandler handler = handler();
        handler.start();
        handler.base("http://localhost:8080/rest/");
        handler.prefix("dc", "http://purl.org/dc/elements/1.1/");
        handler.triple(Triple.create(RESOURCE_NODE, TITLE, createLiteral("title")));
        handler.quad(Quad.create(Quad.defaultGraphIRI, RESOURCE_NODE, CONTAINS.asNode(),
                createURI(CHILD.toString() + "/")));
        handler.finish();

        final String output = output();
        assertTrue(output.contains("\"title\""));
        assertTrue(output.contains("<" + CHILD + "/>"));
        verify(exporter).generateChecksums(file);
        verify(exporter).export(CHILD);
    }

    @Test
    public void testSkipsBinaryChildren() throws Exception {
        config.setIncludeBinaries(false);
        when(headResponse.getLinkHeaders("type"))
                .thenReturn(Collections.singletonList(URI.create(NON_RDF_SOURCE.getURI())));

        final StreamTripleHandler handler = handler();
        handler.start();
        handler.triple(Triple.create(RESOURCE_NODE, CONTAINS.asNode(), createURI(CHILD.toString())));
        handler.finish();

        assertEquals("", output());
        verify(exporter, never()).export(any());
    }

    @Test
    public void testExportsNonBinaryChildrenWhenExcludingBinaries() throws Exception {
        config.setIncludeBinaries(false);
        when(headResponse.getLinkHeaders("type"))
                .thenReturn(Collections.singletonList(URI.create(CONTAINER.getURI())));

        final StreamTripleHandler handler = handler();
        handler.start();
        handler.triple(Triple.create(RESOURCE_NODE, CONTAINS.asNode(), createURI(CHILD.toString())));
        handler.finish();

        verify(exporter).export(CHILD);
    }

    @Test
    public void testBinaryCheckFailureStillExports() throws Exception {
        config.setIncludeBinaries(false);
        when(headBuilder.perform()).thenThrow(new FcrepoOperationFailedException(CHILD, 500, "error"));

        final StreamTripleHandler handler = handler();
        handler.start();
        handler.triple(Triple.create(RESOURCE_NODE, CONTAINS.asNode(), createURI(CHILD.toString())));
        handler.finish();

        verify(exporter).export(CHILD);
    }

    @Test
    public void testInboundReferenceSkipped() throws Exception {
        final Node other = createURI("http://localhost:8080/rest/other");
        final StreamTripleHandler handler = handler();
        handler.start();
        handler.triple(Triple.create(other, TITLE, RESOURCE_NODE));
        handler.finish();

        assertEquals("", output());
        verify(exporter, never()).export(any());
    }

    @Test
    public void testInboundReferenceRetrieved() throws Exception {
        config.setRetrieveInbound(true);
        final URI other = URI.create("http://localhost:8080/rest/other");
        final StreamTripleHandler handler = handler();
        handler.start();
        handler.triple(Triple.create(createURI(other.toString()), TITLE, RESOURCE_NODE));
        handler.finish();

        assertTrue(output().contains("<" + other + ">"));
        verify(exporter).export(eq(other));
    }

    @Test
    public void testStartWithoutFile() {
        final StreamTripleHandler handler = new StreamTripleHandler(config, exporter, client).setResource(RESOURCE);
        handler.start();
        handler.finish();
        verify(exporter, never()).generateChecksums(any());
    }

    @Test
    public void testStartWithoutResource() {
        final StreamTripleHandler handler = new StreamTripleHandler(config, exporter, client).setFile(file);
        handler.start();
        handler.finish();
        assertFalse(file.exists());
        verify(exporter, never()).generateChecksums(any());
    }

    @Test
    public void testStartWithUnwritableFile() throws Exception {
        final File directory = tmp.newFolder("existing");
        final StreamTripleHandler handler = new StreamTripleHandler(config, exporter, client).setResource(RESOURCE)
                .setFile(directory);
        handler.start();
        handler.finish();
        verify(exporter, never()).generateChecksums(any());
    }

    @Test
    public void testFinishWhenFileRemoved() throws Exception {
        final StreamTripleHandler handler = handler();
        handler.start();
        Files.delete(file.toPath());
        handler.finish();
        verify(exporter, never()).generateChecksums(any());
    }
}
