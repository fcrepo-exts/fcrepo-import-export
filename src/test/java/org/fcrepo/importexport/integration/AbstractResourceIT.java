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
package org.fcrepo.importexport.integration;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.util.concurrent.TimeUnit;

import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.jena.query.Dataset;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.sparql.core.DatasetImpl;
import org.fcrepo.client.FcrepoClient;
import org.fcrepo.client.FcrepoOperationFailedException;
import org.fcrepo.client.FcrepoResponse;
import org.junit.Before;
import org.slf4j.Logger;

import static org.apache.http.HttpStatus.SC_OK;
import static org.apache.jena.graph.Node.ANY;
import static org.apache.jena.graph.NodeFactory.createLiteral;
import static org.apache.jena.graph.NodeFactory.createURI;
import static org.apache.jena.rdf.model.ModelFactory.createDefaultModel;
import static org.apache.jena.rdf.model.ResourceFactory.createProperty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author awoods
 * @since 2016-09-01
 */
abstract class AbstractResourceIT {

    private Logger logger = logger();

    private static final int SERVER_PORT = Integer.parseInt(System.getProperty("fcrepo.dynamic.test.port", "8080"));

    private static final String HOSTNAME = "localhost";

    static final String DC_TITLE = "http://purl.org/dc/elements/1.1/title";

    static final String USERNAME = "fedoraAdmin";

    static final String PASSWORD = "password";

    static final String serverAddress = "http://" + HOSTNAME + ":" + SERVER_PORT + "/fcrepo/rest/";

    static final String TARGET_DIR = System.getProperty("project.build.directory");

    static FcrepoClient.FcrepoClientBuilder clientBuilder;

    AbstractResourceIT() {
        clientBuilder = FcrepoClient.client().credentials(USERNAME, PASSWORD).authScope("localhost");

        final PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
        connectionManager.setMaxTotal(Integer.MAX_VALUE);
        connectionManager.setDefaultMaxPerRoute(20);
        connectionManager.closeIdleConnections(3, TimeUnit.SECONDS);
    }

    @Before
    public void before() {
        assertNotNull(TARGET_DIR);
        assertTrue(new File(TARGET_DIR).exists());
    }

    protected FcrepoResponse create(final URI uri) throws FcrepoOperationFailedException {
        logger.debug("Request ------: {}", uri);
        return clientBuilder.build().put(uri).perform();
    }

    protected InputStream insertTitle(final String title) {
        try {
            return new ByteArrayInputStream(("INSERT DATA { <> <" + DC_TITLE + "> '" + title + "' . }")
                    .getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e) {
            // can't actually happen
            throw new RuntimeException(e);
        }
    }

    protected void assertHasTitle(final URI url, final String title) throws FcrepoOperationFailedException {
        final FcrepoResponse getResponse = clientBuilder.build().get(url).accept("application/n-triples").perform();
        assertEquals("GET of " + url + " failed!", SC_OK, getResponse.getStatusCode());
        final Model model = createDefaultModel();
        final Dataset d = new DatasetImpl(model.read(getResponse.getBody(), "", "application/n-triples"));

        assertTrue(url + " should have had the dc:title, \"" + title + "\"!",
                d.asDatasetGraph().contains(ANY, createURI(url.toString()),
                        createProperty(DC_TITLE).asNode(), createLiteral(title)));
    }

    abstract protected Logger logger();
}
