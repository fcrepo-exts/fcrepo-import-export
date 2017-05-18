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
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.jena.rdf.model.Model;
import org.fcrepo.client.FcrepoClient;
import org.fcrepo.client.FcrepoHttpClientBuilder;
import org.fcrepo.client.FcrepoOperationFailedException;
import org.fcrepo.client.FcrepoResponse;
import org.junit.Before;
import org.slf4j.Logger;

import static org.apache.http.HttpStatus.SC_GONE;
import static org.apache.http.HttpStatus.SC_NO_CONTENT;
import static org.apache.jena.rdf.model.ResourceFactory.createPlainLiteral;
import static org.apache.jena.rdf.model.ResourceFactory.createProperty;
import static org.apache.jena.rdf.model.ResourceFactory.createResource;
import static org.apache.jena.riot.RDFDataMgr.loadModel;
import static org.apache.jena.riot.web.HttpOp.setDefaultHttpClient;
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

    static final String DC_DATE = "http://purl.org/dc/elements/1.1/date";
    static final String DC_RELATION = "http://purl.org/dc/elements/1.1/relation";
    static final String DC_TITLE = "http://purl.org/dc/elements/1.1/title";
    static final String DCTERMS_HAS_PART = "http://purl.org/dc/terms/hasPart";
    static final String EBU_HAS_MIME_TYPE = "http://www.ebu.ch/metadata/ontologies/ebucore/ebucore#hasMimeType";
    static final String EDM_BEGIN = "http://www.europeana.eu/schemas/edm/begin";
    static final String EDM_END = "http://www.europeana.eu/schemas/edm/end";
    static final String EDM_TIMESPAN = "http://www.europeana.eu/schemas/edm/TimeSpan";
    static final String LDP_DIRECT_CONTAINER = "http://www.w3.org/ns/ldp#DirectContainer";
    static final String LDP_HAS_MEMBER_RELATION = "http://www.w3.org/ns/ldp#hasMemberRelation";
    static final String LDP_INDIRECT_CONTAINER = "http://www.w3.org/ns/ldp#IndirectContainer";
    static final String LDP_INSERTED_CONTENT_RELATION = "http://www.w3.org/ns/ldp#insertedContentRelation";
    static final String LDP_MEMBERSHIP_RESOURCE = "http://www.w3.org/ns/ldp#membershipResource";
    static final String LDP_NON_RDF_SOURCE = "http://www.w3.org/ns/ldp#NonRDFSource";
    static final String ORE_PROXY = "http://www.openarchives.org/ore/terms/Proxy";
    static final String ORE_PROXY_FOR = "http://www.openarchives.org/ore/terms/proxyFor";
    static final String PCDM_OBJECT = "http://pcdm.org/models#Object";
    static final String PREMIS_SIZE = "http://www.loc.gov/premis/rdf/v1#hasSize";
    static final String PREMIS_DIGEST = "http://www.loc.gov/premis/rdf/v1#hasMessageDigest";
    static final String SKOS_PREFLABEL = "http://www.w3.org/2004/02/skos/core#prefLabel";
    static final String XSD_DATETIME = "http://www.w3.org/2001/XMLSchema#dateTime";
    static final String PCDM_HAS_MEMBER = "http://pcdm.org/models#hasMember";

    static final String USERNAME = "fedoraAdmin";

    static final String PASSWORD = "password";

    static final String serverAddress = "http://" + HOSTNAME + ":" + SERVER_PORT + "/fcrepo/rest/";

    static final String TARGET_DIR = System.getProperty("project.build.directory");

    static final String DEFAULT_BAG_PROFILE = "default";

    static FcrepoClient.FcrepoClientBuilder clientBuilder;

    AbstractResourceIT() {
        clientBuilder = FcrepoClient.client().credentials(USERNAME, PASSWORD).authScope("localhost");
        setDefaultHttpClient(new FcrepoHttpClientBuilder(USERNAME, PASSWORD, "localhost").build());

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
        logger.debug("Create ---------: {}", uri);
        return clientBuilder.build().put(uri).perform();
    }

    protected FcrepoResponse createBody(final URI uri, final String body, final String contentType)
            throws FcrepoOperationFailedException {
        return createBody(uri, new ByteArrayInputStream(body.getBytes()), contentType);
    }

    protected FcrepoResponse createBody(final URI uri, final InputStream stream, final String contentType)
            throws FcrepoOperationFailedException {
        logger.debug("Create with binary: {}", uri);
        return clientBuilder.build().put(uri).body(stream, contentType).perform();
    }

    protected FcrepoResponse createTurtle(final URI uri, final String body)
            throws FcrepoOperationFailedException {
        return createBody(uri, body, "text/turtle");
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

    protected FcrepoResponse patch(final URI uri, final String command) throws FcrepoOperationFailedException {
        logger.debug("Patch: {}", uri);
        final InputStream stream = new ByteArrayInputStream(command.getBytes());
        return clientBuilder.build().patch(uri).body(stream).perform();
    }

    protected boolean exists(final URI uri) throws FcrepoOperationFailedException {
        final FcrepoResponse resp = clientBuilder.build().head(uri).disableRedirects().perform();
        return resp.getStatusCode() == 200 || resp.getStatusCode() == 307;
    }

    protected Model getAsModel(final URI uri) throws FcrepoOperationFailedException {
        return loadModel(uri.toString());
    }

    protected String getAsString(final URI uri) throws FcrepoOperationFailedException, IOException {
        final FcrepoResponse response = clientBuilder.build().get(uri).perform();
        return IOUtils.toString(response.getBody());
    }

    protected void assertHasTitle(final URI uri, final String title) throws FcrepoOperationFailedException {
        final Model model = getAsModel(uri);
        assertTrue(uri + " should have had the dc:title, \"" + title + "\"!",
                model.contains(createResource(uri.toString()), createProperty(DC_TITLE), createPlainLiteral(title)));
    }

    protected void removeAndReset(final URI uri) throws FcrepoOperationFailedException {
        final FcrepoResponse getResponse = remove(uri);
        final URI tombstone = getResponse.getLinkHeaders("hasTombstone").get(0);
        final FcrepoResponse delResponse = clientBuilder.build().delete(tombstone).perform();
        assertEquals("Failed to delete the tombstone!", SC_NO_CONTENT, delResponse.getStatusCode());
    }

    protected FcrepoResponse remove(final URI uri) throws FcrepoOperationFailedException {
        clientBuilder.build().delete(uri).perform();
        final FcrepoResponse getResponse = clientBuilder.build().get(uri).perform();
        assertEquals("Resource should have been deleted!", SC_GONE, getResponse.getStatusCode());
        return getResponse;
    }

    abstract protected Logger logger();
}
