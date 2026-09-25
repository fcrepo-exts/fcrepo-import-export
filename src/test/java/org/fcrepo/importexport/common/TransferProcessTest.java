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
package org.fcrepo.importexport.common;

import static java.net.URI.create;
import static org.fcrepo.importexport.common.TransferProcess.fileForURI;
import static org.fcrepo.importexport.common.FcrepoConstants.CONTAINER;
import static org.fcrepo.importexport.common.FcrepoConstants.REPOSITORY_NAMESPACE;
import static org.junit.Assert.assertEquals;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.fcrepo.importexport.common.FcrepoConstants.NON_RDF_SOURCE;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.net.URI;
import java.util.Collections;

import org.fcrepo.client.FcrepoClient;
import org.fcrepo.client.FcrepoResponse;
import org.fcrepo.client.GetBuilder;
import org.fcrepo.client.HeadBuilder;
import org.junit.Test;

/**
 * @author escowles
 * @since 2016-12-14
 */
public class TransferProcessTest {

    private File dir = new File("/export/dir/");
    private String ext = ".ttl";

    private FcrepoClient client;

    @Test
    public void testMappingNull() throws Exception {
        final URI uri = create("http://localhost:8080/rest/foo");
        assertEquals(new File(dir, "rest/foo" + ext), fileForURI(uri, null, null, dir, ext));
    }

    @Test
    public void testMappingBase() throws Exception {
        final URI uri = create("http://localhost:8080/fedora/rest/foo");
        assertEquals(new File(dir, "rest/foo" + ext), fileForURI(uri, "/rest", "/fedora/rest", dir, ext));
    }

    @Test
    public void testMappingPath() throws Exception {
        final URI uri = create("http://localhost:8888/rest/prod/foo");
        assertEquals(new File(dir, "rest/dev/foo" + ext), fileForURI(uri, "/rest/dev", "/rest/prod", dir, ext));
    }

    @Test
    public void testMappingRest() throws Exception {
        final URI uri = create("http://localhost:8080/rest/foo");
        assertEquals(new File(dir, "rest/foo" + ext), fileForURI(uri, "/rest", "/rest", dir, ext));
    }

    @Test
    public void testMappingRestless() throws Exception {
        final URI uri = create("http://localhost:8080/rest/foo");
        assertEquals(new File(dir, "foo" + ext), fileForURI(uri, "/", "/rest/", dir, ext));
    }

    @Test
    public void testMappingRestless2() throws Exception {
        final URI uri = create("http://localhost:8080/foo");
        assertEquals( new File(dir, "rest/foo" + ext), fileForURI(uri, "/rest/", "/", dir, ext));
    }

    @Test
    public void testIsrepositoryRoot() throws Exception {
        final String rdfLanguage = "application/ld+json";
        final Config config = mock(Config.class);
        final URI uri = URI.create("http://localhost:8080/fcrepo");
        client = mock(FcrepoClient.class);

        final HeadBuilder headBuilder = mock(HeadBuilder.class);
        final FcrepoResponse headResponse = mock(FcrepoResponse.class);
        when(client.head(eq(uri))).thenReturn(headBuilder);
        when(headBuilder.disableRedirects()).thenReturn(headBuilder);
        when(headBuilder.perform()).thenReturn(headResponse);
        when(headResponse.getStatusCode()).thenReturn(200);

        final GetBuilder getBuilder = mock(GetBuilder.class);
        final FcrepoResponse getResponse = mock(FcrepoResponse.class);
        when(config.getRdfLanguage()).thenReturn(rdfLanguage);
        when(client.get(isA(URI.class))).thenReturn(getBuilder);
        when(getBuilder.accept(isA(String.class))).thenReturn(getBuilder);
        when(getBuilder.disableRedirects()).thenReturn(getBuilder);
        when(getBuilder.perform()).thenReturn(getResponse);
        when(getResponse.getBody()).thenReturn(
                new ByteArrayInputStream(("{\"@type\":[\"" + CONTAINER + "\"]}").getBytes()));
        assertFalse(TransferProcess.isRepositoryRoot(uri, client, config));

        when(getResponse.getBody()).thenReturn(new ByteArrayInputStream(
                ("{\"@type\":[\"" + REPOSITORY_NAMESPACE + "RepositoryRoot\"]}").getBytes()));
        assertTrue(TransferProcess.isRepositoryRoot(uri, client, config));
    }

    @Test
    public void testMappingSourceOnly() throws Exception {
        final URI uri = create("http://localhost:8080/rest/foo");
        assertEquals(new File(dir, "rest/foo" + ext), fileForURI(uri, "/other", null, dir, ext));
    }

    @Test
    public void testMappingTrailingSlash() throws Exception {
        final URI uri = create("http://localhost:8080/rest/foo/");
        assertEquals(new File(dir, "rest/foo" + ext), fileForURI(uri, null, "/rest", dir, ext));
    }

    @Test
    public void testEncodeDecodePath() {
        final String path = "/rest/a:b c";
        assertEquals("/rest/a%3Ab+c", TransferProcess.encodePath(path));
        assertEquals(path, TransferProcess.decodePath(TransferProcess.encodePath(path)));
    }

    @Test
    public void testCheckValidResponse() {
        final URI uri = create("http://localhost:8080/rest/foo");
        TransferProcess.checkValidResponse(responseWithStatus(200), uri, "user");
        TransferProcess.checkValidResponse(responseWithStatus(307), uri, "user");
        assertThrows(AuthenticationRequiredRuntimeException.class,
                () -> TransferProcess.checkValidResponse(responseWithStatus(401), uri, "user"));
        final AuthorizationDeniedRuntimeException denied = assertThrows(AuthorizationDeniedRuntimeException.class,
                () -> TransferProcess.checkValidResponse(responseWithStatus(403), uri, "user"));
        assertEquals("authorization denied for " + uri + " by user", denied.getMessage());
        assertThrows(ResourceNotFoundRuntimeException.class,
                () -> TransferProcess.checkValidResponse(responseWithStatus(404), uri, "user"));
        assertThrows(TombstoneFoundException.class,
                () -> TransferProcess.checkValidResponse(responseWithStatus(410), uri, "user"));
        assertThrows(RuntimeException.class,
                () -> TransferProcess.checkValidResponse(responseWithStatus(500), uri, "user"));
        assertThrows(RuntimeException.class,
                () -> TransferProcess.checkValidResponse(responseWithStatus(100), uri, "user"));
    }

    @Test
    public void testIsRepositoryRootBinary() throws Exception {
        final URI uri = URI.create("http://localhost:8080/fcrepo/bin");
        client = mock(FcrepoClient.class);
        final HeadBuilder headBuilder = mock(HeadBuilder.class);
        final FcrepoResponse headResponse = responseWithStatus(200);
        when(client.head(eq(uri))).thenReturn(headBuilder);
        when(headBuilder.disableRedirects()).thenReturn(headBuilder);
        when(headBuilder.perform()).thenReturn(headResponse);
        when(headResponse.getLinkHeaders("type"))
                .thenReturn(Collections.singletonList(URI.create(NON_RDF_SOURCE.getURI())));
        assertFalse(TransferProcess.isRepositoryRoot(uri, client, new Config()));
    }

    private static FcrepoResponse responseWithStatus(final int status) {
        final FcrepoResponse response = mock(FcrepoResponse.class);
        when(response.getStatusCode()).thenReturn(status);
        return response;
    }
}
