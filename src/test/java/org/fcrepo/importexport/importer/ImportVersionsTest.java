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

import static org.fcrepo.importexport.common.FcrepoConstants.CONTAINER;
import static org.fcrepo.importexport.common.FcrepoConstants.CONTAINS;
import static org.fcrepo.importexport.common.FcrepoConstants.FCR_VERSIONS_PATH;
import static org.fcrepo.importexport.common.FcrepoConstants.NON_RDF_SOURCE;
import static org.fcrepo.importexport.common.FcrepoConstants.REPOSITORY_ROOT;
import static org.fcrepo.importexport.test.util.JsonLdResponse.createJson;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.fcrepo.client.FcrepoClient;
import org.fcrepo.client.FcrepoOperationFailedException;
import org.fcrepo.client.FcrepoResponse;
import org.fcrepo.client.HeadBuilder;
import org.fcrepo.client.PostBuilder;
import org.fcrepo.client.PutBuilder;
import org.fcrepo.importexport.common.Config;
import org.fcrepo.importexport.test.util.ResponseMocker;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

/**
 *
 * @author bbpennel
 *
 */
public class ImportVersionsTest {

    private URI rootResource;

    @Mock
    private FcrepoClient client;
    @Mock
    private FcrepoClient.FcrepoClientBuilder clientBuilder;
    @Mock
    private FcrepoResponse headResponse;
    @Mock
    private HeadBuilder headBuilder;
    @Mock
    private PutBuilder binBuilder;
    @Mock
    private PutBuilder containerBuilder;

    private Config config;

    private URI binaryURI;
    private URI binaryMDURI;
    private URI containerURI;
    private URI container2URI;

    private String[] predicates = new String[]{ CONTAINS.toString() };
    private List<URI> containerLinks =
            Arrays.asList(URI.create(CONTAINER.getURI()));
    private List<URI> binaryLinks =
            Arrays.asList(URI.create(NON_RDF_SOURCE.getURI()));

    @Before
    public void setUp() throws Exception {
        initMocks(this);

        containerURI  = new URI("http://localhost:8080/fcrepo/rest/con1");
        container2URI  = new URI("http://localhost:8080/fcrepo/rest/con1/con2");
        binaryURI  = new URI("http://localhost:8080/fcrepo/rest/con1/bin1");
        binaryMDURI  = new URI("http://localhost:8080/fcrepo/rest/con1/bin1/fcr:metadata");

        when(clientBuilder.build()).thenReturn(client);

        rootResource = new URI("http://localhost:8080/rest");
        mockResponse(rootResource, containerLinks, new ArrayList<>(),
                createJson(rootResource, REPOSITORY_ROOT));

        config = new Config();
        config.setMode("import");
        config.setIncludeVersions(true);
        config.setIncludeBinaries(true);
        config.setRdfLanguage("application/ld+json");
        config.setBaseDirectory("src/test/resources/sample/versioned");
        config.setResource(rootResource);

        final HeadBuilder headBuilder = mock(HeadBuilder.class);
        when(client.head(isA(URI.class))).thenReturn(headBuilder);
        when(headBuilder.disableRedirects()).thenReturn(headBuilder);
        when(headBuilder.perform()).thenReturn(headResponse);
        when(headResponse.getStatusCode()).thenReturn(200);
    }

    private void mockResponse(final URI uri, final List<URI> typeLinks, final List<URI> describedbyLinks,
            final String body) throws FcrepoOperationFailedException {
        ResponseMocker.mockHeadResponse(client, uri, typeLinks, describedbyLinks);

        ResponseMocker.mockGetResponse(client, uri, typeLinks, describedbyLinks, body);
    }

    @Test
    public void testImportVersionsOff() throws Exception {
        config.setIncludeVersions(false);

        final URI versionUri = URI.create(containerURI.toString() + "/" + FCR_VERSIONS_PATH);

        ResponseMocker.mockPutResponse(client, containerURI);
        ResponseMocker.mockPutResponse(client, container2URI);
        ResponseMocker.mockPutResponse(client, binaryURI);
        ResponseMocker.mockPutResponse(client, binaryMDURI);
        ResponseMocker.mockPostResponse(client, versionUri);

        final Importer importer = new Importer(config, clientBuilder);
        importer.run();

        // Container should only be updated for head version
        verify(client).put(eq(containerURI));
        verify(client, never()).put(eq(container2URI));
        verify(client).put(eq(binaryURI));
        // no versions should have been created
        verify(client, never()).post(versionUri);
    }

    @Test
    public void testImportVersions() throws Exception {
        final URI versionUri = URI.create(containerURI.toString() + "/" + FCR_VERSIONS_PATH);

        ResponseMocker.mockPutResponse(client, containerURI);
        ResponseMocker.mockPutResponse(client, container2URI);
        ResponseMocker.mockPutResponse(client, binaryURI);
        ResponseMocker.mockPutResponse(client, binaryMDURI);
        final PostBuilder versionBuilder = ResponseMocker.mockPostResponse(client, versionUri);

        final Importer importer = new Importer(config, clientBuilder);
        importer.run();

        // Container should have been updated for every version
        verify(client, times(3)).put(eq(containerURI));
        verify(client).put(eq(container2URI));
        verify(client, times(2)).put(eq(binaryURI));
        // Verify that the correct number of new versions were created
        verify(client, times(2)).post(versionUri);

        verify(versionBuilder).slug("version_1");
        verify(versionBuilder).slug("version_original");
    }

    @Test
    public void testVersionsOnWithoutVersionedResources() throws Exception {
        config.setBaseDirectory("src/test/resources/sample/container");

        containerURI  = new URI("http://localhost:8080/rest/con1");
        ResponseMocker.mockPutResponse(client, containerURI);
        final URI versionUri = URI.create(containerURI.toString() + "/" + FCR_VERSIONS_PATH);

        final Importer importer = new Importer(config, clientBuilder);
        importer.run();

        verify(client).put(eq(containerURI));
        // Ensuring that no versions are created
        verify(client, never()).post(versionUri);
    }

    @Test
    public void testBinarySameAcrossVersions() throws Exception {

    }

    @Test
    public void testBinaryDiffersAcrossVersions() throws Exception {

    }
}
