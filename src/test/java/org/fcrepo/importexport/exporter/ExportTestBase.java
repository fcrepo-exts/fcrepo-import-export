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

import org.apache.commons.io.FileUtils;
import org.fcrepo.client.FcrepoClient;
import org.fcrepo.client.FcrepoOperationFailedException;
import org.fcrepo.importexport.test.util.ResponseMocker;
import org.junit.After;
import org.junit.Before;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.fcrepo.importexport.common.FcrepoConstants.CONTAINER;
import static org.fcrepo.importexport.common.FcrepoConstants.CONTAINS;
import static org.fcrepo.importexport.common.FcrepoConstants.NON_RDF_SOURCE;
import static org.fcrepo.importexport.common.FcrepoConstants.RDF_SOURCE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ExportTestBase {

    // Fcrepo client and builder
    protected FcrepoClient client;
    protected FcrepoClient.FcrepoClientBuilder clientBuilder;

    // URI of the repository root
    protected URI rootResource;
    // URI of the resource to be exported
    protected URI resource;

    // Directory to export to
    protected String exportDirectory;

    // Links for different types of resources
    protected List<URI> binaryLinks = Arrays.asList(new URI(NON_RDF_SOURCE.getURI()));
    protected List<URI> containerLinks = Arrays.asList(new URI(CONTAINER.getURI()));
    protected List<URI> descriptionLinks = new ArrayList<>();
    protected List<URI> describedbyLinks = new ArrayList<>();
    protected String[] predicates = new String[]{ CONTAINS.toString() };

    protected List<File> tearDownFiles = new ArrayList<>();

    public ExportTestBase() throws URISyntaxException {
        // Needed to allow for the URISyntaxException thrown by new URI() above
    }

    @Before
    public void setUp() throws Exception{
        client = mock(FcrepoClient.class);
        clientBuilder = mock(FcrepoClient.FcrepoClientBuilder.class);
        when(clientBuilder.build()).thenReturn(client);
        rootResource = new URI("http://localhost:8080/rest");
        descriptionLinks.add(new URI(RDF_SOURCE.getURI()));
    }

    @After
    public void tearDown() {
        try {
            if (!tearDownFiles.isEmpty()) {
                for (File file : tearDownFiles) {
                    FileUtils.forceDelete(file);
                }
                tearDownFiles.clear();
            }
            FileUtils.deleteDirectory(new File(exportDirectory));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Mocks a response for a resource
     *
     * @param uri URI of the resource
     * @param typeLinks Link headers with rel="type" for the resource
     * @param describedbyLinks Link headers with rel="describedby" for the resource
     * @param body body of the response
     * @throws FcrepoOperationFailedException client failures
     */
    protected void mockResponse(final URI uri, final List<URI> typeLinks, final List<URI> describedbyLinks,
                              final String body) throws FcrepoOperationFailedException {
        mockResponse(uri, typeLinks, describedbyLinks, null, body, null);
    }

    /**
     * Mocks a response for a resource
     *
     * @param uri URI of the resource
     * @param typeLinks Link headers with rel="type" for the resource
     * @param describedbyLinks Link headers with rel="describedby" for the resource
     * @param aclLink Link headers with rel="acl" for the resource
     * @param body body of the response
     * @param contentType content type of the response
     * @throws FcrepoOperationFailedException client failures
     */
    protected void mockResponse(final URI uri, final List<URI> typeLinks, final List<URI> describedbyLinks,
                              final URI aclLink, final String body, final String contentType) throws FcrepoOperationFailedException {
        ResponseMocker.mockHeadResponse(client, uri, typeLinks, describedbyLinks, null, aclLink, contentType);
        ResponseMocker.mockGetResponse(client, uri, typeLinks, describedbyLinks,  null, aclLink, body, contentType);
    }

}
