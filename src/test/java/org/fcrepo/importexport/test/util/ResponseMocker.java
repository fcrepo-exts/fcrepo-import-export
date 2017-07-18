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
package org.fcrepo.importexport.test.util;

import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
import java.util.List;

import org.fcrepo.client.FcrepoClient;
import org.fcrepo.client.FcrepoOperationFailedException;
import org.fcrepo.client.FcrepoResponse;
import org.fcrepo.client.GetBuilder;
import org.fcrepo.client.HeadBuilder;
import org.fcrepo.client.PostBuilder;
import org.fcrepo.client.PutBuilder;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Test utility for common response mocking behaviors
 * 
 * @author bbpennel
 *
 */
public abstract class ResponseMocker {

    /**
     * Mocks a successful HEAD request response
     * 
     * @param client client
     * @param uri uri of destination being mocked
     * @param typeLinks type links
     * @param describedbyLinks described by links
     * @throws FcrepoOperationFailedException client failures
     */
    public static HeadBuilder mockHeadResponse(final FcrepoClient client, final URI uri, final List<URI> typeLinks,
            final List<URI> describedbyLinks) throws FcrepoOperationFailedException {
        final HeadBuilder headBuilder = mock(HeadBuilder.class);
        final FcrepoResponse headResponse = mock(FcrepoResponse.class);
        when(client.head(eq(uri))).thenReturn(headBuilder);
        when(headBuilder.disableRedirects()).thenReturn(headBuilder);
        when(headBuilder.perform()).thenReturn(headResponse);
        when(headResponse.getUrl()).thenReturn(uri);
        when(headResponse.getLinkHeaders(eq("describedby"))).thenReturn(describedbyLinks);
        when(headResponse.getStatusCode()).thenReturn(200);
        when(headResponse.getLinkHeaders(eq("type"))).thenReturn(typeLinks);

        return headBuilder;
    }

    /**
     * Mocks a successful GET request response
     * 
     * @param client client
     * @param uri uri of destination being mocked
     * @param typeLinks type links
     * @param describedbyLinks described by links
     * @param body body of response
     * @throws FcrepoOperationFailedException client failures
     */
    public static GetBuilder mockGetResponse(final FcrepoClient client, final URI uri, final List<URI> typeLinks,
            final List<URI> describedbyLinks, final String body) throws FcrepoOperationFailedException {
        final GetBuilder getBuilder = mock(GetBuilder.class);
        final FcrepoResponse getResponse = mock(FcrepoResponse.class);
        when(client.get(eq(uri))).thenReturn(getBuilder);
        when(getBuilder.accept(isA(String.class))).thenReturn(getBuilder);
        when(getBuilder.disableRedirects()).thenReturn(getBuilder);
        when(getBuilder.perform()).thenReturn(getResponse);
        when(getResponse.getBody()).thenAnswer(new Answer<InputStream>() {
            @Override
            public InputStream answer(final InvocationOnMock invocation) throws Throwable {
                return new ByteArrayInputStream(body.getBytes());
            }
        });
        when(getResponse.getUrl()).thenReturn(uri);
        when(getResponse.getLinkHeaders(eq("describedby"))).thenReturn(describedbyLinks);
        when(getResponse.getStatusCode()).thenReturn(200);
        when(getResponse.getLinkHeaders(eq("type"))).thenReturn(typeLinks);

        return getBuilder;
    }

    /**
     * Mocks an unsuccessful GET request response
     * 
     * @param client client
     * @param uri uri of destination being mocked
     * @param statusCode the status code for the response
     * @throws FcrepoOperationFailedException client failures
     */
    public static GetBuilder mockGetResponseError(final FcrepoClient client, final URI uri, final int statusCode)
            throws FcrepoOperationFailedException {
        final GetBuilder getBuilder = mock(GetBuilder.class);
        final FcrepoResponse getResponse = mock(FcrepoResponse.class);
        when(client.get(eq(uri))).thenReturn(getBuilder);
        when(getBuilder.accept(isA(String.class))).thenReturn(getBuilder);
        when(getBuilder.disableRedirects()).thenReturn(getBuilder);
        when(getBuilder.perform()).thenReturn(getResponse);
        when(getResponse.getStatusCode()).thenReturn(statusCode);

        return getBuilder;
    }

    /**
     * Mocks an unsuccessful HEAD request response
     * 
     * @param client client
     * @param uri uri of destination being mocked
     * @param statusCode the status code for the response
     * @throws FcrepoOperationFailedException client failures
     */
    public static HeadBuilder mockHeadResponseError(final FcrepoClient client, final URI uri, final int statusCode)
            throws FcrepoOperationFailedException {
        final HeadBuilder headBuilder = mock(HeadBuilder.class);
        final FcrepoResponse response = mock(FcrepoResponse.class);
        when(client.head(eq(uri))).thenReturn(headBuilder);
        when(headBuilder.disableRedirects()).thenReturn(headBuilder);
        when(headBuilder.perform()).thenReturn(response);
        when(response.getStatusCode()).thenReturn(statusCode);

        return headBuilder;
    }

    public static PutBuilder mockPutResponse(final FcrepoClient client, final URI uri) throws FcrepoOperationFailedException {
        final PutBuilder putBuilder = mock(PutBuilder.class);
        final FcrepoResponse response = mock(FcrepoResponse.class);
        when(client.put(eq(uri))).thenReturn(putBuilder);
        when(putBuilder.body(isA(InputStream.class), isA(String.class))).thenReturn(putBuilder);
        when(putBuilder.digest(isA(String.class))).thenReturn(putBuilder);
        when(putBuilder.preferLenient()).thenReturn(putBuilder);
        when(putBuilder.perform()).thenReturn(response);
        when(response.getStatusCode()).thenReturn(201);

        return putBuilder;
    }

    public static PostBuilder mockPostResponse(final FcrepoClient client, final URI uri) throws FcrepoOperationFailedException {
        final PostBuilder postBuilder = mock(PostBuilder.class);
        final FcrepoResponse response = mock(FcrepoResponse.class);
        when(client.post(eq(uri))).thenReturn(postBuilder);
        when(postBuilder.body(isA(InputStream.class), isA(String.class))).thenReturn(postBuilder);
        when(postBuilder.digest(isA(String.class))).thenReturn(postBuilder);
        when(postBuilder.slug(anyString())).thenReturn(postBuilder);
        when(postBuilder.perform()).thenReturn(response);
        when(response.getStatusCode()).thenReturn(201);

        return postBuilder;
    }
}
