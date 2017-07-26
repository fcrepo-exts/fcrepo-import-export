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

import java.net.URI;

import org.fcrepo.client.FcrepoResponse;

/**
 * Exception thrown when a resource is gone, and checks to see if the resource has a tombstone
 * 
 * @author bbpennel
 */
public class ResourceGoneRuntimeException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    final URI resourceUri;
    final URI tombstone;

    public ResourceGoneRuntimeException(final FcrepoResponse response) {
        tombstone = response.getLinkHeaders("hasTombstone").get(0);
        resourceUri = response.getUrl();
    }

    /**
     * @return the resourceUri
     */
    public URI getResourceUri() {
        return resourceUri;
    }

    /**
     * @return the tombstone
     */
    public URI getTombstone() {
        return tombstone;
    }
}
