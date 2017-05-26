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

/**
 * Utilities for manipulating URIs.
 *
 * @author escowles
 * @since 2017-05-26
 */
public class UriUtils {

    private UriUtils() {
        // prevent instantiation
    }

    /**
     * Make sure the URI ends with a "/"
     * @param uri The URI
     * @return The URI ending with a "/"
    **/
    public static URI withSlash(final URI uri) {
        return uri.toString().endsWith("/") ? uri : URI.create(uri.toString() + "/");
    }

    /**
     * Make sure the URI does not end with a "/"
     * @param uri The URI
     * @return The URI with any trailing "/" removed
    **/
    public static URI withoutSlash(final URI uri) {
        final String s = uri.toString();
        return s.endsWith("/") ? URI.create(s.substring(0, s.length() - 1)) : uri;
    }
}
