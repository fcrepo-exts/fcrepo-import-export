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
package org.fcrepo.importexport;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;

/**
 * @author barmintor
 * @since 2016-08-31
 */
public interface TransferProcess {
    /**
     * This method does the import or export
     */
    public void run();

    /**
     * Encodes a path (as if from a URI) to avoid
     * characters that may be disallowed in a filename.
     * This operation can be reversed by invoking
     * {@link #decodePath }.
     * @param path the path portion of a URI
     * @return a version of the path that avoid characters
     * such as ":".
     */
    public static String encodePath(final String path) {
        try {
            return URLEncoder.encode(path, "UTF-8").replace("%2F", "/");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Decodes a path. This operation can be reversed by
     * invoking {@link #encodePath }.
     * @param encoded the path portion of a URI
     * @return the original path
     */
    public static String decodePath(final String encoded) {
        try {
            return URLDecoder.decode(encoded, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

}
