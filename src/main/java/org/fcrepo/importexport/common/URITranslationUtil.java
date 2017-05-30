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

import java.io.File;
import java.net.URI;

/**
 * Utility for creating or manipulating uris
 * 
 * @author bbpennel
 *
 */
public class URITranslationUtil {

    private final Config config;

    /**
     * Construct a uri translation util
     * 
     * @param config
     */
    public URITranslationUtil(final Config config) {
        this.config = config;
    }

    /**
     * Builds the repository URI for the given file
     * 
     * @param f file to build URI for
     * @return URI for file
     */
    public URI uriForFile(final File f) {
        // get path of file relative to the data directory
        String relative = config.getBaseDirectory().toPath().relativize(f.toPath()).toString();
        relative = TransferProcess.decodePath(relative);

        // rebase the path on the destination uri (translating source/destination if needed)
        if ( config.getSource() != null && config.getDestination() != null ) {
            relative = baseURI(config.getSource()) + relative;
            relative = relative.replaceFirst(config.getSource().toString(), config.getDestination().toString());
        } else {
            relative = baseURI(config.getResource()) + relative;
        }

        // for exported RDF, just remove the ".extension" and you have the encoded path
        if (relative.endsWith(config.getRdfExtension())) {
            relative = relative.substring(0, relative.length() - config.getRdfExtension().length());
        }
        return URI.create(relative);
    }

    /**
     * Adds relative path to uri
     * 
     * @param uri base uri
     * @param path relative path to add
     * @return joined uri
     */
    public static URI addRelativePath(final URI uri, final String path) {
        final String base = uri.toString();

        if (base.charAt(base.length() - 1) == '/') {
            if (path.charAt(0) == '/') {
                return URI.create(base + path.substring(1, path.length()));
            }
            return URI.create(base + path);
        } else if (path.charAt(0) == '/') {
            return URI.create(base + path);
        }

        return URI.create(base + "/" + path);
    }

    private static String baseURI(final URI uri) {
        final String base = uri.toString().replaceFirst(uri.getPath() + "$", "");
        return (base.endsWith("/")) ? base : base + "/";
    }
}
