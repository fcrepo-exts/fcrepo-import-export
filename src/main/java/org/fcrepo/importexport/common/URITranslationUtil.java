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

import static org.fcrepo.importexport.common.FcrepoConstants.FCR_VERSIONS_PATH;

import java.io.File;
import java.net.URI;

/**
 * Utility for creating or manipulating uris
 *
 * @author bbpennel
 *
 */
public abstract class URITranslationUtil {

    /**
     * Builds the repository URI for the given file
     *
     * @param f file to build URI for
     * @param config config
     * @return URI for file
     */
    public static URI uriForFile(final File f, final Config config) {
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

    /**
     * Remaps the given uri to its expected uri within the destination repository
     *
     * @param uri resource uri
     * @param sourceURI source uri
     * @param destinationURI destination base uri
     * @return remapped resource uri
     */
    public static URI remapResourceUri(final URI uri, final URI sourceURI, final URI destinationURI) {
        return URI.create(remapResourceUri(uri.toString(), sourceURI == null ? null : sourceURI.toString(),
                destinationURI == null ? null : destinationURI.toString()));
    }

    /**
     * Remaps the given uri to its expected uri within the destination repository
     *
     * @param uri resource uri
     * @param sourceURI source uri
     * @param destinationURI destination base uri
     * @return remapped resource uri
     */
    public static String remapResourceUri(final String uri, final String sourceURI, final String destinationURI) {
        String remapped = uri;
        if (remapped.contains(FCR_VERSIONS_PATH)) {
            remapped = remapped.replaceFirst("/fcr:versions/[^/]+", "");
        }
        if (sourceURI != null && destinationURI != null
                && uri.startsWith(sourceURI)) {
            remapped = remapped.replaceFirst(sourceURI, destinationURI);
        }

        return remapped;
    }
}
