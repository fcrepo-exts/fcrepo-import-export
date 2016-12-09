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

import static org.fcrepo.importexport.common.FcrepoConstants.BINARY_EXTENSION;
import static org.fcrepo.importexport.common.FcrepoConstants.EXTERNAL_RESOURCE_EXTENSION;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;

/**
 * @author barmintor
 * @since 2016-08-31
 */
public interface TransferProcess {

    final static String IMPORT_EXPORT_LOG_PREFIX = "org.fcrepo.importexport.audit";

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

    /**
     * Gets the file where a binary resource at the given URL would be stored
     * in the export package.
     * @param uri the URI for the resource
     * @param baseDir the base directory in the export package
     * @return a unique location for binary resources from the path of the given URI
     *         would be stored
     */
    public static File fileForBinary(final URI uri, final File baseDir) {
        return fileForURI(uri, baseDir, BINARY_EXTENSION);
    }

    /**
     * Gets the file where an external binary resource at the given URL would be stored
     * in the export package.
     * @param uri the URI for the resource
     * @param baseDir the base directory in the export package
     * @return a unique location for external resources from the path of the given URI
     *         would be stored
     */
    public static File fileForExternalResources(final URI uri, final File baseDir) {
        return fileForURI(uri, baseDir, EXTERNAL_RESOURCE_EXTENSION);
    }

    /**
     * Gets the file where a resource at the given URL with the given extension
     * would be stored in the export package.
     * @param uri the URI for the resource
     * @param baseDir the baseDir directory in the export package
     * @param extension the arbitrary extension expected the file
     * @return a unique location for resources from the path of the given URI
     *         would be stored
     */
    public static File fileForURI(final URI uri, final File baseDir, final String extension) {
        return new File(baseDir, TransferProcess.encodePath(uri.getPath()) + extension);
    }

    /**
     * Gets the directory where metadata resources contained by the resource at the given
     * URI would be stored in the export package.
     * @param uri the URI for the resource
     * @param baseDir the base directory in the export package
     * @return a unique location for metadata resources contained by the resource at the
     *         given URI would be stored
     */
    public static File directoryForContainer(final URI uri, final File baseDir) {
        return new File(baseDir, TransferProcess.encodePath(uri.getPath()));
    }
}
