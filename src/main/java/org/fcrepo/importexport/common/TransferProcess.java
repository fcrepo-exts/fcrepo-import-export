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

import static org.apache.jena.rdf.model.ModelFactory.createDefaultModel;
import static org.apache.jena.rdf.model.ResourceFactory.createResource;
import static org.fcrepo.importexport.common.FcrepoConstants.BINARY_EXTENSION;
import static org.fcrepo.importexport.common.FcrepoConstants.EXTERNAL_RESOURCE_EXTENSION;
import static org.fcrepo.importexport.common.FcrepoConstants.RDF_TYPE;
import static org.fcrepo.importexport.common.FcrepoConstants.REPOSITORY_NAMESPACE;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.jena.rdf.model.Model;
import org.fcrepo.client.FcrepoClient;
import org.fcrepo.client.FcrepoOperationFailedException;
import org.fcrepo.client.FcrepoResponse;

/**
 * @author lsitu
 * @author barmintor
 * @since 2016-08-31
 */
public interface TransferProcess {

    final static String IMPORT_EXPORT_LOG_PREFIX = "org.fcrepo.importexport.audit";
    final static String BAGIT_CHECKSUM_DELIMITER = "  ";

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
     * @param sourcePath the base path the resource was exported from
     * @param destinationPath the base path the resource is importing to
     * @param baseDir the base directory in the export package
     * @return a unique location for binary resources from the path of the given URI
     *         would be stored
     */
    public static File fileForBinary(final URI uri, final String sourcePath, final String destinationPath,
            final File baseDir) {
        return fileForURI(uri, sourcePath, destinationPath, baseDir, BINARY_EXTENSION);
    }

    /**
     * Gets the file where an external binary resource at the given URL would be stored
     * in the export package.
     * @param uri the URI for the resource
     * @param sourcePath the base path the resource was exported from
     * @param destinationPath the base path the resource is importing to
     * @param baseDir the base directory in the export package
     * @return a unique location for external resources from the path of the given URI
     *         would be stored
     */
    public static File fileForExternalResources(final URI uri, final String sourcePath,
            final String destinationPath, final File baseDir) {
        return fileForURI(uri, sourcePath, destinationPath, baseDir, EXTERNAL_RESOURCE_EXTENSION);
    }

    /**
     * Gets the file where a resource at the given URL with the given extension
     * would be stored in the export package.
     * @param uri the URI for the resource
     * @param sourcePath the base path the resource was exported from
     * @param destinationPath the base path the resource is importing to
     * @param baseDir the baseDir directory in the export package
     * @param extension the arbitrary extension expected the file
     * @return a unique location for resources from the path of the given URI
     *         would be stored
     */
    public static File fileForURI(final URI uri, final String sourcePath, final String destinationPath,
            final File baseDir, final String extension) {
        String path = uri.getPath();
        if (sourcePath != null && destinationPath != null) {
            path = path.replaceFirst(destinationPath, sourcePath);
        }
        return new File(baseDir, encodePath(path) + extension);
    }

    /**
     * Gets the directory where metadata resources contained by the resource at the given
     * URI would be stored in the export package.
     * @param uri the URI for the resource
     * @param sourcePath the base path the resource was exported from
     * @param destinationPath the base path the resource is importing to
     * @param baseDir the base directory in the export package
     * @return a unique location for metadata resources contained by the resource at the
     *         given URI would be stored
     */
    public static File directoryForContainer(final URI uri, final String sourcePath,
            final String destinationPath, final File baseDir) {
        return fileForURI(uri, sourcePath, destinationPath, baseDir, "");
    }

    /**
     * Gets a Map of files and sha1 checksums from the BagIt manifest file.
     *
     * @param manifestFile the manifest file
     * @param baseDir the base directory in the export package
     * @return the map
     */
    public static Map<String, String> getSha1FileMap(final File baseDir, final Path manifestFile) {
        final Map<String, String> sha1FileMap = new HashMap<String, String>();
        try (final Stream<String> stream = Files.lines(manifestFile)) {
            stream.forEach(l -> {
                final String[] manifestTokens = l.split(BAGIT_CHECKSUM_DELIMITER);
                final File file = Paths.get(baseDir.toURI()).resolve(Paths.get(manifestTokens[1])).toFile();
                final String checksum = manifestTokens[0].trim();
                sha1FileMap.put(file.getAbsolutePath(), checksum);
            });
        } catch (IOException e) {
            throw new RuntimeException("Error reading manifest: " + manifestFile.toString(), e);
        }
        return sha1FileMap;
    }

    /**
     * Checks the response code and throws a RuntimeException with a helpful
     * message (when possible) for non 2xx codes.
     * @param response the response from a REST call to Fedora
     * @param uri the URI against which the request was made
     */
    public static void checkValidResponse(final FcrepoResponse response, final URI uri, final String user) {
        switch (response.getStatusCode()) {
        case 401:
            throw new AuthenticationRequiredRuntimeException();
        case 403:
            throw new AuthorizationDeniedRuntimeException(user, uri);
        case 404:
            throw new ResourceNotFoundRuntimeException(uri);
        default:
            if (response.getStatusCode() < 200 || response.getStatusCode() > 307) {
                throw new RuntimeException("Export operation failed: unexpected status "
                        + response.getStatusCode() + " for " + uri);
            }
        }
    }

    /**
     * Utility method to determine whether the current uri is repository root or not. The repository root is the
     * container with type fcrepo:RepositoryRoot
     * @param uri the URI for the resource
     * @param client the FcrepoClient to query the repository
     * @param config the Config for import/export
     * @throws IOException
     * @throws FcrepoOperationFailedException
     */
    public static boolean isRepositoryRoot(final URI uri, final FcrepoClient client, Config config)
            throws IOException, FcrepoOperationFailedException {
        final String userName = config.getUsername();
        final String rdfLanguage = config.getRdfLanguage();
        try (FcrepoResponse response = client.head(uri).disableRedirects().perform()) {
            checkValidResponse(response, uri, userName);
            try (FcrepoResponse resp = client.get(uri).accept(rdfLanguage).disableRedirects()
                    .perform()) {
                final Model model = createDefaultModel().read(resp.getBody(), null, rdfLanguage);
                if (model.contains(null, RDF_TYPE, createResource(REPOSITORY_NAMESPACE + "RepositoryRoot"))) {
                    return true;
                }
            }
        }
        return false;
    }
}
