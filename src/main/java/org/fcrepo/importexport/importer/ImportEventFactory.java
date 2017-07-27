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

import java.io.File;
import java.net.URI;

import org.fcrepo.importexport.common.Config;

/**
 * Factory which produces {@link ImportEvent} objects
 *
 * @author bbpennel
 *
 */
public class ImportEventFactory {

    private final Config config;

    /**
     * Default constructor
     *
     * @param config config
     */
    public ImportEventFactory(final Config config) {
        this.config = config;
    }

    /**
     * Creates an ImportResource event from the given resource uri and description file
     *
     * @param uri original uri of the resource
     * @param descriptionFile description of the resource
     * @param lastModified last modified timestamp of resource
     * @param created timestamp resource was created
     * @return the ImportResource
     */
    public ImportResource createFromUri(final URI uri, final File descriptionFile, final long created,
            final long lastModified) {
        final String uriString = uri.toString();
        final String id = uriString.substring(uriString.lastIndexOf('/') + 1);

        return new ImportResource(id, uri, descriptionFile, created, lastModified, config);
    }

    /**
     * Creates an ImportVersion event for the resource identified by uri
     *
     * @param uri original uri of the versioned resource
     * @param timestamp created timestamp of the version
     * @return the ImportVersion
     */
    public ImportVersion createImportVersion(final URI uri, final long timestamp) {
        final String uriString = uri.toString();
        final String id = uriString.substring(uriString.lastIndexOf('/') + 1);

        return new ImportVersion(id, uri, timestamp, config);
    }
}
