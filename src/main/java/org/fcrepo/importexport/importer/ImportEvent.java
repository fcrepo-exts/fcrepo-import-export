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

import static org.fcrepo.importexport.common.URITranslationUtil.remapResourceUri;

import java.net.URI;

import org.fcrepo.importexport.common.Config;

/**
 * An object representing an event to be executed when importing a resource
 *
 * @author bbpennel
 *
 */
public abstract class ImportEvent {

    protected final URI uri;
    protected final URI mappedUri;
    protected final Config config;
    protected final long lastModified;
    protected final long created;
    protected long timestamp;

    /**
     * Constructs an ImportEvent
     *
     * @param uri uri of the resource affected by the event
     * @param created created timestamp
     * @param lastModified last modified timestamp
     * @param config config
     */
    public ImportEvent(final URI uri, final long created, final long lastModified,
            final Config config) {
        this.config = config;
        this.uri = uri;
        this.created = created;
        this.lastModified = lastModified;
        this.timestamp = created;
        this.mappedUri = remapResourceUri(uri, config.getSource(), config.getDestination());
    }

    /**
     * Get the original URI for this resource
     *
     * @return the uri
     */
    public URI getUri() {
        return uri;
    }

    /**
     * Get the URI for this resource remapped for the destination repository
     *
     * @return the mapped uri
     */
    public URI getMappedUri() {
        return mappedUri;
    }


    /**
     * @return the lastModified timestamp
     */
    public long getLastModified() {
        return lastModified;
    }


    /**
     * @return the created timestamp
     */
    public long getCreated() {
        return created;
    }

    /**
     * @return the comparable timestamp for this resource
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * @param timestamp the timestamp to set
     */
    public void setTimestamp(final long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return uri.toString();
    }
}
