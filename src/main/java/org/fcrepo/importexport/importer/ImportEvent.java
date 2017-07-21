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

    protected final String id;
    protected final URI uri;
    protected final URI mappedUri;
    protected final Config config;
    protected final long timestamp;

    public ImportEvent(final String id, final URI uri, final long timestamp, final Config config) {
        this.id = id;
        this.config = config;
        this.uri = uri;
        this.timestamp = timestamp;
        this.mappedUri = remapResourceUri(uri, config.getSource(), config.getDestination());
    }

    /**
     * Get Id for this resource
     * 
     * @return the id
     */
    public String getId() {
        return id;
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
     * @return the timestamp
     */
    public long getTimestamp() {
        return timestamp;
    }
}
