package org.fcrepo.importexport.importer;

import java.net.URI;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.fcrepo.importexport.common.Config;

/**
 * An event for importing a version of a resource
 *
 * @author bbpennel
 *
 */
public class ImportVersion extends ImportEvent {
    final private static Pattern versionUriPattern = Pattern.compile(".+/fcr:versions/([^/]+)(/.+)?");

    private final String label;

    /**
     * Constructs a ImportVersion object
     *
     * @param id identifier of the version being imported
     * @param uri uri of the resource being versioned
     * @param created created timestamp of the version
     * @param config config
     */
    public ImportVersion(final String id, final URI uri, final long created, final Config config) {
        super(id, uri, created, created, config);

        final Matcher versionUriMatcher = versionUriPattern.matcher(uri.toString());

        if (versionUriMatcher.matches()) {
            this.label = versionUriMatcher.group(1);
        } else {
            throw new RuntimeException("Version for resource " + uri + " does not provide a required label");
        }
    }

    /**
     * The label of the version bring imported
     *
     * @return label
     */
    public String getLabel() {
        return label;
    }

    @Override
    public long getTimestamp() {
        return created;
    }
}
