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
