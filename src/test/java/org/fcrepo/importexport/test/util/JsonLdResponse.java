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
package org.fcrepo.importexport.test.util;

import static org.fcrepo.importexport.common.FcrepoConstants.CONTAINS;
import static org.fcrepo.importexport.common.FcrepoConstants.CREATED_DATE;
import static org.fcrepo.importexport.common.FcrepoConstants.HAS_VERSION;
import static org.fcrepo.importexport.common.FcrepoConstants.HAS_VERSION_LABEL;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.jena.rdf.model.Resource;

/**
 * Helpers for constructing json ld responses in tests.
 *
 * @author bbpennel
 *
 */
public abstract class JsonLdResponse {

    public static String createJson(final URI resource, final URI... children) {
        return createJson(resource, null, children);
    }

    public static String createJson(final URI resource, final Resource type, final URI... children) {
        final StringBuilder json = new StringBuilder("{\"@id\":\"" + resource.toString() + "\"");
        if (type != null) {
            json.append(",\"@type\":[\"" + type.getURI() + "\"]");
        }
        if (children != null && children.length > 0) {
            json.append(",\"" + CONTAINS.getURI() + "\":[")
                .append(Arrays.stream(children)
                    .map(child -> "{\"@id\":\"" + child.toString()  + "\"}")
                    .collect(Collectors.joining(",")))
                .append(']');
        }
        json.append('}');
        return json.toString();
    }

    public static String joinJsonArray(final List<String> array) {
        return "[" + String.join(",", array) + "]";
    }

    public static List<String> addVersionJson(final List<String> versions, final URI rescUri, final URI versionUri,
            final String label, final String timestamp) {
        final String versionJson = "{\"@id\":\"" + rescUri.toString() + "\"," +
                "\"" + HAS_VERSION.getURI() + "\":[{\"@id\":\"" + versionUri.toString() + "\"}]}," +
            "{\"@id\":\"" + versionUri.toString() + "\"," +
                "\"" + CREATED_DATE.getURI() + "\":[{" +
                    "\"@value\":\"" + timestamp + "\"," +
                    "\"@type\": \"http://www.w3.org/2001/XMLSchema#dateTime\"}]," +
                "\"" + HAS_VERSION_LABEL.getURI() + "\":[{" +
                    "\"@value\":\"" + label + " \"}]" +
            "}";
        versions.add(versionJson);
        return versions;
    }
}
