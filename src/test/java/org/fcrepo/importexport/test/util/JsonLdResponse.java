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

public abstract class JsonLdResponse {

    public JsonLdResponse() {
        // TODO Auto-generated constructor stub
    }

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
