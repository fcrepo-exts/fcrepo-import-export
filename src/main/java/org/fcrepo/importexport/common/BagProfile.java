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

import static org.fcrepo.importexport.common.FcrepoConstants.BAG_INFO_FIELDNAME;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author escowles
 * @since 2016-12-12
 */
public class BagProfile {

    private static final Logger logger = getLogger(BagProfile.class);

    private Set<String> payloadDigestAlgorithms;
    private Set<String> tagDigestAlgorithms;

    private Set<String> sections = new HashSet<>();
    private Map<String, Map<String, ProfileFieldRule>> metadataFields = new HashMap<>();

    /**
     * Default constructor.
     * @param in InputStream containing the Bag profile JSON document
     * @throws IOException when there is an I/O error reading JSON
     */
    public BagProfile(final InputStream in) throws IOException {
        final ObjectMapper mapper = new ObjectMapper();
        final JsonNode json = mapper.readTree(in);

        payloadDigestAlgorithms = arrayValues(json, "Manifests-Required");
        tagDigestAlgorithms = arrayValues(json, "Tag-Manifests-Required");
        if (tagDigestAlgorithms == null) {
            tagDigestAlgorithms = payloadDigestAlgorithms;
        }

        metadataFields.put(BAG_INFO_FIELDNAME, metadataFields(json, BAG_INFO_FIELDNAME));
        sections.add(BAG_INFO_FIELDNAME);

        if (json.get("Other-Info") != null) {
            loadOtherTags(json);
        }
    }

    private void loadOtherTags(final JsonNode json) {
        final JsonNode arrayTags = json.get("Other-Info");
        if (arrayTags != null && arrayTags.isArray()) {
            arrayTags.forEach(tag -> tag.fieldNames().forEachRemaining(sections::add));
            final Iterator<JsonNode> arrayEntries = arrayTags.elements();
            while (arrayEntries.hasNext()) {
                final JsonNode entries = arrayEntries.next();
                final Iterator<String> tagNames = entries.fieldNames();
                while (tagNames.hasNext()) {
                    final String tagName = tagNames.next();
                    metadataFields.put(tagName, metadataFields(entries, tagName));
                }
            }
        }
        logger.debug("tagFiles is {}", sections);
        logger.debug("metadataFields is {}", metadataFields);
    }

    private static Set<String> arrayValues(final JsonNode json, final String key) {
        final JsonNode values = json.get(key);

        if (values == null) {
            return null;
        }

        final Set<String> results = new HashSet<>();
        for (int i = 0; i < values.size(); i++) {
            results.add(values.get(i).asText());
        }
        return results;
    }

    /**
     * Loads required tags and allowed values
     *
     * @param json json to parse
     * @param key key in json to load tags from
     * @return map of tags => set of allowed values
     */
    private static Map<String, ProfileFieldRule> metadataFields(final JsonNode json, final String key) {
        final JsonNode fields = json.get(key);

        if (fields == null) {
            return null;
        }

        final Map<String, ProfileFieldRule> results = new HashMap<>();
        for (final java.util.Iterator<String> it = fields.fieldNames(); it.hasNext(); ) {
            // fields we set with
            boolean required = false;
            boolean recommended = false;
            String description = "";
            Set<String> values = Collections.emptySet();

            final String name = it.next();
            final JsonNode field = fields.get(name);


            // not sure if this is exactly what we want but good enough for a first pass
            // can probably move to BagProfileField constructor imo
            final JsonNode requiredNode = field.get("required");
            if (requiredNode != null && requiredNode.asBoolean()) {
                required = requiredNode.asBoolean();
            }

            final JsonNode recommendedNode = field.get("recommended");
            if (recommendedNode != null && recommendedNode.asBoolean()) {
                recommended = recommendedNode.asBoolean();
            }

            final JsonNode descriptionNode = field.get("description");
            if (descriptionNode != null && descriptionNode.asText().isEmpty()) {
               description = descriptionNode.asText();
            }

            final Set<String> readValues = arrayValues(field, "values");
            values = readValues == null ? values : readValues;

            results.put(name, new ProfileFieldRule(required, recommended, description, values));
            /*
            if (field.get("required") != null && field.get("required").asBoolean()) {
                results.put(name, arrayValues(field, "values"));
            }
             */
        }

        return results;
    }

    /**
     * Get the required digest algorithms for payload manifests.
     * @return Set of digest algorithm names
     */
    public Set<String> getPayloadDigestAlgorithms() {
        return payloadDigestAlgorithms;
    }

    /**
     * Get the required digest algorithms for tag manifests.
     * @return Set of digest algorithm names
     */
    public Set<String> getTagDigestAlgorithms() {
        return tagDigestAlgorithms;
    }

    /**
     * Get the required Bag-Info metadata fields.
     * @return A map of field names to a Set of acceptable values (or null when the values are restricted).
     */
    public Map<String, ProfileFieldRule> getMetadataFields() {
        return getMetadataFields(BAG_INFO_FIELDNAME);
    }

    /**
     * Get the required tags for the extra tag file
     *
     * @param tagFile the tag file to get tags for
     * @return map of tag = set of acceptable values, or null if tagFile doesn't exist
     */
    public Map<String, ProfileFieldRule> getMetadataFields(final String tagFile) {
        return metadataFields.get(tagFile);
    }

    /**
     * Get all the section names in this profile, which can be used with getMetadataFields().
     *
     * @return set of section names
     */
    public Set<String> getSectionNames() {
        return sections;
    }

    /**
     * Validate a given BagConfig against the current profile
     *
     * @param config the BagConfig
     */
    public void validateConfig(final BagConfig config) {
        for (final String section : sections) {
            if (config.hasTagFile(section.toLowerCase() + ".txt")) {
                try {
                    ProfileValidationUtil.validate(section, getMetadataFields(section),
                        config.getFieldsForTagFile(section.toLowerCase() + ".txt"));
                } catch (ProfileValidationException e) {
                    throw new RuntimeException(e.getMessage(), e);
                }
            } else {
                throw new RuntimeException(String.format("Error missing section %s from bag config", section));
            }
        }
    }
}
