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

import static org.fcrepo.importexport.common.BagProfileConstants.ACCEPT_BAGIT_VERSION;
import static org.fcrepo.importexport.common.BagProfileConstants.ACCEPT_SERIALIZATION;
import static org.fcrepo.importexport.common.BagProfileConstants.ALLOW_FETCH_TXT;
import static org.fcrepo.importexport.common.BagProfileConstants.BAGIT_PROFILE_INFO;
import static org.fcrepo.importexport.common.BagProfileConstants.MANIFESTS_ALLOWED;
import static org.fcrepo.importexport.common.BagProfileConstants.MANIFESTS_REQUIRED;
import static org.fcrepo.importexport.common.BagProfileConstants.OTHER_INFO;
import static org.fcrepo.importexport.common.BagProfileConstants.SERIALIZATION;
import static org.fcrepo.importexport.common.BagProfileConstants.TAG_FILES_ALLOWED;
import static org.fcrepo.importexport.common.BagProfileConstants.TAG_FILES_REQUIRED;
import static org.fcrepo.importexport.common.BagProfileConstants.TAG_MANIFESTS_ALLOWED;
import static org.fcrepo.importexport.common.BagProfileConstants.TAG_MANIFESTS_REQUIRED;
import static org.fcrepo.importexport.common.FcrepoConstants.BAG_INFO_FIELDNAME;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author mikejritter
 * @author escowles
 * @since 2016-12-12
 */
public class BagProfile {

    public enum Serialization {
        FORBIDDEN, REQUIRED, OPTIONAL, UNKNOWN;

        /**
         * Retrieve the {@link Serialization} from a string representation
         *
         * @param value the String value to use
         * @return the {@link Serialization} the {@code value} is equal to
         */
        public static Serialization of(final String value) {
            switch (value.toLowerCase()) {
                case "forbidden": return FORBIDDEN;
                case "required": return REQUIRED;
                case "optional": return OPTIONAL;
                default: return UNKNOWN;
            }
        }
    }

    private static final Logger logger = getLogger(BagProfile.class);

    private boolean allowFetch;
    private Serialization serialization;

    private Set<String> acceptedBagItVersions;
    private Set<String> acceptedSerializations;

    private Set<String> tagFilesAllowed;
    private Set<String> tagFilesRequired;

    private Set<String> allowedPayloadAlgorithms;
    private Set<String> allowedTagAlgorithms;

    private Set<String> payloadDigestAlgorithms;
    private Set<String> tagDigestAlgorithms;

    private Set<String> sections = new HashSet<>();
    private Map<String, Map<String, ProfileFieldRule>> metadataFields = new HashMap<>();
    private Map<String, String> profileMetadata = new HashMap<>();

    /**
     * Default constructor.
     *
     * @param in InputStream containing the Bag profile JSON document
     * @throws IOException when there is an I/O error reading JSON
     */
    public BagProfile(final InputStream in) throws IOException {
        final ObjectMapper mapper = new ObjectMapper();
        final JsonNode json = mapper.readTree(in);

        loadProfileInfo(json);

        allowFetch = json.has(ALLOW_FETCH_TXT) ? json.get(ALLOW_FETCH_TXT).asBoolean() : true;
        serialization = json.has(SERIALIZATION) ? Serialization.of(json.get("Serialization").asText())
                                                  : Serialization.OPTIONAL;

        acceptedBagItVersions = arrayValues(json, ACCEPT_BAGIT_VERSION);
        acceptedSerializations = arrayValues(json, ACCEPT_SERIALIZATION);

        tagFilesAllowed = arrayValues(json, TAG_FILES_ALLOWED);
        tagFilesRequired = arrayValues(json, TAG_FILES_REQUIRED);

        allowedPayloadAlgorithms = arrayValues(json, MANIFESTS_ALLOWED);
        allowedTagAlgorithms = arrayValues(json, TAG_MANIFESTS_ALLOWED);

        payloadDigestAlgorithms = arrayValues(json, MANIFESTS_REQUIRED);
        tagDigestAlgorithms = arrayValues(json, TAG_MANIFESTS_REQUIRED);
        if (tagDigestAlgorithms == null) {
            tagDigestAlgorithms = payloadDigestAlgorithms;
        }

        metadataFields.put(BAG_INFO_FIELDNAME, metadataFields(json, BAG_INFO_FIELDNAME));
        sections.add(BAG_INFO_FIELDNAME);

        if (json.get(OTHER_INFO) != null) {
            loadOtherTags(json);
        }
    }

    private void loadProfileInfo(final JsonNode json) {
        final JsonNode tag = json.get(BAGIT_PROFILE_INFO);
        if (tag != null) {
            tag.fields().forEachRemaining(entry -> profileMetadata.put(entry.getKey(), entry.getValue().asText()));
        }
    }

    private void loadOtherTags(final JsonNode json) {
        final JsonNode arrayTags = json.get(OTHER_INFO);
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
            return Collections.emptySet();
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
     * @param key  key in json to load tags from
     * @return map of tags => set of allowed values
     */
    private static Map<String, ProfileFieldRule> metadataFields(final JsonNode json, final String key) {
        final JsonNode fields = json.get(key);

        if (fields == null) {
            return Collections.emptyMap();
        }

        final Map<String, ProfileFieldRule> results = new HashMap<>();
        for (final Iterator<String> it = fields.fieldNames(); it.hasNext(); ) {
            // fields to pass to the ProfileFieldRule constructor
            boolean required = false;
            boolean repeatable = true;
            boolean recommended = false;
            String description = "No description";
            Set<String> values = Collections.emptySet();

            final String name = it.next();
            final JsonNode field = fields.get(name);

            // read each of the fields for the ProfileFieldRule:
            // required, repeated, recommended, description, and values
            final JsonNode requiredNode = field.get("required");
            if (requiredNode != null && requiredNode.asBoolean()) {
                required = requiredNode.asBoolean();
            }

            final JsonNode repeatedNode = field.get("repeatable");
            if (repeatedNode != null) {
                repeatable = repeatedNode.asBoolean();
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

            results.put(name, new ProfileFieldRule(required, repeatable, recommended, description, values));
        }

        return results;
    }

    /**
     * Boolean flag allowing a fetch.txt file
     *
     * @return true if fetch.txt is allowed, false otherwise
     */
    public boolean isAllowFetch() {
        return allowFetch;
    }

    /**
     * Get the support of serialization for a Bag.
     *
     * Allowed values are: forbidden, required, and optional
     *
     * @return String value of "forbidden", "required", or "optional"
     */
    public Serialization getSerialization() {
        return serialization;
    }

    /**
     * Get the supported BagIt versions
     *
     * @return Set of BagIt version numbers
     */
    public Set<String> getAcceptedBagItVersions() {
        return acceptedBagItVersions;
    }

    /**
     * Get the supported serialization formats
     *
     * If {@link BagProfile#getSerialization()} has a value of required or optional, at least one value is needed.
     * If {@link BagProfile#getSerialization()} is forbidden, this has no meaning
     *
     * @return Set of serialization formats
     */
    public Set<String> getAcceptedSerializations() {
        return acceptedSerializations;
    }

    /**
     * Get the names of allowed tag files; supports unix style globbing
     *
     * All the tag files listed in {@link BagProfile#getTagFilesRequired()} must be in included in this
     *
     * @return Set of allowed tag files
     */
    public Set<String> getTagFilesAllowed() {
        return tagFilesAllowed;
    }

    /**
     * Get the tag files which are required to exist
     *
     * @return Set of tag filenames
     */
    public Set<String> getTagFilesRequired() {
        return tagFilesRequired;
    }

    /**
     * Get the payload algorithms which are allowed
     *
     * When specified along with {@link BagProfile#getPayloadDigestAlgorithms()}, this must include at least all of the
     * manifest types listed in {@link BagProfile#getPayloadDigestAlgorithms()}.
     *
     * @return Set of digest algorithm names
     */
    public Set<String> getAllowedPayloadAlgorithms() {
        return allowedPayloadAlgorithms;
    }

    /**
     * Get the tag manifest algorithms which are allowed.
     *
     * When specified along with {@link BagProfile#getTagDigestAlgorithms()}, this must include at least all of the tag
     * manifest types listed in {@link BagProfile#getTagDigestAlgorithms()}.
     *
     * @return Set of digest algorithm names
     */
    public Set<String> getAllowedTagAlgorithms() {
        return allowedTagAlgorithms;
    }

    /**
     * Get the required digest algorithms for payload manifests.
     *
     * @return Set of digest algorithm names
     */
    public Set<String> getPayloadDigestAlgorithms() {
        return payloadDigestAlgorithms;
    }

    /**
     * Get the required digest algorithms for tag manifests.
     *
     * @return Set of digest algorithm names
     */
    public Set<String> getTagDigestAlgorithms() {
        return tagDigestAlgorithms;
    }

    /**
     * Get the required Bag-Info metadata fields.
     *
     * @return A map of field names to a ProfileFieldRule containing acceptance criteria
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
     * Get the BagIt-Profile-Info section describing the BagIt Profile
     *
     * @return map of fields names to text descriptions
     */
    public Map<String, String> getProfileMetadata() {
        return profileMetadata;
    }

    /**
     * Validate a given BagConfig against the current profile
     *
     * @param config the BagConfig
     */
    public void validateConfig(final BagConfig config) {
        for (final String section : sections) {
            final String tagFile = section.toLowerCase() + ".txt";
            if (config.hasTagFile(tagFile)) {
                try {
                    ProfileValidationUtil.validate(section, getMetadataFields(section),
                        config.getFieldsForTagFile(tagFile));

                    ProfileValidationUtil.validateTagIsAllowed(Paths.get(tagFile), tagFilesAllowed);
                } catch (ProfileValidationException e) {
                    throw new RuntimeException(e.getMessage(), e);
                }
            } else {
                throw new RuntimeException(String.format("Error missing section %s from bag config", section));
            }
        }
    }

    /**
     * Validates this {@link BagProfile} according to the BagIt Profiles specification found at
     * https://bagit-profiles.github.io/bagit-profiles-specification/
     *
     * This checks the following fields:
     *
     * BagIt-Profile-Info
     * Existence of the Source-Organization, External-Description, Version, BagIt-Profile-Identifier, and
     * BagIt-Profile-Version fields
     *
     * Serialization
     * Is equal to one of "forbidden", "required", or "optional"
     *
     * Accept-Serialization
     * If serialization has a value of required or optional, at least one value is needed.
     *
     * Manifests-Allowed
     * If specified, the {@link BagProfile#getPayloadDigestAlgorithms()} must be a subset of
     * {@link BagProfile#getAllowedPayloadAlgorithms()}
     *
     * Tag-Manifests-Allowed
     * If specified, the {@link BagProfile#getTagDigestAlgorithms()} must be a subset of
     * {@link BagProfile#getAllowedTagAlgorithms()}
     *
     * Tag-Files-Allowed
     * If specified, the {@link BagProfile#getTagFilesRequired()} must be a subset of
     * {@link BagProfile#getTagFilesAllowed()}. If not specified, all tags must match the '*' glob
     *
     * Accept-BagIt-Version
     * At least one version is required
     */
    public void validateProfile() {
        final StringBuilder errors = new StringBuilder();

        // Bag-Info
        final List<String> expectedInfoFields = Arrays.asList("Source-Organization", "External-Description", "Version",
                                                              "BagIt-Profile-Identifier", "BagIt-Profile-Version");
        final Map<String, String> bagInfo = getProfileMetadata();
        for (final String expected : expectedInfoFields) {
            if (!bagInfo.containsKey(expected)) {
                if (errors.length() == 0) {
                    errors.append("Error(s) in BagIt-Profile-Info:\n");
                }
                errors.append("  * Missing key ").append(expected).append("\n");
            }
        }

        // Serialization / Accept-Serialization
        if (serialization == Serialization.REQUIRED || serialization == Serialization.OPTIONAL) {
            if (acceptedSerializations.isEmpty()) {
                errors.append("Serialization value of ").append(serialization)
                      .append(" requires at least one value in the Accept-Serialization field!\n");
            }
        } else if(serialization == Serialization.UNKNOWN) {
            errors.append("Unknown Serialization value ").append(serialization)
                  .append(". Allowed values are forbidden, required, or optional.\n");
        }

        // Manifests-Allowed / Manifests-Required
        if (!(allowedPayloadAlgorithms.isEmpty() || isSubset(payloadDigestAlgorithms, allowedPayloadAlgorithms))) {
            errors.append("Manifests-Required must be a subset of Manifests-Allowed!\n");
        }

        // Tag-Manifests-Allowed / Tag-Manifests-Required
        if (!(allowedTagAlgorithms.isEmpty() || isSubset(tagDigestAlgorithms, allowedTagAlgorithms))) {
            errors.append("Tag-Manifests-Required must be a subset of Tag-Manifests-Allowed!\n");
        }

        // Tag-Files-Allowed / Tag-Files-Required
        // should this do strict matching on globs?
        if (!(tagFilesAllowed.isEmpty() || isSubset(tagFilesRequired, tagFilesAllowed))) {
            errors.append("Tag-Files-Required must be a subset of Tag-Files-Allowed!\n");
        }

        if (acceptedBagItVersions.isEmpty()) {
            errors.append("Accept-BagIt-Version requires at least one entry!");
        }

        if (errors.length() > 0) {
            errors.insert(0, "Bag Profile json does not conform to BagIt Profiles specification! " +
                             "The following errors occurred:\n");
            throw new RuntimeException(errors.toString());
        }
    }

    /**
     * Check to see if a collection (labelled as {@code subCollection}) is a subset of the {@code superCollection}
     *
     * @param subCollection   the sub collection to iterate against and check if elements are contained within
     *                        {@code superCollection}
     * @param superCollection the super collection containing all the elements
     * @param <T>             the type of each collection
     * @return true if {@code superCollection} is empty or if all elements of {@code subCollection} are contained within
     * {@code superCollection}
     */
    private <T> boolean isSubset(final Collection<T> subCollection, final Collection<T> superCollection) {
        for (T t : subCollection) {
            if (!superCollection.contains(t)) {
                return false;
            }
        }

        return true;
    }

}
