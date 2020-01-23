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

import static org.slf4j.LoggerFactory.getLogger;

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

/**
 * Utility methods for validating profiles.
 *
 * @author mikejritter
 * @author dbernstein
 * @since Dec 14, 2016
 */
public class ProfileValidationUtil {

    private static final Logger logger = getLogger(ProfileValidationUtil.class);

    /*
     * System generated Bag Info that should be ignored by validator.
     */
    protected static final Set<String> SYSTEM_GENERATED_FIELD_NAMES =
            new HashSet<>(Arrays.asList("Bagging-Date",
                                        "Bag-Size",
                                        "Payload-Oxum",
                                        "BagIt-Profile-Identifier"));

    private ProfileValidationUtil() {
    }

    /**
     * Validates the fields against the set of required fields and their constrained values.
     *
     * @param profileSection describes the section of the profile that is being validated.
     * @param requiredFields the required fields and any allowable values (if constrained).
     * @param fields The key value pairs to be validated.
     * @throws ProfileValidationException when the fields do not pass muster. The exception message contains a
     *         description of all validation errors found.
     */
    public static void validate(final String profileSection, final Map<String, ProfileFieldRule> requiredFields,
                                final Map<String, String> fields) throws ProfileValidationException {
        if (requiredFields != null) {
            final StringBuilder errors = new StringBuilder();

            for (String requiredField : requiredFields.keySet()) {
                // ignore validation on system generated fields
                if (SYSTEM_GENERATED_FIELD_NAMES.contains(requiredField)) {
                    logger.debug("skipping system generated field {}...", requiredField);
                    continue;
                }

                final ProfileFieldRule rule = requiredFields.get(requiredField);
                if (fields.containsKey(requiredField)) {
                    final String value = fields.get(requiredField);
                    final Set<String> validValues = rule.getValues();
                    if (validValues != null && !validValues.isEmpty()) {
                        if (!validValues.contains(value)) {
                            final String invalidMessage = "\"%s\" is not valid for \"%s\". Valid values: %s\n";
                            errors.append(String.format(invalidMessage, value, requiredField,
                                                        StringUtils.join(validValues, ",")));
                        }
                    }
                } else if (rule.isRequired()) {
                    errors.append("\"" + requiredField + "\" is a required field.\n");
                } else if (rule.isRecommended()) {
                    logger.warn("{} does not contain the recommended field {}", profileSection, requiredField);
                }
            }

            if (errors.length() > 0) {
                throw new ProfileValidationException(
                        "Bag profile validation failure: The following errors occurred in the " +
                                profileSection + ":\n" + errors.toString());
            }
        }

    }

    /**
     * Check if a given tag file is part of the allowed tags. Should not be used against non-tag files such as the
     * manifests or bagit.txt.
     *
     * @param tag the tag file to check
     * @param allowedTags the list of allowed tag files, with unix style globbing allowed
     */
    public static void validateTagIsAllowed(final Path tag, final Set<String> allowedTags)
        throws ProfileValidationException {
        if (tag != null && allowedTags != null && !allowedTags.isEmpty()) {
            // sanity check against required BagIt files
            final String systemFiles = "bagit\\.txt|bag-info\\.txt|manifest-.*|tagmanifest-.*";
            if (Pattern.matches(systemFiles, tag.toString())) {
                logger.warn("Tag validator used against required file {}; ignoring", tag);
                return;
            }

            boolean match = false;
            for (String allowedTag : allowedTags) {
                final PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + allowedTag);

                if (matcher.matches(tag)) {
                    match = true;
                    break;
                }
            }

            if (!match) {
                throw new ProfileValidationException("Bag profile validation failure: tag " + tag +
                                                     " is not allowed. List of allowed tag files are " +
                                                     StringUtils.join(allowedTags, ","));
            }
        }
    }

}
