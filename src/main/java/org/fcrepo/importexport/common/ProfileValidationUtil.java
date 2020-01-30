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

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import gov.loc.repository.bagit.domain.Manifest;
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
                                        "Bag-Count",
                                        "Payload-Oxum",
                                        "BagIt-Profile-Identifier"));

    private ProfileValidationUtil() {
    }

    /**
     * Validates a {@code tag} file against a set of {@code requiredFields} and their constrained values.
     *
     * @param profileSection describes the section of the profile that is being validated.
     * @param requiredFields the required fields and associated rule
     * @param tag the path to the info file to read
     * @throws ProfileValidationException when the fields do not pass muster. The exception message contains a
     *         description of all validation errors found.
     */
    public static void validate(final String profileSection, final Map<String, ProfileFieldRule> requiredFields,
                                final Path tag) throws ProfileValidationException, IOException {
        final Map<String, String> fields = readInfo(tag);
        validate(profileSection, requiredFields, fields, Collections.emptySet());
    }

    /**
     * Validates the {@code fields} against a set of {@code requiredFields} and their constrained values.
     *
     * @param profileSection describes the section of the profile that is being validated.
     * @param requiredFields the required fields and associated rule
     * @param fields the key value pairs to be validated
     * @throws ProfileValidationException when the fields do not pass muster. The exception message contains a
     *         description of all validation errors found.
     */
    public static void validate(final String profileSection, final Map<String, ProfileFieldRule> requiredFields,
                                final Map<String, String> fields) throws ProfileValidationException {
        validate(profileSection, requiredFields, fields, SYSTEM_GENERATED_FIELD_NAMES);
    }


    /**
     * Validates the fields against the set of required fields and their constrained values.
     *
     * @param profileSection describes the section of the profile that is being validated.
     * @param requiredFields the required fields and any allowable values (if constrained).
     * @param fields The key value pairs to be validated.
     * @param filter A set of fields to filter against. Useful for export.
     * @throws ProfileValidationException when the fields do not pass muster. The exception message contains a
     *         description of all validation errors found.
     */
    private static void validate(final String profileSection, final Map<String, ProfileFieldRule> requiredFields,
                                final Map<String, String> fields, final Set<String> filter) throws ProfileValidationException {
        if (requiredFields != null) {
            final StringBuilder errors = new StringBuilder();

            for (String requiredField : requiredFields.keySet()) {
                // ignore validation on system generated fields
                if (filter.contains(requiredField)) {
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
     * Validate that a {@code manifests} found in a {@link gov.loc.repository.bagit.domain.Bag} are allowed according to
     * both the {@code required} and {@code allowed} sets from a {@link BagProfile}.
     *
     * @param manifests the manifests found in a {@link gov.loc.repository.bagit.domain.Bag}
     * @param required the set of required manifest algorithms
     * @param allowed the set of allowed manifest algorithms
     * @param type the type of manifest being processed, normally 'tag' or 'payload'
     * @return A String with any validation errors associated with the {@code manifests}
     */
    public static StringBuilder validateManifest(final Set<Manifest> manifests, final Set<String> required,
                                                 final Set<String> allowed, final String type) {
        final String missing = "Missing %s manifest algorithm: %s\n";
        final String unsupported = "Unsupported %s manifest algorithm: %s\n";
        final StringBuilder errors = new StringBuilder();

        // make a copy so we do not mutate the BagProfile
        final Set<String> requiredCopy = new HashSet<>(required);

        for (final Manifest manifest : manifests) {
            final String algorithm = manifest.getAlgorithm().getBagitName();
            requiredCopy.remove(algorithm);

            if (!allowed.isEmpty() && !allowed.contains(algorithm)) {
                errors.append(String.format(unsupported, type, algorithm));
            }
        }

        if (!requiredCopy.isEmpty()) {
            errors.append(String.format(missing, type, StringUtils.join(required, ",")));
        }

        return errors;
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
                // debug?
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

    /**
     * Read an info file (bag-info.txt, aptrust-info.txt, etc)
     *
     * @param info the {@link Path} to the info file to read
     * @return a mapping of keys to values read from the info file
     * @throws IOException if a file cannot be read
     */
    private static Map<String, String> readInfo(final Path info) throws IOException {
        logger.debug("Trying to read info file {}", info);
        final Map<String, String> data = new HashMap<>();
        final AtomicReference<String> previousKey = new AtomicReference<>("");

        // if a line starts indented, it is part of the previous key so we track what key we're working on
        try (Stream<String> lines = Files.lines(info)) {
            lines.forEach(line -> {
                if (line.matches("^\\s+")) {
                    data.merge(previousKey.get(), line, String::concat);
                } else {
                    final String[] split = line.split(":");
                    final String key = split[0].trim();
                    final String value = split[1].trim();
                    previousKey.set(key);
                    data.put(key, value);
                }
            });
        }

        return data;
    }

}
