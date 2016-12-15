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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

/**
 * Utility methods for validating profiles.
 *
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
                                        "Payload-Oxum"));

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
    public static void validate(final String profileSection, final Map<String, Set<String>> requiredFields,
            final Map<String, String> fields) throws ProfileValidationException {
        if (requiredFields != null) {
            final StringBuilder errors = new StringBuilder();

            for (String fieldName : requiredFields.keySet()) {
                // ignore validation on system generated fields
                if (SYSTEM_GENERATED_FIELD_NAMES.contains(fieldName)) {
                    logger.debug("skipping system generated field {}...", fieldName);
                    continue;
                }

                if (fields.containsKey(fieldName)) {
                    final String value = fields.get(fieldName);
                    final Set<String> validValues = requiredFields.get(fieldName);
                    if (validValues != null && !validValues.isEmpty()) {
                        if (!validValues.contains(value)) {
                            errors.append("\"" + value + "\" is not valid for \"" + fieldName + "\". Valid values: " +
                                    StringUtils.join(validValues, ",") + "\n");
                        }
                    }

                } else {
                    errors.append("\"" + fieldName + "\" is a required field.\n");
                }
            }

            if (errors.length() > 0) {
                throw new ProfileValidationException(
                        "Bag profile validation failure: The following errors occurred in the " +
                                profileSection + ":\n" + errors.toString());
            }
        }

    }

}
