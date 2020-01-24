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

import java.util.Set;

/**
 *
 *
 * @author mikejritter
 * @since 2020-01-20
 */
public class ProfileFieldRule {

    private final boolean required;
    private final boolean repeatable;
    private final boolean recommended;
    private final String description;
    private final Set<String> values;

    /**
     * Constructor for a ProfileFieldRule. Takes the 4 possible json fields from a BagIt Profile *-Info field.
     *
     * @param required boolean value stating if this rule is required
     * @param repeatable boolean value allowing a field to be repeated
     * @param recommended boolean value stating if this rule is recommended
     * @param description a text description of this rule
     * @param values a set of string values which a field is allowed to be set to
     */
    public ProfileFieldRule(final boolean required,
                            final boolean repeatable,
                            final boolean recommended,
                            final String description,
                            final Set<String> values) {
        this.required = required;
        this.repeatable = repeatable;
        this.recommended = recommended;
        this.description = description;
        this.values = values;
    }

    /**
     *
     * @return if the field for this rule is required to exist
     */
    public boolean isRequired() {
        return required;
    }

    /**
     *
     * @return if the field is allowed to be repeated
     */
    public boolean isRepeatable() {
        return repeatable;
    }

    /**
     *
     * @return if the field for this rule is recommended to exist
     */
    public boolean isRecommended() {
        return recommended;
    }

    /**
     *
     * @return the description of this rule
     */
    public String getDescription() {
        return description;
    }

    /**
     *
     * @return the allowed values for fields matching this rule
     */
    public Set<String> getValues() {
        return values;
    }

    /**
     * String representation of a ProfileFieldRule
     *
     * @return the string representation
     */
    @Override
    public String toString() {
        return "ProfileFieldRule{" +
               "required=" + required +
               ", recommended=" + recommended +
               ", description='" + description + '\'' +
               ", values=" + values +
               '}';
    }
}
