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

import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for profile validation.
 *
 * @author dbernstein
 * @since Dec 14, 2016
 */
public class ProfileValidationUtilTest {

    private static final String FIELD1 = "field1";

    private static final String FIELD2 = "field2";

    private Map<String, ProfileFieldRule> rules;

    private LinkedHashMap<String, String> fields;

    @Before
    public void setup() {
        rules = new HashMap<>();
        fields = new LinkedHashMap<>();
        final Set<String> set = new HashSet<>();
        set.add("value1");
        set.add("value2");
        set.add("value3");
        final ProfileFieldRule field = new ProfileFieldRule(true, false, "", set);
        rules.put(FIELD1, field);
    }

    @Test
    public void testEnforceValues() throws ProfileValidationException {
        fields.put(FIELD1, "value1");
        ProfileValidationUtil.validate("profile-section", rules, fields);
    }

    @Test(expected = ProfileValidationException.class)
    public void testEnforceValuesMissingRequired() throws ProfileValidationException {
        fields.put("field2", "value1");
        ProfileValidationUtil.validate("profile-section", rules, fields);
    }

    @Test(expected = ProfileValidationException.class)
    public void testEnforceValuesInvalidValue() throws ProfileValidationException {
        fields.put(FIELD1, "invalidValue");
        ProfileValidationUtil.validate("profile-section", rules, fields);
    }

    @Test
    public void testMultipleValidationErrorsInOneExceptionMessage() {
        fields.put(FIELD1, "invalidValue");
        rules.put(FIELD2, new ProfileFieldRule(true, true, "field 2 should fail", Collections.emptySet()));
        fields.put("field3", "any value");
        try {
            ProfileValidationUtil.validate("profile-section", rules, fields);
            Assert.fail("previous line should have failed.");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains(FIELD1));
            Assert.assertTrue(e.getMessage().contains(FIELD2));
            Assert.assertFalse(e.getMessage().contains("field3"));
        }
    }

    @Test
    public void testIgnoreSystemGeneratedFields() throws Exception {
        fields.put(FIELD1, "value1");

        for (String fieldName : ProfileValidationUtil.SYSTEM_GENERATED_FIELD_NAMES) {
            rules.put(fieldName, null);
        }

        ProfileValidationUtil.validate("profile-section", rules, fields);

    }

    @Test
    public void testGlobalTagMatch() throws ProfileValidationException {
        Set<String> allowedTags = Collections.singleton("**");
        ProfileValidationUtil.validateTagIsAllowed(Paths.get("test-info.txt"), allowedTags);
        ProfileValidationUtil.validateTagIsAllowed(Paths.get("test-info/test-info.txt"), allowedTags);
    }

    @Test
    public void testEmptyListValidates() throws ProfileValidationException {
        ProfileValidationUtil.validateTagIsAllowed(Paths.get("test-info.txt"), Collections.emptySet());
    }

    @Test
    public void testUniqueTagMatch() throws ProfileValidationException {
        Set<String> allowedTags = Collections.singleton("test-info.txt");
        ProfileValidationUtil.validateTagIsAllowed(Paths.get("test-info.txt"), allowedTags);
    }

    @Test(expected = ProfileValidationException.class)
    public void testTagIsNotAllowed() throws ProfileValidationException {
        Set<String> allowedTags = Collections.singleton("test-tag.txt");
        ProfileValidationUtil.validateTagIsAllowed(Paths.get("test-info.txt"), allowedTags);
    }

    @Test
    public void testSubDirectoryMatch() throws ProfileValidationException {
        Set<String> allowedTags = Collections.singleton("ddp-tags/test-*");
        ProfileValidationUtil.validateTagIsAllowed(Paths.get("ddp-tags/test-info.txt"), allowedTags);
        ProfileValidationUtil.validateTagIsAllowed(Paths.get("ddp-tags/test-extra-info.txt"), allowedTags);
    }

    @Test
    public void testTagValidateIgnoresRequired() throws ProfileValidationException {
        Set<String> allowedTags = Collections.singleton("test-info.txt");
        ProfileValidationUtil.validateTagIsAllowed(Paths.get("bag-info.txt"), allowedTags);
        ProfileValidationUtil.validateTagIsAllowed(Paths.get("bagit.txt"), allowedTags);
        ProfileValidationUtil.validateTagIsAllowed(Paths.get("manifest-md5.txt"), allowedTags);
        ProfileValidationUtil.validateTagIsAllowed(Paths.get("tagmanifest-sha512.txt"), allowedTags);
    }

}
