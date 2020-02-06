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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;

import org.junit.Test;

/**
 * @author escowles
 * @since 2016-12-13
 */
public class BagProfileTest {

    @Test
    public void testBasicProfileFromFile() throws Exception {
        final File testFile = new File("src/test/resources/profiles/profile.json");
        final BagProfile profile = new BagProfile(new FileInputStream(testFile));

        assertTrue(profile.getPayloadDigestAlgorithms().contains("md5"));
        assertTrue(profile.getPayloadDigestAlgorithms().contains("sha1"));
        assertTrue(profile.getPayloadDigestAlgorithms().contains("sha256"));

        assertFalse(profile.getTagDigestAlgorithms().contains("md5"));
        assertTrue(profile.getTagDigestAlgorithms().contains("sha1"));
        assertFalse(profile.getTagDigestAlgorithms().contains("sha256"));

        assertTrue(profile.getMetadataFields().get("Source-Organization").isRequired());
        assertTrue(profile.getMetadataFields().get("Organization-Address").isRequired());
        assertTrue(profile.getMetadataFields().get("Contact-Name").isRequired());
        assertTrue(profile.getMetadataFields().get("Contact-Phone").isRequired());
        assertTrue(profile.getMetadataFields().get("Bag-Size").isRequired());
        assertTrue(profile.getMetadataFields().get("Bagging-Date").isRequired());
        assertTrue(profile.getMetadataFields().get("Payload-Oxum").isRequired());
        assertFalse(profile.getMetadataFields().get("Contact-Email").isRequired());

        assertFalse(
            profile.getSectionNames().stream().filter(t -> !t.equalsIgnoreCase(BAG_INFO_FIELDNAME)).count() > 0);

        assertFalse(profile.isAllowFetch());
        assertEquals("optional", profile.getSerialization());
        assertTrue(profile.getAcceptedBagItVersions().contains("0.97"));
        assertTrue(profile.getAcceptedSerializations().isEmpty());
        assertTrue(profile.getTagFilesAllowed().isEmpty());
        assertTrue(profile.getTagFilesRequired().isEmpty());
        assertTrue(profile.getAllowedTagAlgorithms().isEmpty());
        assertTrue(profile.getAllowedPayloadAlgorithms().isEmpty());
    }

    @Test
    public void testExtendedProfile() throws Exception {
        final String aptrustInfo = "APTrust-Info";
        final File testFile = new File("src/test/resources/profiles/profileWithExtraTags.json");
        final BagProfile profile = new BagProfile(new FileInputStream(testFile));

        assertTrue(profile.getSectionNames().stream().filter(t -> !t.equalsIgnoreCase(BAG_INFO_FIELDNAME)).count() > 0);
        assertTrue(profile.getSectionNames().stream().anyMatch(t -> t.equals(aptrustInfo)));
        assertFalse(profile.getSectionNames().stream().anyMatch(t -> t.equals("Wrong-Tags")));
        assertTrue(profile.getMetadataFields(aptrustInfo).containsKey("Title"));
        assertTrue(profile.getMetadataFields(aptrustInfo).containsKey("Access"));
        assertTrue(profile.getMetadataFields(aptrustInfo).get("Access").getValues().contains("Consortia"));
        assertTrue(profile.getMetadataFields(aptrustInfo).get("Access").getValues().contains("Institution"));
        assertTrue(profile.getMetadataFields(aptrustInfo).get("Access").getValues().contains("Restricted"));

    }

    @Test
    public void testGoodConfig() throws Exception {
        final File configFile = new File("src/test/resources/configs/bagit-config.yml");
        final BagConfig config = new BagConfig(configFile);
        final File profileFile = new File("src/test/resources/profiles/profileWithExtraTags.json");
        final BagProfile profile = new BagProfile(new FileInputStream(profileFile));
        profile.validateConfig(config);
    }

    @Test(expected = RuntimeException.class)
    public void testBadAccessValue() throws Exception {
        final File configFile = new File("src/test/resources/configs/bagit-config-bad-access.yml");
        final BagConfig config = new BagConfig(configFile);
        final File profileFile = new File("src/test/resources/profiles/profileWithExtraTags.json");
        final BagProfile profile = new BagProfile(new FileInputStream(profileFile));
        profile.validateConfig(config);
    }

    @Test(expected = RuntimeException.class)
    public void testMissingAccessValue() throws Exception {
        final File configFile = new File("src/test/resources/configs/bagit-config-missing-access.yml");
        final BagConfig config = new BagConfig(configFile);
        final File profileFile = new File("src/test/resources/profiles/profileWithExtraTags.json");
        final BagProfile profile = new BagProfile(new FileInputStream(profileFile));
        profile.validateConfig(config);
    }

    @Test
    public void testMissingSectionNotNeeded() throws Exception {
        final File configFile = new File("src/test/resources/configs/bagit-config-no-aptrust.yml");
        final BagConfig config = new BagConfig(configFile);
        final File profileFile = new File("src/test/resources/profiles/profile.json");
        final BagProfile profile = new BagProfile(new FileInputStream(profileFile));
        profile.validateConfig(config);
    }

    @Test(expected = RuntimeException.class)
    public void testMissingSectionRequired() throws Exception {
        final File configFile = new File("src/test/resources/configs/bagit-config-no-aptrust.yml");
        final BagConfig config = new BagConfig(configFile);
        final File profileFile = new File("src/test/resources/profiles/profileWithExtraTags.json");
        final BagProfile profile = new BagProfile(new FileInputStream(profileFile));
        profile.validateConfig(config);
    }

    public void testexport() {
        assertTrue( true );
    }
}
