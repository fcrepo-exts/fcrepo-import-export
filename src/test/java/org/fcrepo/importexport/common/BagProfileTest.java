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
    public void testFromFile() throws Exception {
        final File testFile = new File("src/test/resources/profiles/test.json");
        final BagProfile profile = new BagProfile(new FileInputStream(testFile));

        assertTrue(profile.getPayloadDigestAlgorithms().contains("md5"));
        assertTrue(profile.getPayloadDigestAlgorithms().contains("sha1"));
        assertTrue(profile.getPayloadDigestAlgorithms().contains("sha256"));

        assertFalse(profile.getTagDigestAlgorithms().contains("md5"));
        assertTrue(profile.getTagDigestAlgorithms().contains("sha1"));
        assertFalse(profile.getTagDigestAlgorithms().contains("sha256"));

        assertTrue(profile.getMetadataFields().keySet().contains("Source-Organization"));
        assertTrue(profile.getMetadataFields().keySet().contains("Organization-Address"));
        assertTrue(profile.getMetadataFields().keySet().contains("Contact-Name"));
        assertTrue(profile.getMetadataFields().keySet().contains("Contact-Phone"));
        assertTrue(profile.getMetadataFields().keySet().contains("Bag-Size"));
        assertTrue(profile.getMetadataFields().keySet().contains("Bagging-Date"));
        assertTrue(profile.getMetadataFields().keySet().contains("Payload-Oxum"));
        assertFalse(profile.getMetadataFields().keySet().contains("Contact-Email"));

        assertTrue(profile.getAPTrustFields().keySet().contains("Title"));
        assertTrue(profile.getAPTrustFields().keySet().contains("Access"));
        assertTrue(profile.getAPTrustFields().get("Access").contains("Consortia"));
        assertTrue(profile.getAPTrustFields().get("Access").contains("Institution"));
        assertTrue(profile.getAPTrustFields().get("Access").contains("Restricted"));
    }

    public void testexport() {
        assertTrue( true );
    }
}
