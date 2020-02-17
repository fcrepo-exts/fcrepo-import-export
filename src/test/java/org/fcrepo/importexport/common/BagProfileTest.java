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
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.net.URL;
import java.util.Collections;

import org.junit.Assert;
import gov.loc.repository.bagit.domain.Bag;
import gov.loc.repository.bagit.domain.FetchItem;
import gov.loc.repository.bagit.domain.Manifest;
import gov.loc.repository.bagit.domain.Version;
import gov.loc.repository.bagit.hash.StandardSupportedAlgorithms;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author escowles
 * @since 2016-12-13
 */
public class BagProfileTest {

    private final String testValue = "test-value";
    private final String defaultBag = "/bag";
    private final String defaultProfile = "src/test/resources/profiles/profile.json";
    private final String bagInfoIdentifier = "Bag-Info";

    private final Version defaultVersion = new Version(1, 0);
    private final String targetDir = "src/test/resources/sample";

    private final Logger logger = LoggerFactory.getLogger(BagProfileTest.class);

    @Test
    public void testBasicProfileFromFile() throws Exception {
        final File testFile = new File("src/test/resources/profiles/profile.json");
        final BagProfile profile = new BagProfile(new FileInputStream(testFile));

        assertTrue(profile.getPayloadDigestAlgorithms().contains("md5"));
        assertTrue(profile.getPayloadDigestAlgorithms().contains("sha1"));
        assertTrue(profile.getPayloadDigestAlgorithms().contains("sha256"));
        assertTrue(profile.getPayloadDigestAlgorithms().contains("sha512"));

        assertFalse(profile.getTagDigestAlgorithms().contains("md5"));
        assertTrue(profile.getTagDigestAlgorithms().contains("sha1"));
        assertTrue(profile.getTagDigestAlgorithms().contains("sha256"));
        assertTrue(profile.getTagDigestAlgorithms().contains("sha512"));

        assertTrue(profile.getMetadataFields().get("Source-Organization").isRequired());
        assertTrue(profile.getMetadataFields().get("Organization-Address").isRequired());
        assertTrue(profile.getMetadataFields().get("Contact-Name").isRequired());
        assertTrue(profile.getMetadataFields().get("Contact-Phone").isRequired());
        assertTrue(profile.getMetadataFields().get("Bag-Size").isRequired());
        assertTrue(profile.getMetadataFields().get("Bagging-Date").isRequired());
        assertTrue(profile.getMetadataFields().get("Payload-Oxum").isRequired());
        assertFalse(profile.getMetadataFields().get("Contact-Email").isRequired());

        assertFalse(profile.getSectionNames().stream().anyMatch(t -> !t.equalsIgnoreCase(BAG_INFO_FIELDNAME)));

        assertFalse(profile.isAllowFetch());
        assertEquals(BagProfile.Serialization.OPTIONAL, profile.getSerialization());
        assertTrue(profile.getAcceptedBagItVersions().contains("0.97"));
        assertTrue(profile.getAcceptedBagItVersions().contains("1.0"));
        assertTrue(profile.getAcceptedSerializations().contains("application/tar"));
        assertTrue(profile.getTagFilesAllowed().contains("*"));
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

    @Test
    public void testAllProfilesPassValidation() throws IOException {
        final Path profiles = Paths.get("src/main/resources/profiles");

        Files.list(profiles).forEach(path -> {
            logger.debug("Validating {}", path);
            BagProfile profile = null;
            try {
                profile = new BagProfile(Files.newInputStream(path));
            } catch (IOException e) {
                Assert.fail(e.getMessage());
            }

            validateProfile(Objects.requireNonNull(profile));
        });

    }

    @Test
    public void testInvalidBagProfile() throws IOException {
        final File profileFile = new File("src/test/resources/profiles/invalidProfile.json");
        final BagProfile profile = new BagProfile(new FileInputStream(profileFile));
        try {
            validateProfile(profile);
            Assert.fail("Should throw an exception");
        } catch (RuntimeException e) {
            final String message = e.getMessage();
            // check that the error message contains each failed section
            Assert.assertTrue(message.contains(BagProfileConstants.BAGIT_PROFILE_INFO));
            Assert.assertTrue(message.contains(BagProfileConstants.BAGIT_PROFILE_IDENTIFIER));
            Assert.assertTrue(message.contains(BagProfileConstants.ACCEPT_SERIALIZATION));
            Assert.assertTrue(message.contains(BagProfileConstants.MANIFESTS_REQUIRED));
            Assert.assertTrue(message.contains(BagProfileConstants.TAG_MANIFESTS_REQUIRED));
            Assert.assertTrue(message.contains(BagProfileConstants.TAG_FILES_REQUIRED));
            Assert.assertTrue(message.contains(BagProfileConstants.ACCEPT_BAGIT_VERSION));
        }
    }

    @Test
    public void testInvalidBagProfileSerializationTypo() throws IOException {
        final File profileFile = new File("src/test/resources/profiles/invalidProfileSerializationError.json");
        final BagProfile profile = new BagProfile(new FileInputStream(profileFile));
        try {
            validateProfile(profile);
            Assert.fail("Should throw an exception");
        } catch (RuntimeException e) {
            final String message = e.getMessage();
            // check that the serialization field failed to parse
            Assert.assertTrue(message.contains("Unknown Serialization"));
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
    public void validateProfile(final BagProfile profile) {
        final StringBuilder errors = new StringBuilder();

        // Bag-Profile-Info
        final List<String> expectedInfoFields = Arrays.asList(BagConfig.SOURCE_ORGANIZATION_KEY,
                                                              BagConfig.EXTERNAL_DESCRIPTION_KEY,
                                                              BagProfileConstants.PROFILE_VERSION,
                                                              BagProfileConstants.BAGIT_PROFILE_IDENTIFIER,
                                                              BagProfileConstants.BAGIT_PROFILE_VERSION);
        final Map<String, String> bagInfo = profile.getProfileMetadata();
        for (final String expected : expectedInfoFields) {
            if (!bagInfo.containsKey(expected)) {
                if (errors.length() == 0) {
                    errors.append("Error(s) in BagIt-Profile-Info:\n");
                }
                errors.append("  * Missing key ").append(expected).append("\n");
            }
        }

        // Serialization / Accept-Serialization
        final BagProfile.Serialization serialization = profile.getSerialization();
        if (serialization == BagProfile.Serialization.REQUIRED || serialization == BagProfile.Serialization.OPTIONAL) {
            if (profile.getAcceptedSerializations().isEmpty()) {
                errors.append("Serialization value of ").append(serialization)
                      .append(" requires at least one value in the Accept-Serialization field!\n");
            }
        } else if(serialization == BagProfile.Serialization.UNKNOWN) {
            errors.append("Unknown Serialization value ").append(serialization)
                  .append(". Allowed values are forbidden, required, or optional.\n");
        }

        // Manifests-Allowed / Manifests-Required
        final Set<String> allowedPayloadAlgorithms = profile.getAllowedPayloadAlgorithms();
        final Set<String> payloadDigestAlgorithms = profile.getPayloadDigestAlgorithms();
        if (!(allowedPayloadAlgorithms.isEmpty() || isSubset(payloadDigestAlgorithms, allowedPayloadAlgorithms))) {
            errors.append("Manifests-Required must be a subset of Manifests-Allowed!\n");
        }

        // Tag-Manifests-Allowed / Tag-Manifests-Required
        final Set<String> allowedTagAlgorithms = profile.getAllowedTagAlgorithms();
        final Set<String> tagDigestAlgorithms = profile.getTagDigestAlgorithms();
        if (!(allowedTagAlgorithms.isEmpty() || isSubset(tagDigestAlgorithms, allowedTagAlgorithms))) {
            errors.append("Tag-Manifests-Required must be a subset of Tag-Manifests-Allowed!\n");
        }

        // Tag-Files-Allowed / Tag-Files-Required
        final Set<String> tagFilesAllowed = profile.getTagFilesAllowed();
        final Set<String> tagFilesRequired = profile.getTagFilesRequired();
        if (!(tagFilesAllowed.isEmpty() || isSubset(tagFilesRequired, tagFilesAllowed))) {
            errors.append("Tag-Files-Required must be a subset of Tag-Files-Allowed!\n");
        }

        if (profile.getAcceptedBagItVersions().isEmpty()) {
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
     * @return true if all elements of {@code subCollection} are contained within {@code superCollection}
     */
    private <T> boolean isSubset(final Collection<T> subCollection, final Collection<T> superCollection) {
        for (T t : subCollection) {
            if (!superCollection.contains(t)) {
                return false;
            }
        }

        return true;
    }

    @Test
    public void testValidateBag() throws IOException {
        final Bag bag = new Bag();
        bag.setVersion(defaultVersion);
        bag.setRootDir(Paths.get(targetDir, defaultBag));
        final File testFile = new File(defaultProfile);
        final BagProfile bagProfile = new BagProfile(new FileInputStream(testFile));

        putRequiredBagInfo(bag, bagProfile);
        putRequiredManifests(bag.getTagManifests(), bagProfile.getTagDigestAlgorithms());
        putRequiredManifests(bag.getPayLoadManifests(), bagProfile.getPayloadDigestAlgorithms());
        putRequiredTags(bag, bagProfile);

        bagProfile.validateBag(bag);
    }

    @Test
    public void testValidateBagFailure() throws IOException {
        final Long fetchLength = 0L;
        final Path fetchFile = Paths.get("data/fetch.txt");
        final URL fetchUrl = new URL("http://localhost/data/fetch.txt");

        final Bag bag = new Bag();
        bag.setItemsToFetch(Collections.singletonList(new FetchItem(fetchUrl, fetchLength, fetchFile)));
        bag.setVersion(new Version(0, 0));
        bag.setRootDir(Paths.get(targetDir, defaultBag));
        final File testFile = new File("src/main/resources/profiles/aptrust.json");
        final BagProfile bagProfile = new BagProfile(new FileInputStream(testFile));

        putRequiredBagInfo(bag, bagProfile);
        putRequiredManifests(bag.getPayLoadManifests(), bagProfile.getPayloadDigestAlgorithms());
        putRequiredTags(bag, bagProfile);

        try {
            bagProfile.validateBag(bag);
            Assert.fail("Validation did not throw an exception");
        } catch (RuntimeException e) {
            final String message = e.getMessage();
            Assert.assertTrue(message.contains("Profile does not allow a fetch.txt"));
            Assert.assertTrue(message.contains("No tag manifest"));
            Assert.assertTrue(message.contains("Required tag file \"aptrust-info.txt\" does not exist"));
            Assert.assertTrue(message.contains("Could not read info from \"aptrust-info.txt\""));
            Assert.assertTrue(message.contains("BagIt version incompatible"));

            Assert.assertFalse(message.contains("Missing tag manifest algorithm"));
        }
    }

    /**
     * Add required tag files to a Bag from a BagProfile
     *
     * @param bag the Bag
     * @param bagProfile the BagProfile defining the required files
     */
    private void putRequiredTags(final Bag bag, final BagProfile bagProfile) {
        final List<String> tagManifestExpected = Arrays.asList("manifest-sha1.txt", "bag-info.txt", "bagit.txt");

        // Always populate with the files we expect to see
        for (String expected : tagManifestExpected) {
            final Path required = Paths.get(expected);
            for (Manifest manifest : bag.getTagManifests()) {
                manifest.getFileToChecksumMap().put(required, testValue);
            }
        }

        for (String requiredTag : bagProfile.getTagFilesRequired()) {
            final Path requiredPath = Paths.get(requiredTag);
            for (Manifest manifest : bag.getTagManifests()) {
                manifest.getFileToChecksumMap().put(requiredPath, testValue);
            }
        }
    }

    /**
     *
     * @param manifests the manifests to add algorithms to
     * @param algorithms the algorithms to add
     */
    private void putRequiredManifests(final Set<Manifest> manifests, final Set<String> algorithms) {
        for (String algorithm : algorithms) {
            manifests.add(new Manifest(StandardSupportedAlgorithms.valueOf(algorithm.toUpperCase())));
        }
    }

    /**
     *
     * @param bag the Bag to set info fields for
     * @param profile the BagProfile defining the required info fields
     */
    private void putRequiredBagInfo(final Bag bag, final BagProfile profile) {
        final Map<String, ProfileFieldRule> bagInfoMeta = profile.getMetadataFields(bagInfoIdentifier);
        for (Map.Entry<String, ProfileFieldRule> entry : bagInfoMeta.entrySet()) {
            if (entry.getValue().isRequired())  {
                bag.getMetadata().add(entry.getKey(), testValue);
            }
        }
    }
}
