package org.fcrepo.importexport.common;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import gov.loc.repository.bagit.domain.Bag;
import gov.loc.repository.bagit.domain.Manifest;
import gov.loc.repository.bagit.domain.Version;
import gov.loc.repository.bagit.hash.StandardSupportedAlgorithms;
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 * @author mikejritter
 * @since 2020-01-24
 */
public class BagValidatorTest {

    private final String TARGET_DIR = System.getProperty("project.build.directory");

    @Test
    public void testValidatePass() throws IOException {
        final Bag bag = new Bag();
        bag.setVersion(new Version(1, 0));
        bag.setRootDir(Paths.get(TARGET_DIR + "/test-classes/sample/bag").toFile());
        final File testFile = new File("src/main/resources/profiles/beyondtherepository.json");
        final BagProfile bagProfile = new BagProfile(new FileInputStream(testFile));

        putRequiredBagInfo(bag, bagProfile);
        putRequiredManifests(bag, bagProfile);
        putRequiredTags(bag, bagProfile);

        BagValidator.validate(bag, bagProfile);
    }

    private void putRequiredTags(Bag bag, BagProfile bagProfile) {
        for (String requiredTag : bagProfile.getTagFilesRequired()) {
            Path requiredPath = Paths.get(requiredTag);
            bag.getTagManifests().forEach(manifest ->
                                              manifest.getFileToChecksumMap().put(requiredPath.toFile(), "test-value"));
        }
    }

    private void putRequiredManifests(Bag bag, BagProfile bagProfile) {
        for (String algorithm : bagProfile.getTagDigestAlgorithms()) {
            Manifest manifest = new Manifest(StandardSupportedAlgorithms.valueOf(algorithm.toUpperCase()));
            bag.getTagManifests().add(manifest);
        }

        for (String algorithm : bagProfile.getPayloadDigestAlgorithms()) {
            Manifest manifest = new Manifest(StandardSupportedAlgorithms.valueOf(algorithm.toUpperCase()));
            bag.getPayLoadManifests().add(manifest);
        }
    }

    @Test
    @Ignore
    public void testValidateNoFetchButExists() {
    }

    @Test
    @Ignore
    public void testValidateNoFetchButNotEmpty() {
    }

    @Test
    @Ignore
    public void testValidateUnsupportedManifest() {
    }

    @Test
    @Ignore
    public void testValidateMissingRequiredManifest() {
    }

    @Test
    @Ignore
    public void testValidateUnsupportedTag() {
    }

    @Test
    @Ignore
    public void testValidateMissingRequiredTag() {
    }

    @Test
    @Ignore
    public void testValidateMissingExtraInfo() {
    }

    @Test
    @Ignore
    public void testValidateFailsExtraInfo() {
    }

    @Test
    @Ignore
    public void testValidateUnsupportedVersion() {
    }

    @Test
    @Ignore
    public void testValidateIsSerialized() {
    }

    private void putRequiredBagInfo(Bag bag, BagProfile profile) {
        Map<String, ProfileFieldRule> bagInfoMeta = profile.getMetadataFields("Bag-Info");
        for (Map.Entry<String, ProfileFieldRule> entry : bagInfoMeta.entrySet()) {
            if (entry.getValue().isRequired())  {
                bag.getMetadata().put(entry.getKey(), "test-value");
            }
        }
    }
}
