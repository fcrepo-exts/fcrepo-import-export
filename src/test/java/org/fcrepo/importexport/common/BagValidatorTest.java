package org.fcrepo.importexport.common;


import static java.util.Collections.emptySet;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import gov.loc.repository.bagit.domain.Bag;
import gov.loc.repository.bagit.domain.FetchItem;
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

    private final String testValue = "test-value";
    private final String defaultBag = "/test-classes/sample/bag";
    private final String defaultProfile = "src/test/resources/profiles/profile.json";
    private final String bagInfoIdentifier = "Bag-Info";

    private final Version defaultVersion = new Version(1, 0);
    private final String targetDir = System.getProperty("project.build.directory");
    private final List<String> tagManifestExpected = Arrays.asList("manifest-sha1.txt", "bag-info.txt", "bagit.txt");

    @Test
    public void testValidatePass() throws IOException {
        final Bag bag = new Bag();
        bag.setVersion(defaultVersion);
        bag.setRootDir(Paths.get(targetDir + defaultBag).toFile());
        final File testFile = new File(defaultProfile);
        final BagProfile bagProfile = new BagProfile(new FileInputStream(testFile));

        putRequiredBagInfo(bag, bagProfile);
        putRequiredManifests(bag.getTagManifests(), bagProfile.getTagDigestAlgorithms());
        putRequiredManifests(bag.getPayLoadManifests(), bagProfile.getPayloadDigestAlgorithms());
        putRequiredTags(bag, bagProfile);

        BagValidator.validate(bag, bagProfile);
    }

    @Test
    public void testValidateNoFetchButExists() throws IOException {
        final Bag bag = new Bag();
        bag.setVersion(defaultVersion);
        bag.setRootDir(Paths.get(targetDir + defaultBag).toFile());
        final File testFile = new File(defaultProfile);
        final BagProfile bagProfile = new BagProfile(new FileInputStream(testFile));

        putRequiredBagInfo(bag, bagProfile);
        putRequiredManifests(bag.getTagManifests(), bagProfile.getTagDigestAlgorithms());
        putRequiredManifests(bag.getPayLoadManifests(), bagProfile.getPayloadDigestAlgorithms());
        putRequiredTags(bag, bagProfile);
    }

    @Test(expected = RuntimeException.class)
    public void testValidateNoFetchButNotEmpty() throws IOException {
        final Long fetchSize = 1L;
        final String fetchFile = "data/fetch-one.txt";
        final URL fetchUrl = new URL("http://localhost/fetch-one.txt");

        final Bag bag = new Bag();
        bag.getItemsToFetch().add(new FetchItem(fetchUrl, fetchSize, fetchFile));
        bag.setVersion(defaultVersion);
        bag.setRootDir(Paths.get(targetDir + defaultBag).toFile());
        final File testFile = new File(defaultProfile);
        final BagProfile bagProfile = new BagProfile(new FileInputStream(testFile));

        putRequiredBagInfo(bag, bagProfile);
        putRequiredManifests(bag.getTagManifests(), bagProfile.getTagDigestAlgorithms());
        putRequiredManifests(bag.getPayLoadManifests(), bagProfile.getPayloadDigestAlgorithms());
        putRequiredTags(bag, bagProfile);

        BagValidator.validate(bag, bagProfile);
    }

    @Test(expected = RuntimeException.class)
    public void testValidateUnsupportedManifestAlgorithm() throws IOException {
        String supported = StandardSupportedAlgorithms.SHA1.getBagitName().toLowerCase();
        String unsupported = StandardSupportedAlgorithms.MD5.getBagitName().toLowerCase();

        final Bag bag = new Bag();
        bag.setVersion(defaultVersion);
        bag.setRootDir(Paths.get(targetDir + defaultBag).toFile());
        final File testFile = new File(defaultProfile);

        // Only allow SHA-1 as a supported payload manifest algorithm
        final BagProfile bagProfile = new BagProfile(new FileInputStream(testFile));
        bagProfile.getAllowedPayloadAlgorithms().clear();
        bagProfile.getAllowedPayloadAlgorithms().add(supported);

        putRequiredBagInfo(bag, bagProfile);
        putRequiredManifests(bag.getTagManifests(), bagProfile.getTagDigestAlgorithms());
        putRequiredManifests(bag.getPayLoadManifests(), bagProfile.getPayloadDigestAlgorithms());
        putRequiredTags(bag, bagProfile);
        putRequiredManifests(bag.getPayLoadManifests(), Collections.singleton(unsupported));

        BagValidator.validate(bag, bagProfile);
    }

    @Test(expected = RuntimeException.class)
    public void testValidateMissingRequiredManifest() throws IOException {
        final Bag bag = new Bag();
        bag.setVersion(defaultVersion);
        bag.setRootDir(Paths.get(targetDir + defaultBag).toFile());
        final File testFile = new File(defaultProfile);
        final BagProfile bagProfile = new BagProfile(new FileInputStream(testFile));

        putRequiredBagInfo(bag, bagProfile);
        putRequiredManifests(bag.getTagManifests(), bagProfile.getTagDigestAlgorithms());
        putRequiredManifests(bag.getPayLoadManifests(), bagProfile.getPayloadDigestAlgorithms());
        putRequiredTags(bag, bagProfile);

        // Remove all manifests
        bag.getPayLoadManifests().clear();
        BagValidator.validate(bag, bagProfile);
    }

    @Test(expected = RuntimeException.class)
    public void testValidateUnsupportedTag() throws IOException {
        final Bag bag = new Bag();
        bag.setVersion(defaultVersion);
        bag.setRootDir(Paths.get(targetDir + defaultBag).toFile());
        final File testFile = new File(defaultProfile);
        final BagProfile bagProfile = new BagProfile(new FileInputStream(testFile));

        putRequiredBagInfo(bag, bagProfile);
        putRequiredManifests(bag.getTagManifests(), bagProfile.getTagDigestAlgorithms());
        putRequiredManifests(bag.getPayLoadManifests(), bagProfile.getPayloadDigestAlgorithms());
        putRequiredTags(bag, bagProfile);

        // Remove all manifests
        bag.getTagManifests().clear();
        BagValidator.validate(bag, bagProfile);
    }

    @Test(expected = RuntimeException.class)
    public void testValidateMissingRequiredTag() throws IOException {
        final String requiredInfo = "required-info.txt";

        final Bag bag = new Bag();
        bag.setVersion(defaultVersion);
        bag.setRootDir(Paths.get(targetDir + defaultBag).toFile());
        final File testFile = new File(defaultProfile);
        final BagProfile bagProfile = new BagProfile(new FileInputStream(testFile));
        bagProfile.getTagFilesRequired().add(requiredInfo);

        putRequiredBagInfo(bag, bagProfile);
        putRequiredManifests(bag.getTagManifests(), bagProfile.getTagDigestAlgorithms());
        putRequiredManifests(bag.getPayLoadManifests(), bagProfile.getPayloadDigestAlgorithms());
        putRequiredTags(bag, bagProfile);

        // Remove all manifests
        BagValidator.validate(bag, bagProfile);
    }

    @Test(expected = RuntimeException.class)
    public void testValidateMissingTagManifest() throws IOException {
        final Bag bag = new Bag();
        bag.setVersion(defaultVersion);
        bag.setRootDir(Paths.get(targetDir + defaultBag).toFile());
        final File testFile = new File(defaultProfile);
        final BagProfile bagProfile = new BagProfile(new FileInputStream(testFile));

        putRequiredBagInfo(bag, bagProfile);
        putRequiredManifests(bag.getTagManifests(), bagProfile.getTagDigestAlgorithms());
        putRequiredManifests(bag.getPayLoadManifests(), bagProfile.getPayloadDigestAlgorithms());
        putRequiredTags(bag, bagProfile);

        // Remove all manifests
        bag.getTagManifests().clear();
        BagValidator.validate(bag, bagProfile);
    }

    @Test(expected = RuntimeException.class)
    public void testValidateMissingExtraInfo() throws IOException {
        final String profilePath = "src/test/resources/profiles/profileWithExtraTags.json";

        final Bag bag = new Bag();
        bag.setVersion(defaultVersion);
        bag.setRootDir(Paths.get(targetDir + defaultBag).toFile());
        final File testFile = new File(profilePath);
        final BagProfile bagProfile = new BagProfile(new FileInputStream(testFile));

        putRequiredBagInfo(bag, bagProfile);
        putRequiredManifests(bag.getTagManifests(), bagProfile.getTagDigestAlgorithms());
        putRequiredManifests(bag.getPayLoadManifests(), bagProfile.getPayloadDigestAlgorithms());
        putRequiredTags(bag, bagProfile);

        // Remove all manifests
        BagValidator.validate(bag, bagProfile);
    }

    @Test(expected = RuntimeException.class)
    public void testValidateFailsExtraInfo() throws IOException {
        final Path invalid = Paths.get("extra-info/extra-info.txt");
        final String checksum = "test-checksum";

        final Bag bag = new Bag();
        bag.setVersion(defaultVersion);
        bag.setRootDir(Paths.get(targetDir + defaultBag).toFile());
        final File testFile = new File(defaultProfile);
        final BagProfile bagProfile = new BagProfile(new FileInputStream(testFile));

        putRequiredBagInfo(bag, bagProfile);
        putRequiredManifests(bag.getTagManifests(), bagProfile.getTagDigestAlgorithms());
        putRequiredManifests(bag.getPayLoadManifests(), bagProfile.getPayloadDigestAlgorithms());
        putRequiredTags(bag, bagProfile);
        bag.getTagManifests().forEach(manifest -> manifest.getFileToChecksumMap().put(invalid.toFile(), checksum));

        // Remove all manifests
        BagValidator.validate(bag, bagProfile);
    }

    @Test(expected = RuntimeException.class)
    public void testValidateInfoFailsValidation() throws IOException {
        final String requiredField = "Required-Field";
        final ProfileFieldRule rule = new ProfileFieldRule(true, false, true, requiredField, emptySet());

        final Bag bag = new Bag();
        bag.setVersion(defaultVersion);
        bag.setRootDir(Paths.get(targetDir + defaultBag).toFile());
        final File testFile = new File(defaultProfile);
        final BagProfile bagProfile = new BagProfile(new FileInputStream(testFile));
        bagProfile.getMetadataFields(bagInfoIdentifier).put(requiredField, rule);

        putRequiredBagInfo(bag, bagProfile);
        putRequiredManifests(bag.getTagManifests(), bagProfile.getTagDigestAlgorithms());
        putRequiredManifests(bag.getPayLoadManifests(), bagProfile.getPayloadDigestAlgorithms());
        putRequiredTags(bag, bagProfile);

        // Remove all manifests
        BagValidator.validate(bag, bagProfile);
    }

    @Test(expected = RuntimeException.class)
    public void testValidateUnsupportedVersion() throws IOException {
        final Version invalidVersion = new Version(0, 0);

        final Bag bag = new Bag();
        bag.setVersion(invalidVersion);
        bag.setRootDir(Paths.get(targetDir + defaultBag).toFile());
        final File testFile = new File(defaultProfile);
        final BagProfile bagProfile = new BagProfile(new FileInputStream(testFile));

        putRequiredBagInfo(bag, bagProfile);
        putRequiredManifests(bag.getTagManifests(), bagProfile.getTagDigestAlgorithms());
        putRequiredManifests(bag.getPayLoadManifests(), bagProfile.getPayloadDigestAlgorithms());
        putRequiredTags(bag, bagProfile);

        BagValidator.validate(bag, bagProfile);
    }

    @Test
    @Ignore
    public void testValidateIsSerialized() {
    }

    /**
     * Add the required tag files to a Bag
     *
     * @param bag the Bag
     * @param bagProfile the BagProfile defining the required files
     */
    private void putRequiredTags(final Bag bag, final BagProfile bagProfile) {
        // Always populate with the files we expect to see
        for (String expected : tagManifestExpected) {
            Path required = Paths.get(expected);
            for (Manifest manifest : bag.getTagManifests()) {
                manifest.getFileToChecksumMap().put(required.toFile(), testValue);
            }
        }

        for (String requiredTag : bagProfile.getTagFilesRequired()) {
            Path requiredPath = Paths.get(requiredTag);
            for (Manifest manifest : bag.getTagManifests()) {
                manifest.getFileToChecksumMap().put(requiredPath.toFile(), testValue);
            }
        }
    }

    /**
     * Put the specified algorithms into a Bag
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
     * Set required fields for the Bag-Info.txt file in a Bag
     *
     * @param bag the Bag to set info fields for
     * @param profile the BagProfile defining the required info fields
     */
    private void putRequiredBagInfo(final Bag bag, final BagProfile profile) {
        final Map<String, ProfileFieldRule> bagInfoMeta = profile.getMetadataFields(bagInfoIdentifier);
        for (Map.Entry<String, ProfileFieldRule> entry : bagInfoMeta.entrySet()) {
            if (entry.getValue().isRequired())  {
                bag.getMetadata().put(entry.getKey(), testValue);
            }
        }
    }
}
