package org.fcrepo.importexport.common;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import gov.loc.repository.bagit.domain.Bag;
import gov.loc.repository.bagit.domain.Manifest;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Straight forward no nonsense validator. Might integrate with {@link ProfileValidationUtil}....
 *
 * @author mikejritter
 * @since 2020-01-22
 */
public class BagValidator {

    public static final Logger logger = LoggerFactory.getLogger(BagValidator.class);

    /**
     * Validate a {@link Bag} against a set of rules defined by a {@link BagProfile}
     *
     * @param bag the {@link Bag} to validate
     * @param profile the {@link BagProfile} to validate against
     */
    public static void validate(Bag bag, BagProfile profile) {
        logger.info("Starting Bag to BagProfile conformance validator");
        final StringBuilder errors = new StringBuilder();

        final Path root = bag.getRootDir().toPath();

        // check fetch rule
        if (!profile.isAllowFetch() && (!bag.getItemsToFetch().isEmpty() || Files.exists(root.resolve("fetch.txt")))) {
            errors.append("Profile does not allow a fetch.txt but fetch file found!\n");
        }

        // check manifest algorithms (required + allowed)
        errors.append(checkManifests(bag.getPayLoadManifests(), profile.getPayloadDigestAlgorithms(),
                                     profile.getAllowedPayloadAlgorithms(), "payload"));
        errors.append(checkManifests(bag.getTagManifests(), profile.getTagDigestAlgorithms(),
                                     profile.getAllowedTagAlgorithms(), "tag"));

        // check tag files allowed
        try {
            checkAllowedTagFiles(bag.getTagManifests(), profile.getTagFilesAllowed());
        } catch (ProfileValidationException e) {
            errors.append(e.getMessage());
        }

        // check tag files required
        // directory stream + filter (for manifest, bagit.txt, bag-info.txt)
        final Set<String> requiredTagFiles = profile.getTagFilesRequired();
        for (String requiredTag : requiredTagFiles) {
            if (!root.resolve(requiredTag).toFile().exists()) {
                errors.append("Required tag file \"").append(requiredTag).append("\" does not exist!\n");
            }
        }

        // check *-info required fields
        for (String sectionName : profile.getSectionNames()) {
            final Path resolved = root.resolve(sectionName.toLowerCase() + ".txt");
            try {
                final Map<String, String> infoData = readInfo(resolved);
                ProfileValidationUtil.validate(sectionName, profile.getMetadataFields(sectionName), infoData);
            } catch (IOException e) {
                // error - could not read info
                errors.append("Could not read file ").append(sectionName.toLowerCase()).append(".txt").append("!\n");
            } catch (ProfileValidationException e) {
                errors.append(e.getMessage());
            }

        }

        // check allowed bagit versions
        final Set<String> acceptedVersions = profile.getAcceptedBagItVersions();
        if (!acceptedVersions.contains(bag.getVersion().toString())) {
            errors.append("Version incompatible; accepted versions are ")
                  .append(StringUtils.join(acceptedVersions, ","))
                  .append("\n");
        }

        // serialization seems unnecessary as the import export tool does not support importing serialized bags
        if (profile.getSerialization().equalsIgnoreCase("required")) {
            errors.append("Serialization is not supported in the import export utility\n");
        }

        if (errors.length() > 0) {
            throw new RuntimeException("Bag profile validation failure: The following errors occurred: \n" +
                                       errors.toString());
        }

    }

    private static void checkAllowedTagFiles(final Set<Manifest> tagManifests,
                                             final Set<String> tagDigestAlgorithms) throws ProfileValidationException {
        if (!tagManifests.isEmpty()) {
            final Manifest manifest = tagManifests.iterator().next();
            final HashMap<File, String> fileToChecksumMap = manifest.getFileToChecksumMap();

            // As this was parsed by the LoC library, we should know that it conforms to the BagIt expectations
            // so we should not see any back references or absolute paths - but if they are seen they should fail on
            // validation (we are only comparing the file paths, no opportunity for reading sys files)
            for (File file : fileToChecksumMap.keySet()) {
                Path path = file.toPath();
                ProfileValidationUtil.validateTagIsAllowed(path, tagDigestAlgorithms);
            }
        } else {
            throw new ProfileValidationException("No tag manifest found!\n");
        }
    }

    private static StringBuilder checkManifests(final Set<Manifest> manifests, final Set<String> required,
                                                final Set<String> allowed, final String type) {
        final String missing = "Missing %s manifest algorithm: %s\n";
        final String unsupported = "Unsupported %s manifest algorithm: %s\n";
        final StringBuilder errors = new StringBuilder();
        final Set<String> requiredCopy = new HashSet<>(required);

        for (Manifest manifest : manifests) {
            String algorithm = manifest.getAlgorithm().getBagitName().toLowerCase();
            logger.debug("Found {} manifest algorithm {}", type, algorithm);
            requiredCopy.remove(algorithm);

            if (!allowed.contains(algorithm)) {
                errors.append(String.format(unsupported, type, algorithm));
            }
        }

        if (!requiredCopy.isEmpty()) {
            errors.append(String.format(missing, type, StringUtils.join(required, ",")));
        }

        return errors;
    }

    private static Map<String, String> readInfo(Path info) throws IOException {
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
