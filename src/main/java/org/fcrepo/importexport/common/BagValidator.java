package org.fcrepo.importexport.common;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
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
            errors.append("Profile does not allow a fetch.txt but fetch files found.\n");
        }

        // check manifest algorithms (required + allowed)
        errors.append(checkManifests(bag.getPayLoadManifests(), profile.getPayloadDigestAlgorithms(),
                                     profile.getAllowedPayloadAlgorithms(), "payload"));
        errors.append(checkManifests(bag.getTagManifests(), profile.getTagDigestAlgorithms(),
                                     profile.getAllowedTagAlgorithms(), "tag"));

        // check tag files (required + allowed)

        // directory stream + filter (for manifest, bagit.txt, bag-info.txt)
        final Set<String> requiredTagFiles = profile.getTagFilesRequired();
        for (String requiredTag : requiredTagFiles) {
            if (!root.resolve(requiredTag).toFile().exists()) {
                errors.append("Required tag file \"\" does not exist.");
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
            } catch (ProfileValidationException e) {
                // error - section did not validate
            }

        }

        // check allowed bagit versions
        final Set<String> acceptedVersions = profile.getAcceptedBagItVersions();
        if (!acceptedVersions.contains(bag.getVersion().toString())) {
            errors.append("Version incompatible; accepted versions are ")
                  .append(StringUtils.join(acceptedVersions, ","))
                  .append("\n");
        }

        // check serialization (?)

        if (errors.length() > 0) {
            throw new RuntimeException("Bag profile validation failure: The following errors occurred: \n" +
                                       errors.toString());
        }

    }

    private static StringBuilder checkManifests(final Set<Manifest> manifests, final Set<String> required,
                                                final Set<String> allowed, final String type) {
        final String missing = "Missing %s manifest algorithm: %s";
        final String unsupported = "Unsupported %s manifest algorithm: %s";
        final StringBuilder errors = new StringBuilder();

        for (Manifest manifest : manifests) {
            String algorithm = manifest.getAlgorithm().getBagitName();
            required.remove(algorithm);

            if (!allowed.contains(algorithm)) {
                errors.append(String.format(unsupported, type, algorithm));
            }
        }

        if (!required.isEmpty()) {
            errors.append(String.format(missing, type, StringUtils.join(required, ",")));
        }

        return errors;
    }

    private static Map<String, String> readInfo(Path info) throws IOException {
        final Map<String, String> data = new HashMap<>();
        final AtomicReference<String> previousKey = new AtomicReference<>("");

        // need to read a Bag-Info class for the fields and collect them into a Map<String, String>
        // note: if a line starts indented, it is part of the previous key so we should we aware of what key
        // we're working on
        try (Stream<String> lines = Files.lines(info)) {
            lines.forEach(line -> {
                if (line.matches("^\\s+")) {
                    data.merge(previousKey.get(), line, String::concat);
                } else {
                    final String[] split = line.split(":");
                    final String key = split[0].trim();
                    final String value = split[1].trim();
                    previousKey.getAndSet(key);
                    data.put(key, value);
                }
            });
        }

        return data;
    }

}
