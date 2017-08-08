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
package org.fcrepo.importexport.importer;

import static org.fcrepo.importexport.common.FcrepoConstants.CREATED_DATE;
import static org.fcrepo.importexport.common.FcrepoConstants.FCR_VERSIONS_PATH;
import static org.fcrepo.importexport.common.TransferProcess.directoryForContainer;
import static org.fcrepo.importexport.common.URITranslationUtil.remapResourceUri;
import static org.fcrepo.importexport.common.UriUtils.withSlash;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.File;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.jena.datatypes.xsd.XSDDateTime;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ResIterator;
import org.apache.jena.rdf.model.Resource;
import org.fcrepo.importexport.common.Config;
import org.fcrepo.importexport.common.FcrepoConstants;
import org.slf4j.Logger;

/**
 * Generates events to cause the cleanup of resources that are deleted between versions of a resource tree.
 *
 * @author bbpennel
 */
public class VersionDiffDeletionGenerator {
    private static final Logger logger = getLogger(VersionDiffDeletionGenerator.class);

    private static final String FCR_VERSIONS_ESCAPED = "fcr%3Aversions";

    final Config config;

    /**
     * Constructs a VersionDeletionGenerator
     *
     * @param config config
     */
    public VersionDiffDeletionGenerator(final Config config) {
        this.config = config;
    }

    /**
     * Gets a list of import deletion events from the given fcr:versions model
     *
     * @param versionsModel fcr:versions model
     * @return list of deletion events
     */
    public List<ImportDeletion> generateImportDeletions(final Model versionsModel) {
        final List<TimestampUriPair> timestampedUris = getTimestampSortedVersionUris(versionsModel);

        final Map<URI, Set<String>> versionFileMap = getFilesPerVersion(timestampedUris);

        return makeDeletionListFromFileDiffs(timestampedUris, versionFileMap);
    }

    private List<TimestampUriPair> getTimestampSortedVersionUris(final Model versionsModel) {
        final List<TimestampUriPair> timestampedUris = new ArrayList<>();

        final ResIterator vRescIt = versionsModel.listResourcesWithProperty(CREATED_DATE);
        while (vRescIt.hasNext()) {
            final Resource vResc = vRescIt.next();
            final URI rescUri = URI.create(vResc.getURI());
            final XSDDateTime dateTime = (XSDDateTime) vResc.getProperty(CREATED_DATE).getLiteral().getValue();
            final long time =  dateTime.asCalendar().getTimeInMillis();

            timestampedUris.add(new TimestampUriPair(time, rescUri));
        }

        // Sort the version creation events
        timestampedUris.sort(new Comparator<TimestampUriPair>() {
            @Override
            public int compare(final TimestampUriPair o1, final TimestampUriPair o2) {
                return new Long(o1.timestamp).compareTo(o2.timestamp);
            }
        });

        // Set head timestamp for deletion to just after the most recent version was created
        final long headTimestamp = timestampedUris.get(timestampedUris.size() - 1).timestamp + 1;

        final String headVersionUri = versionsModel
                .listResourcesWithProperty(FcrepoConstants.HAS_VERSION).next().getURI();

        timestampedUris.add(new TimestampUriPair(headTimestamp, URI.create(headVersionUri)));

        return timestampedUris;
    }

    private Map<URI, Set<String>> getFilesPerVersion(final List<TimestampUriPair> timestampedUris) {
        final Map<URI, Set<String>> versionFileMap = new HashMap<>();
        timestampedUris.forEach(timestampedUriPair -> {
            final File versionDir = getVersionDirectory(timestampedUriPair.uri);
            final Path versionPath = versionDir.toPath();

            // No child resources, return empty set of file paths
            if (!versionDir.exists() || !versionDir.isDirectory()) {
                versionFileMap.put(timestampedUriPair.uri, new HashSet<>());
                return;
            }

            final boolean excludeVersionDir = !timestampedUriPair.uri.toString().contains(FCR_VERSIONS_PATH);

            final Set<String> relativeVersionFiles = FileUtils.listFiles(versionDir,
                    new IOFileFilter() {
                        @Override
                        public boolean accept(final File file) {
                            final String absPath = file.toPath().toString();
                            if (excludeVersionDir && absPath.contains(FCR_VERSIONS_ESCAPED)) {
                                return false;
                            }
                            return absPath.endsWith(config.getRdfExtension());
                        }

                        @Override
                        public boolean accept(final File dir, final String name) {
                            return false;
                        }
                    },
                    DirectoryFileFilter.DIRECTORY)
                .stream().map(f -> {
                    return versionPath.relativize(f.toPath()).toString();
                }).collect(Collectors.toCollection(HashSet::new));

            versionFileMap.put(timestampedUriPair.uri, relativeVersionFiles);
        });

        return versionFileMap;
    }

    private List<ImportDeletion> makeDeletionListFromFileDiffs(final List<TimestampUriPair> timestampedUris,
            final Map<URI, Set<String>> versionFileMap) {
        final String fcrMetadataEscaped = "fcr%3Ametadata" + config.getRdfExtension();

        final List<ImportDeletion> deletions = new ArrayList<>();

        for (int i = 1; i < timestampedUris.size(); i++) {
            final TimestampUriPair previousVersion = timestampedUris.get(i - 1);
            final TimestampUriPair currentVersion = timestampedUris.get(i);

            final Set<String> previousFiles = versionFileMap.get(previousVersion.uri);
            final Set<String> currentFiles = versionFileMap.get(currentVersion.uri);

            previousFiles.removeAll(currentFiles);
            previousFiles.forEach(relativeFile -> {
                String relativePath = relativeFile;
                if (relativeFile.endsWith(fcrMetadataEscaped)) {
                    relativePath = relativeFile.substring(0, relativeFile.length() - fcrMetadataEscaped.length() - 1);
                } else if (relativeFile.endsWith(config.getRdfExtension())) {
                    relativePath = relativeFile.substring(0, relativeFile.length() - config.getRdfExtension().length());
                }

                final URI fileUri = URI.create(previousVersion.uri.toString() + "/" + relativePath);
                final URI remappedUri = remapResourceUri(fileUri,
                        config.getSource(), config.getDestination());

                deletions.add(new ImportDeletion(remappedUri, currentVersion.timestamp, config));

                logger.debug("Adding version deletion of {}", remappedUri);
            });

        }

        return deletions;
    }

    private File getVersionDirectory(final URI uri) {
        return directoryForContainer(withSlash(uri), config.getSourcePath(), config.getDestinationPath(),
                config.getBaseDirectory());
    }

    private class TimestampUriPair {
        public final long timestamp;
        public final URI uri;

        public TimestampUriPair(final long timestamp, final URI uri) {
            this.timestamp = timestamp;
            this.uri = uri;
        }
    }
}
