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

import static java.nio.file.FileVisitResult.CONTINUE;
import static org.fcrepo.importexport.common.FcrepoConstants.CREATED_DATE;
import static org.fcrepo.importexport.common.FcrepoConstants.LAST_MODIFIED_DATE;
import static org.fcrepo.importexport.common.FcrepoConstants.NON_RDF_SOURCE;
import static org.fcrepo.importexport.common.FcrepoConstants.RDF_TYPE;
import static org.fcrepo.importexport.common.ModelUtils.mapRdfStream;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.apache.jena.datatypes.xsd.XSDDateTime;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ResIterator;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.fcrepo.importexport.common.Config;
import org.fcrepo.importexport.common.URITranslationUtil;
import org.slf4j.Logger;

/**
 * Iterates through a deduplicated set of import events in chronological order based on timestamp
 *
 * Generates the list of events from the configured import directory. Events are either resource import or version
 * creation events. They are sorted chronologically by timestamp, using the created timestamp for the original
 * version of the resource, or the last modified timestamp for subsequent versions. Unmodified versions of a
 * resource are also deduplicated if another version with the same last modified timestamp is present.
 *
 * @author bbpennel
 *
 */
public class ChronologicalImportEventIterator implements Iterator<ImportEvent> {

    private static final Logger logger = getLogger(ChronologicalImportEventIterator.class);

    private final Config config;
    private Queue<ImportEvent> eventQueue;
    final File importBaseDirectory;

    /**
     * Constructs a new ChronologicalImportEventIterator from the given base directory
     *
     * @param importBaseDirectory base directory where resources are extracted from
     * @param config config
     */
    public ChronologicalImportEventIterator(final File importBaseDirectory, final Config config) {
        this.config = config;
        this.importBaseDirectory = importBaseDirectory;
    }

    @Override
    public boolean hasNext() {
        if (eventQueue == null) {
            try {
                eventQueue = generateEventQueue();
            } catch (IOException e) {
                throw new RuntimeException("Failed to read resources for import from specified directory "
                        + importBaseDirectory, e);
            }
        }

        return eventQueue.size() > 0;
    }

    @Override
    public ImportEvent next() {
        return eventQueue.remove();
    }

    /**
     * Generates the list of events from the configured import directory. Events are either resource import or version
     * creation events. They are sorted chronologically by timestamp, using the created timestamp for the original
     * version of the resource, or the last modified timestamp for subsequent versions. Unmodified versions of a
     * resource are also deduplicated if another version with the same last modified timestamp is present.
     *
     * @throws IOException thrown if the directory cannot be walked
     */
    private Queue<ImportEvent> generateEventQueue() throws IOException {
        final ChronologicalUriExtractingFileVisitor treeWalker =
                new ChronologicalUriExtractingFileVisitor(this.config);
        Files.walkFileTree(importBaseDirectory.toPath(), treeWalker);

        // Retrieve list of events from the configured directory
        final List<ImportEvent> eventList = treeWalker.getEvents();

        // If including versions, then remove unmodified duplicates and setup sort keys
        if (config.includeVersions()) {
            final Map<String, List<ImportResource>> resourcesGroupedByUri = treeWalker.getResourcesGroupedByUri();
            prepareResources(eventList, resourcesGroupedByUri);
        }

        // Sort the events
        sortEventsChronologically(eventList);

        final Queue<ImportEvent> events = new ArrayDeque<>();
        events.addAll(eventList);

        return events;
    }

    private void prepareResources(final List<ImportEvent> eventList,
            final Map<String, List<ImportResource>> resourcesGroupedByUri) {

        resourcesGroupedByUri.entrySet().forEach(uriGroup -> {
            final List<ImportResource> rescGroup = uriGroup.getValue();
            if (rescGroup.size() <= 1) {
                return;
            }

            // Sort the versions of this resource by last modified
            rescGroup.sort(new Comparator<ImportEvent>() {
                @Override
                public int compare(final ImportEvent o1, final ImportEvent o2) {
                    return new Long(o1.getLastModified()).compareTo(o2.getLastModified());
                }
            });

            // Remove unmodified resources and switch sort key to last modified for all versions after original
            for (int i = 1; i < rescGroup.size(); i++) {
                final ImportResource resc = rescGroup.get(i);
                final long previousLastModified = rescGroup.get(i - 1).getLastModified();

                // Remove the unmodified resource from the total event list
                if (resc.getLastModified() == previousLastModified) {
                    eventList.remove(resc);
                } else {
                    resc.setTimestamp(resc.getLastModified());
                }
            }
        });
    }

    /**
     * Sorts the events list chronologically by timestamp
     *
     * @param eventList
     */
    private List<ImportEvent> sortEventsChronologically(final List<ImportEvent> eventList) {
        eventList.sort(new Comparator<ImportEvent>() {
            @Override
            public int compare(final ImportEvent o1, final ImportEvent o2) {
                return new Long(o1.getTimestamp()).compareTo(o2.getTimestamp());
            }
        });

        return eventList;
    }

    private class ChronologicalUriExtractingFileVisitor extends SimpleFileVisitor<Path> {

        private List<ImportEvent> resources;
        private Map<String, List<ImportResource>> resourcesGroupedByUri;

        private final Config config;

        public ChronologicalUriExtractingFileVisitor(final Config config) {
            resources = new ArrayList<>();
            resourcesGroupedByUri = new HashMap<>();
            this.config = config;
        }

        public List<ImportEvent> getEvents() {
            return resources;
        }

        public Map<String, List<ImportResource>> getResourcesGroupedByUri() {
            return resourcesGroupedByUri;
        }

        @Override
        public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) throws IOException {
            // Skip over files other than descriptions
            if (!file.toString().endsWith(config.getRdfExtension())) {
                return CONTINUE;
            }

            // Create events for versions of this resource
            if (config.includeVersions()
                    && file.toString().endsWith("fcr%3Aversions" + config.getRdfExtension())) {
                addVersionEvents(file.toFile());
                return CONTINUE;
            }

            // If versioning is excluded then skip all past versions of resources
            if (!config.includeVersions() && file.toString().contains("fcr%3Aversions")) {
                return CONTINUE;
            }

            final File rdfFile = file.toFile();
            final Model model = mapRdfStream(new FileInputStream(rdfFile), config);

            // Determine the URI for this resource depending on if it is a binary or not
            final URI resourceUri;
            final ResIterator binaryResources = model.listResourcesWithProperty(RDF_TYPE, NON_RDF_SOURCE);
            if (binaryResources.hasNext()) {
                resourceUri = URI.create(binaryResources.next().getURI());
            } else {
                resourceUri = URITranslationUtil.uriForFile(rdfFile, config);
            }

            // Store the resource along with its last modified date for sorting later
            final Resource resc = model.getResource(resourceUri.toString());
            final Statement lastModStmt = resc.getProperty(LAST_MODIFIED_DATE);
            final Statement createdStmt = resc.getProperty(CREATED_DATE);
//            final List<String> digests = getMessageDigests(resc);

            final long lastModified = lastModStmt == null ? 0L : getTimestampFromProperty(lastModStmt);
            final long created = createdStmt == null ? 0L : getTimestampFromProperty(createdStmt);

            final ImportResource impResc = new ImportResource(resourceUri, rdfFile, created, lastModified, config);

            resources.add(impResc);

            logger.debug("Added resource for import {}", impResc.getUri());

            // If including versions, then store a map of destination uris to all resources with the same uris
            if (config.includeVersions()) {
                final String groupUri = impResc.getMappedUri().toString();
                List<ImportResource> rescsForUri = resourcesGroupedByUri.get(groupUri);
                if (rescsForUri == null) {
                    rescsForUri = new ArrayList<>();
                    resourcesGroupedByUri.put(groupUri, rescsForUri);
                }
                rescsForUri.add(impResc);
            }

            return CONTINUE;
        }

//        private List<String> getMessageDigests(final Resource resc) {
//            StmtIterator stmtIt = resc.listProperties(HAS_MESSAGE_DIGEST);
//            List<String> digests = new ArrayList<>();
//
//            while (stmtIt.hasNext()) {
//                Statement stmt = stmtIt.next();
//                digests.add(stmt.getObject().toString());
//            }
//            if (digests.size() == 0) {
//                return null;
//            }
//            return digests;
//        }

        private void addVersionEvents(final File versionsFile) throws IOException {
            final Model model = mapRdfStream(new FileInputStream(versionsFile), config);

            final ResIterator vRescIt = model.listResourcesWithProperty(CREATED_DATE);
            while (vRescIt.hasNext()) {
                final Resource vResc = vRescIt.next();
                final long time = getTimestampFromProperty(vResc.getProperty(CREATED_DATE));

                final ImportVersion impVersion = new ImportVersion(URI.create(vResc.getURI()), time, config);
                resources.add(impVersion);

                logger.debug("Added version {}", impVersion.getUri());
            }
        }

        private long getTimestampFromProperty(final Statement stmt) {
            final XSDDateTime dateTime = (XSDDateTime) stmt.getLiteral().getValue();
            return dateTime.asCalendar().getTimeInMillis();
        }
    }
}
