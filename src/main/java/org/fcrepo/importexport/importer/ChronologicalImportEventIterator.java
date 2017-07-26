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
import java.util.Iterator;
import java.util.List;
import java.util.Queue;

import org.apache.jena.datatypes.xsd.XSDDateTime;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ResIterator;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.fcrepo.importexport.common.Config;
import org.fcrepo.importexport.common.FcrepoConstants;
import org.fcrepo.importexport.common.URITranslationUtil;
import org.slf4j.Logger;

/**
 * Iterates through resources in chronological order based on last modified timestamp 
 * 
 * @author bbpennel
 *
 */
public class ChronologicalImportEventIterator implements Iterator<ImportEvent> {

    private static final Logger logger = getLogger(ChronologicalImportEventIterator.class);

    private final Config config;
    private final ChronologicalUriExtractingFileVisitor treeWalker;
    private Queue<ImportEvent> eventQueue;

    private ImportResourceFactory rescFactory;

    public ChronologicalImportEventIterator(final File importBaseDirectory, final Config config, final ImportResourceFactory rescFactory)
            throws IOException {
        this.config = config;
        this.rescFactory = rescFactory;

        this.treeWalker = new ChronologicalUriExtractingFileVisitor(this.config);
        Files.walkFileTree(importBaseDirectory.toPath(), treeWalker);
    }

    @Override
    public boolean hasNext() {
        if (eventQueue == null) {
            List<ImportEvent> eventList = treeWalker.getSortedResources();

            // Remove resources which are in multiple versions but are unchanged
            if (config.includeVersions()) {
                eventQueue = getImportEventQueueWithVersions(eventList);
            } else {
                eventQueue = getImportEventQueue(eventList);
            }
        }
        return eventQueue.size() > 0;
    }

    @Override
    public ImportEvent next() {
        return eventQueue.remove();
    }

    private Queue<ImportEvent> getImportEventQueue(List<ImportEvent> eventList) {
        final Queue<ImportEvent> events = new ArrayDeque<>();
        events.addAll(eventList);

        return events;
    }

    private Queue<ImportEvent> getImportEventQueueWithVersions(final List<ImportEvent> eventList) {
        final Queue<ImportEvent> events = new ArrayDeque<>();

        long previousLastModified = -1;
        for (int i = 0; i < eventList.size(); i++) {
            final ImportEvent event = eventList.get(i);
            
            if (event instanceof ImportVersion) {
                logger.debug("Registering creation of version {} for resource {} to queue",
                        ((ImportVersion) event).getLabel(), event.getMappedUri());
                events.add(event);
                continue;
            }

            final ImportResource resc = (ImportResource) event;
            if (resc.getTimestamp() == previousLastModified && resc.isVersion()
                    && isUnmodified(resc, i, eventList)) {
                // Repeat timestamp and is a versioned resource, and is unmodified, so skip importing this iteration
            } else {
                logger.debug("Adding resource for import to queue: {}", resc.getUri());
                events.add(event);
            }
            
            previousLastModified = event.getTimestamp();
        }

        return events;
    }

    private boolean isUnmodified(final ImportResource resc, final int index,
            final List<ImportEvent> eventList) {
        for (int i = index - 1; i >= 0; i--) {
            final ImportEvent olderResc = eventList.get(i);

            if (olderResc.getTimestamp() != resc.getTimestamp()) {
                return false;
            }

            // Consider the object to be unchanged if last modified matches and same destination uri
            if (olderResc.getMappedUri().equals(resc.getMappedUri())) {
                return true;
            }
        }

        return false;
    }

    private class ChronologicalUriExtractingFileVisitor extends SimpleFileVisitor<Path> {

        private List<ImportEvent> resources;

        private final Config config;

        public ChronologicalUriExtractingFileVisitor(final Config config) {
            resources = new ArrayList<>();
            this.config = config;
        }

        public List<ImportEvent> getSortedResources() {
            resources.sort(new Comparator<ImportEvent>() {

                @Override
                public int compare(ImportEvent o1, ImportEvent o2) {
                    int compareModified = new Long(o1.getTimestamp()).compareTo(o2.getTimestamp());
                    // When time equal, tiebreak by filename
                    if (compareModified == 0) {
                        return o1.getUri().compareTo(o2.getUri());
                    }
                    return compareModified;
                }
            });
            return resources;
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
            final Statement stmt = resc.getProperty(FcrepoConstants.LAST_MODIFIED_DATE);
            final boolean isVersion = resc.hasProperty(RDF_TYPE, FcrepoConstants.VERSION_RESOURCE);
//            final List<String> digests = getMessageDigests(resc);

            final long lastModified = stmt == null ? 0L : getTimestampFromProperty(stmt);

            ImportResource impResc = rescFactory.createFromUri(resourceUri, rdfFile, lastModified);
            impResc.setIsVersion(isVersion);

            resources.add(impResc);

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

            ResIterator vRescIt = model.listResourcesWithProperty(FcrepoConstants.CREATED_DATE);
            while (vRescIt.hasNext()) {
                final Resource vResc = vRescIt.next();
                final long time = getTimestampFromProperty(vResc.getProperty(FcrepoConstants.CREATED_DATE));

                final ImportVersion impVersion = rescFactory.createImportVersion(URI.create(vResc.getURI()), time);
                resources.add(impVersion);
            }
        }

        private long getTimestampFromProperty(final Statement stmt) {
            final XSDDateTime dateTime = (XSDDateTime) stmt.getLiteral().getValue();
            return dateTime.asCalendar().getTimeInMillis();
        }
    }
}
