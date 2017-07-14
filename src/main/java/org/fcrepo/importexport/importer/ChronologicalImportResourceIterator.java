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
import static org.apache.jena.riot.RDFLanguages.contentTypeToLang;
import static org.fcrepo.importexport.common.FcrepoConstants.NON_RDF_SOURCE;
import static org.fcrepo.importexport.common.FcrepoConstants.RDF_TYPE;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.jena.datatypes.xsd.XSDDateTime;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ResIterator;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.riot.RDFDataMgr;
import org.fcrepo.importexport.common.Config;
import org.fcrepo.importexport.common.FcrepoConstants;
import org.fcrepo.importexport.common.URITranslationUtil;
import org.fcrepo.importexport.importer.VersionImporter.ImportResource;

/**
 * Iterates through resources in chronological order based on last modified timestamp 
 * 
 * @author bbpennel
 *
 */
public class ChronologicalImportResourceIterator implements Iterator<ImportResource> {

    private final Config config;
    private final ChronologicalUriExtractingFileVisitor treeWalker;
    private Iterator<ChronologicalResource> rescIt;

    private ImportResourceFactory rescFactory;

    public ChronologicalImportResourceIterator(final Config config, final ImportResourceFactory rescFactory)
            throws IOException {
        this.config = config;
        this.rescFactory = rescFactory;

        this.treeWalker = new ChronologicalUriExtractingFileVisitor(this.config);
        Files.walkFileTree(config.getBaseDirectory().toPath(), treeWalker);
    }

    @Override
    public boolean hasNext() {
        if (rescIt == null) {
            rescIt = treeWalker.getSortedResources().iterator();
        }
        return rescIt.hasNext();
    }

    @Override
    public ImportResource next() {
        final ChronologicalResource nextResc = rescIt.next();

        return rescFactory.createFromUri(nextResc.uri, nextResc.metadataFile);
    }

    private static Model parseStream(final InputStream in, final Config config) throws IOException {
        final SubjectMappingStreamRDF mapper = new SubjectMappingStreamRDF(config.getSource(),
                                                                           config.getDestination());
        try (final InputStream in2 = in) {
            RDFDataMgr.parse(mapper, in2, contentTypeToLang(config.getRdfLanguage()));
        }
        return mapper.getModel();
    }

    private class ChronologicalResource {
        public long modified;
        public URI uri;
        public File metadataFile;
        
        public ChronologicalResource(long modified, URI uri, File metadataFile) {
            super();
            this.modified = modified;
            this.uri = uri;
            this.metadataFile = metadataFile;
        }
    }
    
    private class ChronologicalUriExtractingFileVisitor extends SimpleFileVisitor<Path> {
        private List<ChronologicalResource> resources;
        private final Config config;

        public ChronologicalUriExtractingFileVisitor(final Config config) {
            resources = new ArrayList<>();
            this.config = config;
        }

        public List<ChronologicalResource> getSortedResources() {
            resources.sort(new Comparator<ChronologicalResource>() {
                @Override
                public int compare(ChronologicalResource o1, ChronologicalResource o2) {
                    return new Long(o1.modified).compareTo(o2.modified);
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

            // Skip over the details of versions as they are not treated as resources
            if (file.toString().endsWith("fcr%3Aversions" + config.getRdfExtension())) {
                return CONTINUE;
            }
            
            // If versioning is excluded then skip all past versions of resources
            if (!config.includeVersions() && file.toString().contains("fcr%3Aversions")) {
                return CONTINUE;
            }

            final File rdfFile = file.toFile();
            final Model model = parseStream(new FileInputStream(rdfFile), config);

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
            if (stmt == null) {
                // No modified date, store with 0 for sorting
                resources.add(new ChronologicalResource(0L, resourceUri, rdfFile));
            } else {
                final XSDDateTime created = (XSDDateTime) stmt.getLiteral().getValue();
                final long createdMillis = created.asCalendar().getTimeInMillis();
                resources.add(new ChronologicalResource(createdMillis, resourceUri, rdfFile));
            }

            return CONTINUE;
        }
    }
}
