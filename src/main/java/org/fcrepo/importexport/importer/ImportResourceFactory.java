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

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.fcrepo.importexport.common.Config;
import org.fcrepo.importexport.importer.VersionImporter.ImportResource;
import org.fcrepo.importexport.importer.VersionImporter.ImportVersion;

/**
 * Factory which produces {@link ImportResource} objects
 * 
 * @author bbpennel
 *
 */
public class ImportResourceFactory {

    private final Config config;

    /**
     * Default constructor
     * 
     * @param config
     * @param uriTranslator
     */
    public ImportResourceFactory(final Config config) {
        this.config = config;
    }

    public ImportResource createFromUri(final URI uri, final File descriptionFile, final long lastModified) {
        final String uriString = uri.toString();
        final String id = uriString.substring(uriString.lastIndexOf('/') + 1);
        
        return new ImportResource(id, uri, descriptionFile, lastModified, config);
    }
    
    public ImportVersion createImportVersion(final URI uri, final long timestamp) {
        final String uriString = uri.toString();
        final String id = uriString.substring(uriString.lastIndexOf('/') + 1);
        
        return new ImportVersion(id, uri, timestamp, config);
    }
    
    /**
     * Returns a list of ImportResource objects constructed from the files present in the given directory.
     * Files/folders are grouped together into a resource based on sharing an id, which is the unsuffixed portion of
     * the filename.
     * 
     * @param directory directory whose children will be aggregated to created ImportResource objects.
     * @return
     */
    public List<ImportResource> createFromDirectory(final File directory) {
//        final Pattern resourceIdPattern = Pattern.compile("(.+?)(\\" + EXTERNAL_RESOURCE_EXTENSION +
//                "|\\" + BINARY_EXTENSION + "|\\" + config.getRdfExtension() + ")?");
//
//        // Associate all files/directories that comprise a resource together
//        final Map<String, ImportResource> resourceMap = new HashMap<>();
//        stream(directory.listFiles()).forEach(f -> {
//            final Matcher matcher = resourceIdPattern.matcher(f.getName());
//            if (matcher.matches()) {
//                final String id = matcher.group(1);
//                ImportResource resc = resourceMap.get(id);
//                if (resc == null) {
//                    // build the uri for this resource with the de-extensioned filename
//                    final URI uri = URITranslationUtil.uriForFile(new File(f.getParentFile(), id), config);
//
//                    resc = new ImportResource(id, uri, config);
//                    resourceMap.put(id, resc);
//                }
//                resc.addFile(f);
//            }
//        });

        //return new ArrayList<>(resourceMap.values());
        return null;
    }

    /**
     * Create a list of ImportResources containing all versions present for the given ImportResource, including the
     * resource itself
     * 
     * @param resource
     * @return
     */
    public List<ImportResource> createVersionResourceList(final ImportResource resource) {
        if (!config.includeVersions() || !resource.hasVersions()) {
            return Arrays.asList(resource);
        }

        final List<ImportResource> unorderedVersions = createFromDirectory(resource.getVersionsDirectory());

        final List<String> orderedLabels = resource.getVersionLabels();

        // Order the previous versions by the order of the labels
        final List<ImportResource> versions = orderedLabels.stream()
                .map(label -> unorderedVersions.stream()
                        .filter(version -> version.getId().equals(label))
                        .findFirst().get())
                .peek(v -> v.setIsVersion(true))
                .collect(Collectors.toCollection(ArrayList::new));
        // Add the base resource as the last version
        versions.add(resource);

        return versions;
    }
}
