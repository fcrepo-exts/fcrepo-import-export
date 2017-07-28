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

import static org.fcrepo.importexport.common.FcrepoConstants.BINARY_EXTENSION;
import static org.fcrepo.importexport.common.FcrepoConstants.EXTERNAL_RESOURCE_EXTENSION;
import static org.fcrepo.importexport.common.FcrepoConstants.FCR_METADATA_PATH;
import static org.fcrepo.importexport.common.FcrepoConstants.RDF_TYPE;
import static org.fcrepo.importexport.common.ModelUtils.mapRdfStreamToNonversionedSubjects;
import static org.fcrepo.importexport.common.URITranslationUtil.addRelativePath;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
import org.fcrepo.importexport.common.Config;
import org.fcrepo.importexport.common.TransferProcess;


/**
 * An event for importing a resource
 *
 * @author bbpennel
 *
 */
public class ImportResource extends ImportEvent {

    private URI descriptionUri;
    private Model model;
    private Resource resource;
    private File binary;
    private File descriptionFile;

    /**
     * Construct new ImportResource
     *
     * @param uri uri of resource
     * @param descriptionFile description file for resource
     * @param created created timestamp
     * @param lastModified last modified timestamp for resource
     * @param config config
     */
    public ImportResource(final URI uri, final File descriptionFile, final long created,
            final long lastModified, final Config config) {
        super(uri, created, lastModified, config);
        this.descriptionFile = descriptionFile;
    }

    /**
     * Get the URI for metadata for this resource
     *
     * @return uri of the RDF endpoint for resource
     */
    public URI getDescriptionUri() {
        if (descriptionUri == null) {
            if (isBinary()) {
                descriptionUri = addRelativePath(getMappedUri(), FCR_METADATA_PATH);
            } else {
                descriptionUri = getMappedUri();
            }
        }
        return descriptionUri;
    }

    /**
     * Test if this resource is a binary
     *
     * @return true if resource is a binary
     */
    public boolean isBinary() {
        return getBinary().exists();
    }

    /**
     * Get the binary file for this resource
     *
     * @return the binary for this resource or null if not found
     */
    public File getBinary() {
        if (binary == null) {
            binary = TransferProcess.fileForURI(uri, config.getSourcePath(),
                    config.getDestinationPath(), config.getBaseDirectory(), BINARY_EXTENSION);
            if (!binary.exists()) {
                binary = TransferProcess.fileForURI(uri, config.getSourcePath(),
                        config.getDestinationPath(), config.getBaseDirectory(), EXTERNAL_RESOURCE_EXTENSION);
            }
        }
        return binary;
    }

    /**
     * Get the file containing metadata for this resource
     *
     * @return returns the file containing this resource's description
     */
    public File getDescriptionFile() {
        return descriptionFile;
    }

    /**
     * Get the model containing properties assigned to this resource
     *
     * @return model containing properties assigned to this resource
     */
    public Model getModel() {
        if (model == null) {
            final File mdFile = getDescriptionFile();
            if (mdFile == null) {
                return null;
            }
            try {
                model = mapRdfStreamToNonversionedSubjects(new FileInputStream(mdFile), config);
            } catch (IOException e) {
                throw new RuntimeException("Failed to read model for " + uri, e);
            }
        }
        return model;
    }

    /**
     * Get the resource representing this ImportResource from its model
     *
     * @return rdf resource for this resource
     */
    public Resource getResource() {
        if (resource == null) {
            final Model model = getModel();
            if (model == null) {
                return null;
            }
            resource = model.listResourcesWithProperty(RDF_TYPE).next();
        }
        return resource;
    }
}
