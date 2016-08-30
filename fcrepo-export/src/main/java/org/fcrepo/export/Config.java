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
package org.fcrepo.export;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author awoods
 * @author escowles
 * @since 2016-08-29
 */
public class Config {

    private String mode;
    private URI resource;
    private File binaryDirectory;
    private File descriptionDirectory;
    private String rdfExtension;
    private String rdfLanguage;

    /**
     * This method returns true if the configuration is set for 'import'
     *
     * @return true if import config
     */
    public boolean isImport() {
        return mode.equalsIgnoreCase("import");
    }

    /**
     * This method returns true if the configuration is set for 'export'
     *
     * @return true if export config
     */
    public boolean isExport() {
        return !isImport();
    }

    /**
     * This method sets the import/export mode
     *
     * @param mode of this configuration
     */
    public void setMode(final String mode) {
        this.mode = mode;
    }

    /**
     * Sets the base directory for binaries.
     */
    public void setBinaryDirectory(final String directory) {
        this.binaryDirectory = directory == null ? null : new File(directory);
    }

    /**
     * Sets the base directory for binaries.
     */
    public void setBinaryDirectory(final File directory) {
        this.binaryDirectory = directory;
    }

    /**
     * Gets the base directory for binaries.
     */
    public File getBinaryDirectory() {
        return binaryDirectory;
    }

    /**
     * Sets the base directory for descriptions.
     */
    public void setDescriptionDirectory(final String directory) {
        this.descriptionDirectory = new File(directory);
    }

    /**
     * Sets the base directory for descriptions.
     */
    public void setDescriptionDirectory(final File directory) {
        this.descriptionDirectory = directory;
    }

    /**
     * Gets the base directory for descriptions.
     */
    public File getDescriptionDirectory() {
        return descriptionDirectory;
    }

    /**
     * Sets the URI of the resource to import/export
     */
    public void setResource(final String resource) throws URISyntaxException {
        this.resource = new URI(resource);
    }

    /**
     * Sets the URI of the resource to import/export
     */
    public void setResource(final URI resource) {
        this.resource = resource;
    }

    /**
     * Gets the URI of the resource to import/export
     */
    public URI getResource() {
        return resource;
    }

    /**
     * Sets the RDF filename extension
     */
    public void setRdfExtension(final String extension) {
        this.rdfExtension = extension;
    }

    /**
     * Gets the RDF filename extension
     */
    public String getRdfExtension() {
        return rdfExtension;
    }

    /**
     * Sets the RDF language
     */
    public void setRdfLanguage(final String language) {
        this.rdfLanguage = language;
    }

    /**
     * Gets the RDF language
     */
    public String getRdfLanguage() {
        return rdfLanguage;
    }
}
