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
package org.fcrepo.importexport.common;

import java.io.File;
import java.net.URI;

/**
 * @author awoods
 * @author escowles
 * @since 2016-08-29
 */
public class Config {

    private String mode;
    private URI resource;
    private URI source;
    private File baseDirectory;
    private boolean includeBinaries;
    private String[] predicates;
    private String rdfExtension;
    private String rdfLanguage;
    private String username;
    private String password;

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
     * Sets the directory for the import/export
     *
     * @param directory to export to or import from
     */
    public void setBaseDirectory(final String directory) {
        this.baseDirectory = directory == null ? null : new File(directory);
    }

    /**
     * Gets the base directory for the import/export
     *
     * @return binaryDirectory
     */
    public File getBaseDirectory() {
        return baseDirectory;
    }

    /**
     * Sets flag indicating whether or not binaries should be imported/exported.
     *
     * @param includeBinaries in import/export
     */
    public void setIncludeBinaries(final boolean includeBinaries) {
        this.includeBinaries = includeBinaries;
    }

    /**
     * Returns true if binaries should be imported/exported.
     *
     * @return include binaries flag
     */
    public boolean isIncludeBinaries() {
        return includeBinaries;
    }

    /**
     * Sets the URI of the resource to import/export
     *
     * @param resource URI to import/export
     */
    public void setResource(final String resource) {
        setResource(URI.create(resource));
    }

    /**
     * Sets the URI of the resource to import/export
     *
     * @param resource URI to import/export
     */
    public void setResource(final URI resource) {
        if (resource.toString().endsWith("/")) {
            this.resource = URI.create(resource.toString().substring(0, resource.toString().length() - 1));
        } else {
            this.resource = resource;
        }
    }

    /**
     * Gets the URI of the resource to import/export
     *
     * @return resource
     */
    public URI getResource() {
        return resource;
    }

    /**
     * Sets the URI of the source resource, for mapping URIs being imported
     *
     * @param source URI to import/export
     */
    public void setSource(final String source) {
        if (source != null) {
            setSource(URI.create(source));
        }
    }

    /**
     * Sets the URI of the source resoruce, for mapping URIs being imported
     *
     * @param source URI to import/export
     */
    public void setSource(final URI source) {
        if (source.toString().endsWith("/")) {
            this.source = URI.create(source.toString().substring(0, source.toString().length() - 1));
        } else {
            this.source = source;
        }
    }

    /**
     * Gets the URI of the source resoruce, for mapping URIs being imported
     *
     * @return source source
     */
    public URI getSource() {
        // If 'source' exists, use it... else return 'resource'
        if (source != null) {
            return source;
        } else {
            return resource;
        }
    }

    /**
     * Get the predicates that define resource containment
     * @return An array of predicates
     */
    public String[] getPredicates() {
        return predicates;
    }

    /**
     * Set the predicates that define resource containment
     * @param predicates An array of predicates
     */
    public void setPredicates(final String[] predicates) {
        this.predicates = predicates;
    }

    /**
     * Sets the RDF filename extension
     *
     * @param extension of the RDF filename
     */
    public void setRdfExtension(final String extension) {
        this.rdfExtension = extension;
    }

    /**
     * Gets the RDF filename extension
     *
     * @return rdfExtension
     */
    public String getRdfExtension() {
        return rdfExtension;
    }

    /**
     * Sets the RDF language
     *
     * @param language of the exported RDF
     */
    public void setRdfLanguage(final String language) {
        this.rdfLanguage = language;
    }

    /**
     * Gets the RDF language
     *
     * @return rdfLanguage
     */
    public String getRdfLanguage() {
        return rdfLanguage;
    }

    /**
     * Sets the username for basic auth.
     *
     * @param username the username
     */
    public void setUsername(final String username) {
        this.username = username;
    }

    /**
     * Gets the username for basic auth.
     *
     * @return the username
     */
    public String getUsername() {
        return this.username;
    }

    /**
     * Sets the password for basic auth.
     *
     * @param password the password
     */
    public void setPassword(final String password) {
        this.password = password;
    }

    /**
     * Gets the password for basic auth.
     *
     * @return the password
     */
    public String getPassword() {
        return this.password;
    }
}
