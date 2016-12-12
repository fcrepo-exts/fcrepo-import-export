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

import static org.apache.jena.riot.RDFLanguages.contentTypeToLang;
import static org.fcrepo.importexport.common.FcrepoConstants.CONTAINS;
import static org.fcrepo.importexport.common.TransferProcess.IMPORT_EXPORT_LOG_PREFIX;
import static org.slf4j.LoggerFactory.getLogger;
import static org.slf4j.helpers.NOPLogger.NOP_LOGGER;

import java.io.File;
import java.net.URI;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;

import org.apache.jena.riot.Lang;
import org.slf4j.Logger;


/**
 * @author awoods
 * @author escowles
 * @author whikloj
 * @since 2016-08-29
 */
public class Config {

    private final static Logger logger = getLogger(Config.class);

    public static final String DEFAULT_RDF_LANG = "text/turtle";
    public static final String DEFAULT_RDF_EXT = getRDFExtension(DEFAULT_RDF_LANG);
    public static final String[] DEFAULT_PREDICATES = new String[] { CONTAINS.toString() };

    private String mode;
    private URI resource;
    private URI source;
    private File baseDirectory;

    private boolean includeBinaries = false;
    private String bagProfile = null;

    private String[] predicates = DEFAULT_PREDICATES;
    private String rdfExtension = DEFAULT_RDF_EXT;
    private String rdfLanguage = DEFAULT_RDF_LANG;
    private String username;
    private String password;

    private boolean auditLog = false;

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
        return (bagProfile == null) ? baseDirectory : new File(baseDirectory, "data");
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
     * Get the BagIt profile
     * @return BagIt profile name, or null for not using BagIt
     */
    public String getBagProfile() {
        return bagProfile;
    }

    /**
     * Set the BagIt profile
     * @param bagProfile The name of the BagIt profile, or null for not using BagIt
     */
    public void setBagProfile(final String bagProfile) {
        this.bagProfile = bagProfile;
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
        this.rdfExtension = getRDFExtension(language);
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

    /**

     * Turn on/off audit logging
     *
     * @param auditLevel the state of audit logging.
     */
    public void setAuditLog(final boolean auditLevel) {
        this.auditLog = auditLevel;
    }

    /**
     * Check whether the audit log is enabled.
     *
     * @return whether audit logging is enabled.
     */
    public Logger getAuditLog() {
        if (this.auditLog) {
            return getLogger(IMPORT_EXPORT_LOG_PREFIX);
        }
        return NOP_LOGGER;
    }

    /** Static constructor using Yaml hashmap
     *
     * @param configVars config vars from Yaml file
     * @return Config object with values from Yaml
     * @throws ParseException If the Yaml does not parse correctly.
     */
    public static Config fromFile(final Map<String, String> configVars) throws ParseException {
        final Config c = new Config();
        int lineNumber = 0;
        for (Map.Entry<String, String> entry : configVars.entrySet()) {
            logger.debug("config map entry is ({}) and value ({})", entry.getKey(), entry.getValue());
            lineNumber += 1;
            if (entry.getKey().equalsIgnoreCase("mode")) {
                if (entry.getValue().equalsIgnoreCase("import") || entry.getValue().equalsIgnoreCase("export")) {
                    c.setMode(entry.getValue());
                } else {
                    throw new ParseException(String.format("Invalid value for \"mode\": {}", entry.getValue()),
                        lineNumber);
                }
            } else if (entry.getKey().equalsIgnoreCase("resource")) {
                c.setResource(entry.getValue());
            } else if (entry.getKey().equalsIgnoreCase("source")) {
                c.setSource(entry.getValue());
            } else if (entry.getKey().equalsIgnoreCase("dir")) {
                c.setBaseDirectory(entry.getValue());
            } else if (entry.getKey().equalsIgnoreCase("rdfLang")) {
                c.setRdfLanguage(entry.getValue());
            } else if (entry.getKey().trim().equalsIgnoreCase("binaries")) {
                if (entry.getValue().equalsIgnoreCase("true") || entry.getValue().equalsIgnoreCase("false")) {
                    c.setIncludeBinaries(Boolean.parseBoolean(entry.getValue()));
                } else {
                    throw new ParseException(String.format(
                        "binaries configuration parameter only accepts \"true\" or \"false\", \"{}\" received",
                        entry.getValue()), lineNumber);
                }
            } else {
                throw new ParseException(String.format("Unknown configuration key: {}", entry.getKey()), lineNumber);
            }
        }
        return c;
    }

    /**
     * Generate a HashMap suitable for serializing to Yaml
     *
     * @return Map key value pairs of configuration
     */
    public Map<String, String> getMap() {
        final Map<String, String> map = new HashMap<String, String>();
        map.put("mode", (this.isImport() ? "import" : "export"));
        map.put("resource", this.getResource().toString());
        if (!this.getSource().toString().isEmpty()) {
            map.put("source", this.getSource().toString());
        }
        map.put("dir", this.getBaseDirectory().getAbsolutePath());
        if (!this.getRdfLanguage().isEmpty()) {
            map.put("rdfLang", this.getRdfLanguage());
        }
        map.put("binaries", Boolean.toString(this.includeBinaries));
        return map;
    }

    /**
     * Get extension for the RDF language provided
     *
     * @param language RDF language mimetype
     * @return string of file extension
     */
    private static String getRDFExtension(final String language) {
        final Lang lang = contentTypeToLang(language);
        if (lang == null) {
            throw new RuntimeException(language + " is not a recognized RDF language");
        }

        return "." + lang.getFileExtensions().get(0);

    }
}
