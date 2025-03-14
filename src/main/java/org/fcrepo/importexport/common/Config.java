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

import org.apache.jena.riot.Lang;
import org.duraspace.bagit.profile.BagProfile;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import static org.apache.jena.riot.RDFLanguages.contentTypeToLang;
import static org.fcrepo.importexport.common.FcrepoConstants.CONTAINS;
import static org.fcrepo.importexport.common.TransferProcess.IMPORT_EXPORT_LOG_PREFIX;
import static org.slf4j.LoggerFactory.getLogger;
import static org.slf4j.helpers.NOPLogger.NOP_LOGGER;


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
    public static final String DEFAULT_STREAMING_RDF_LANG = "application/n-triples";
    public static final String[] DEFAULT_PREDICATES = new String[] { CONTAINS.toString() };

    private String mode;
    private URI resource;
    private URI source;
    private URI destination;
    private File baseDirectory;
    private File writeConfig;

    private URI repositoryRoot;

    private boolean includeAcls = false;
    private boolean includeBinaries = false;
    private boolean retrieveExternal = false;
    private boolean retrieveInbound = false;
    private boolean includeMembership = false;
    private boolean overwriteTombstones = false;
    private boolean legacy = false;
    private boolean includeVersions = false;
    private String bagProfile = null;
    private String bagConfigPath = null;
    private String bagSerialization = null;
    private String[] bagAlgorithms = null;

    private String[] predicates = DEFAULT_PREDICATES;
    private String rdfExtension = DEFAULT_RDF_EXT;
    private String rdfLanguage = DEFAULT_RDF_LANG;
    private String username;
    private String password;

    private Integer threadCount;
    private Path resourceFile;

    private boolean auditLog = false;

    private boolean streaming = false;
    /**
     * Flag indicating whether the RDF language has been set by the user.
     */
    private boolean rdf_set = false;

    private boolean skipTombstoneErrors = false;

    /**
     * This method returns true if the configuration is set for 'import'
     *
     * @return true if import config
     */
    public boolean isImport() {
        return mode != null && mode.equalsIgnoreCase("import");
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
        this.baseDirectory = directory == null ? null : Paths.get(directory).normalize().toFile();
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
     * Sets the location to write a sample config file.
     * @param configFile The filename where the sample config should be written
    **/
    public void setWriteConfig(final String configFile) {
        this.writeConfig = new File(configFile);
    }

    /**
     * Gets the location to write a sample config file.
     * @return The filename to write the config file to, or null if no config file should be written
    **/
    public File getWriteConfig() {
        return writeConfig;
    }

    /**
     * Sets flag indicating whether or not external content should be retrieved when exporting.
     *
     * @param retrieveExternal in import/export
     */
    public void setRetrieveExternal(final boolean retrieveExternal) {
        this.retrieveExternal = retrieveExternal;
    }

    /**
     * Returns true if external content should be retrieved.
     *
     * @return retrieve external content flag
     */
    public boolean retrieveExternal() {
        return retrieveExternal;
    }

    /**
     * Sets flag indicating whether or not inbound references should be retrieved when exporting.
     *
     * @param retrieveInbound Whether to retrieve inbound references
     */
    public void setRetrieveInbound(final boolean retrieveInbound) {
        this.retrieveInbound = retrieveInbound;
    }

    /**
     * Get the inbound references flag.
     *
     * @return true if inbound references should be retrieved
     */
    public boolean retrieveInbound() {
        return retrieveInbound;
    }

    /**
     * Sets flag indicating whether or not membership should be included when exporting direct and indirect containers.
     *
     * @param includeMembership Whether to include membership
     */
    public void setIncludeMembership(final boolean includeMembership) {
        this.includeMembership = includeMembership;
    }

    /**
     * Get the include membership flag.
     *
     * @return true if membership should be included
     */
    public boolean includeMembership() {
        return includeMembership;
    }

    /**
     * Sets flag indicating whether or not tombstones should be overwritten when importing.
     *
     * @param overwriteTombstones on import
     */
    public void setOverwriteTombstones(final boolean overwriteTombstones) {
        this.overwriteTombstones = overwriteTombstones;
    }

    /**
     * Returns true if tombstones should be deleted when importing.
     *
     * @return retrieve overwite tombstones flag
     */
    public boolean overwriteTombstones() {
        return overwriteTombstones;
    }

    /**
     * Returns true if versions should be exported.
     * 
     * @return retrieve include versions flag
     */
    public boolean includeVersions() {
        return includeVersions;
    }

    /**
     * Sets flag indicating whether versions should be exported.
     * 
     * @param includeVersions in export
     */
    public void setIncludeVersions(final boolean includeVersions) {
        this.includeVersions = includeVersions;
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
     * Sets the URI map, for mapping URIs being imported
     *
     * @param map Array containing two URIs for the export baseURL and the import baseURL
     */
    public void setMap(final String[] map) {
        if (map.length == 2 && map[0] != null && map[1] != null) {
            final String sourceStr = map[0];
            final String destinationStr = map[1];
            if ((sourceStr.endsWith("/") && !destinationStr.endsWith("/")) ||
                    (!sourceStr.endsWith("/") && destinationStr.endsWith("/"))) {
                logger.warn("Possible mismatch between the source and destination URIs: one ended with a trailing "
                        + "slash but the other did not: \"{}\" -> \"{}\" (dropping the trailing slash)", sourceStr,
                        destinationStr);
                this.source = URI.create(removeTrailingSlash(sourceStr));
                this.destination = URI.create(removeTrailingSlash(destinationStr));
            } else {
                this.source = URI.create(sourceStr);
                this.destination = URI.create(destinationStr);
            }

        } else {
            throw new IllegalArgumentException("The map should contain the export and import baseURLs");
        }
    }

    private String removeTrailingSlash(final String url) {
        if (url.endsWith("/")) {
            return url.substring(0, url.lastIndexOf('/'));
        } else {
            return url;
        }
    }

    /**
     * Gets the source (export) baseURL, for mapping URIs being imported
     *
     * @return Export baseURL
     */
    public URI getSource() {
        return source;
    }

    /**
     * Gets the source (export) base path, for mapping URIs being imported
     *
     * @return Export base path
     */
    public String getSourcePath() {
        return (source == null) ? null : source.getPath();
    }

    /**
     * Gets the destination (import) baseURL, for mapping URIs being imported
     *
     * @return Import baseURL
     */
    public URI getDestination() {
        return destination;
    }

    /**
     * Gets the destination (import) base path, for mapping URIs being imported
     *
     * @return Import base path
     */
    public String getDestinationPath() {
        return (destination == null) ? null : destination.getPath();
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
     * Try to initialize a BagProfile for either the Importer or Exporter. If the bag profile passed in is "default",
     * use the fedora-import-export identifier.
     *
     * @return the {@link BagProfile}
     * @throws IOException if the {@link BagProfile} cannot be initialized
     */
    public BagProfile initBagProfile() throws IOException {
        final BagProfile.BuiltIn builtIn = "default".equalsIgnoreCase(bagProfile)
                                           ? BagProfile.BuiltIn.FEDORA_IMPORT_EXPORT
                                           : BagProfile.BuiltIn.from(bagProfile);
        return new BagProfile(builtIn);
    }

    /**
     * Set the BagIt serialization format
     * @param bagSerialization the BagIt serialization format, or null if not using serialization
     */
    public void setBagSerialization(final String bagSerialization) {
        this.bagSerialization = bagSerialization;
    }

    /**
     * Get the BagIt serialization format
     * @return the BagIt serialization format, or null if not serializing
     */
    public String getBagSerialization() {
        return bagSerialization;
    }

    /**
     * Set the BagIt config yaml file path
     * @param bagConfigPath The path to the BagIt config yaml file, or null for not using BagIt
     */
    public void setBagConfigPath(final String bagConfigPath) {
        this.bagConfigPath = bagConfigPath;
    }

    /**
     * Get the BagIt config yaml file path
     * @return BagIt config yaml file path or null if not using BagIt
     */
    public String getBagConfigPath() {
        return bagConfigPath;
    }

    /**
     * Set the BagIt profile
     * @param bagProfile The name of the BagIt profile, or null for not using BagIt
     */
    public void setBagProfile(final String bagProfile) {
        this.bagProfile = bagProfile;
    }

    /**
     * Set the BagIt algorithms to use during export
     * @param bagAlgorithms An array of algorithms
     */
    public void setBagAlgorithms(final String[] bagAlgorithms) {
        this.bagAlgorithms = bagAlgorithms;
    }

    /**
     * Gets the user specified algorithms to use when bagging
     *
     * @return the algorithms, or null if not specified
     */
    public String[] getBagAlgorithms() {
        return bagAlgorithms;
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
        this.rdf_set = true;
        this.rdfLanguage = language;
        this.rdfExtension = getRDFExtension(language);
    }

    /**
     * Gets the RDF language
     *
     * @return rdfLanguage if set or default RDF language depending on streaming
     */
    public String getRdfLanguage() {
        return (this.rdf_set ? rdfLanguage : (this.isStreaming() ? DEFAULT_STREAMING_RDF_LANG : DEFAULT_RDF_LANG));
    }

    /**
     * @return boolean Whether the RDF language has been set by the user.
     */
    public boolean isRdfSet() {
        return rdf_set;
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

    /**
     * Turn on/off "legacy" mode, a mode in which certain server-managed triples are
     * intentionally omitted from import because updating them wasn't supported by
     * Fedora.
     * @param legacy true to indicate legacy mode should be enabled, false to disable it
     */
    public void setLegacy(final boolean legacy) {
        this.legacy = legacy;
    }

    /**
     * Check whether "legacy" mode is enabled.
     * @return true if legacy mode is enabled, false, otherwise
     */
    public boolean isLegacy() {
        return this.legacy;
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
        if (this.getSource() != null && this.getDestination() != null) {
            map.put("map", this.getSource() + "," + this.getDestination());
        }
        map.put("dir", this.baseDirectory.getAbsolutePath());
        if (!this.getRdfLanguage().isEmpty()) {
            map.put("rdfLang", this.getRdfLanguage());
        }
        map.put("acls", Boolean.toString(this.includeAcls));
        map.put("binaries", Boolean.toString(this.includeBinaries));
        map.put("external", Boolean.toString(this.retrieveExternal));
        map.put("inbound", Boolean.toString(this.retrieveInbound));
        map.put("membership", Boolean.toString(this.includeMembership));
        map.put("overwriteTombstones", Boolean.toString(this.overwriteTombstones()));
        map.put("legacyMode", Boolean.toString(this.isLegacy()));
        map.put("versions", Boolean.toString(this.includeVersions));
        if (this.getBagProfile() != null) {
            map.put("bag-profile", this.getBagProfile());
        }
        if (this.getBagConfigPath() != null) {
            map.put("bag-config", this.getBagConfigPath());
        }
        if (this.getBagSerialization() != null) {
            map.put("bag-serialization", this.getBagSerialization());
        }
        if (this.getBagAlgorithms() != null) {
            map.put("bag-algorithms", String.join(",", this.getBagAlgorithms()));
        }
        final String predicates = String.join(",", this.getPredicates());
        map.put("predicates", predicates);
        map.put("auditLog", Boolean.toString(this.auditLog));
        if (threadCount != null) {
            map.put("threadCount", threadCount.toString());
        }
        if (resourceFile != null) {
            map.put("resourceFile", resourceFile.toAbsolutePath().toString());
        }
        map.put("streaming", Boolean.toString(this.streaming));
        map.put("isRdfSet", Boolean.toString(this.isRdfSet()));
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

    /**
     * Get the URI of the repository root.
     *
     * @return URI of repository root
     */
    public URI getRepositoryRoot() {
        return repositoryRoot;
    }

    /**
     * Set the repository root to the given URI.
     *
     * @param repositoryRoot URI
     */
    public void setRepositoryRoot(final URI repositoryRoot) {
        this.repositoryRoot = repositoryRoot;
    }

    /**
     * Set the repository root to the URI given as a string.
     *
     * @param repositoryRoot string representation of URI
     */
    public void setRepositoryRoot(final String repositoryRoot) {
        this.repositoryRoot = URI.create(repositoryRoot);
    }

    /**
     * Returns true if acls should be exported/imported
     * @return include acls flag
     */
    public boolean isIncludeAcls() {
        return includeAcls;
    }

    /**
     * Set the flag to include acls
     * @param includeAcls in export/import
     */
    public void setIncludeAcls(final boolean includeAcls) {
        this.includeAcls = includeAcls;
    }

    /**
     * @return the number of threads to use, may be null
     */
    public Integer getThreadCount() {
        return threadCount;
    }

    /**
     * Set the number of threads to use. If null, then the number of threads will be defaulted
     * based on the number of available processors.
     *
     * @param threadCount the number of threads to use, or null
     */
    public void setThreadCount(final Integer threadCount) {
        if (threadCount == null || threadCount < 1) {
            this.threadCount = null;
        } else {
            this.threadCount = threadCount;
        }
    }

    /**
     * @return the path to the file that contains resource uris to process
     */
    public Path getResourceFile() {
        return resourceFile;
    }

    /**
     * @param resourceFile the path to the file that contains resource uris to process
     */
    public void setResourceFile(final Path resourceFile) {
        this.resourceFile = resourceFile;
    }

    /**
     * @return true if mode is export and streaming is enabled
     */
    public boolean isStreaming() {
        return this.streaming;
    }

    /**
     * @param streaming true if streaming is enabled for export
     */
    public void setStreaming(final boolean streaming) {
        this.streaming = streaming;
    }

    /**
     * @return true if tombstone errors should be skipped
     */
    public boolean isSkipTombstoneErrors() {
        return skipTombstoneErrors;
    }

    /**
     * @param skipTombstoneErrors true if tombstone errors should be skipped
     */
    public void setSkipTombstoneErrors(final boolean skipTombstoneErrors) {
        this.skipTombstoneErrors = skipTombstoneErrors;
    }
}
