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
package org.fcrepo.importexport;

import static org.duraspace.bagit.SerializationSupport.GZIP_TYPES;
import static org.duraspace.bagit.SerializationSupport.TAR_TYPES;
import static org.duraspace.bagit.SerializationSupport.ZIP_TYPES;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.duraspace.bagit.BagProfile;
import org.fcrepo.client.FcrepoClient;
import org.fcrepo.importexport.common.Config;
import org.fcrepo.importexport.common.TransferProcess;
import org.fcrepo.importexport.exporter.Exporter;
import org.fcrepo.importexport.importer.Importer;
import org.slf4j.Logger;

import com.esotericsoftware.yamlbeans.YamlReader;
import com.esotericsoftware.yamlbeans.YamlWriter;

/**
 * Command-line arguments parser.
 *
 * @author awoods
 * @author escowles
 * @author whikloj
 * @since 2016-08-29
 */
public class ArgParser {

    /**
     *
     */
    private static final String BAG_CONFIG_OPTION_KEY = "bag-config";

    /**
     *
     */
    private static final String BAG_PROFILE_OPTION_KEY = "bag-profile";

    private static final Logger logger = getLogger(ArgParser.class);

    private static final Options configOptions = new Options();

    private static final Options configFileOptions = new Options();

    static {
        // Help option
        configOptions.addOption(Option.builder("h")
                .longOpt("help")
                .hasArg(false)
                .desc("Print these options")
                .required(false)
                .build());

        // Mode option
        configOptions.addOption(Option.builder("m")
                .longOpt("mode")
                .hasArg(true).numberOfArgs(1).argName("mode")
                .desc("Mode: [import|export]")
                .required(true)
                .build());

        // Resource option
        configOptions.addOption(Option.builder("r")
                .longOpt("resource")
                .hasArg(true).numberOfArgs(1).argName("resource")
                .desc("Resource (URI) to import/export")
                .required(true).build());

        // Source Resource option
        configOptions.addOption(Option.builder("M")
                .longOpt("map").argName("map")
                .hasArgs().numberOfArgs(2)
                .valueSeparator(',')
                .required(false)
                .desc("Old and new base URIs, separated by comma, to map URIs when importing")
                .build());

        // Base Directory option
        configOptions.addOption(Option.builder("d")
                .longOpt("dir")
                .hasArg(true).numberOfArgs(1).argName("dir")
                .desc("The directory to export repo to or import the repo from.")
                .required(true).build());

        // Import/export binaries option
        configOptions.addOption(Option.builder("b")
                .longOpt("binaries")
                .hasArg(false)
                .desc("When present this flag indicates that binaries should be imported/exported.")
                .required(false).build());

        // Retrieve external content
        configOptions.addOption(Option.builder("x")
                .longOpt("external")
                .hasArg(false)
                .desc("When present this flag indicates that external content should be exported.")
                .required(false).build());

        // Retrieve inbound references
        configOptions.addOption(Option.builder("i")
                .longOpt("inbound")
                .hasArg(false)
                .desc("When present this flag indicates that inbound references should be exported.")
                .required(false).build());

        // Write config file
        configOptions.addOption(Option.builder("w")
                .longOpt("writeConfig")
                .hasArg(true).numberOfArgs(1).argName("writeConfig")
                .desc("When present this flag indicates that a sample config should be written at the" +
                       " specified filename.")
                .required(false).build());

        // Overwrite Tombstones
        configOptions.addOption(Option.builder("t")
                 .longOpt("overwriteTombstones")
                 .hasArg(false)
                 .desc("When importing, overwrite \"tombstones\" left behind after resources were deleted.")
                 .required(false).build());

        // Legacy Mode option
        configOptions.addOption(Option.builder("L")
                .longOpt("legacyMode")
                .hasArg(false)
                .desc("When importing, omit certain server-managed-triples that aren't modifiable in old" +
                        " versions of fedora.")
                .required(false).build());

        // Include versions
        configOptions.addOption(Option.builder("V")
                 .longOpt("versions")
                 .hasArg(false)
                 .desc("When exporting, include versions of resources and binaries.")
                 .required(false).build());

        // RDF language option
        configOptions.addOption(Option.builder("l")
                .longOpt("rdfLang")
                .hasArg(true).numberOfArgs(1).argName("rdfLang")
                .desc("RDF language (default: " + Config.DEFAULT_RDF_LANG + ")")
                .required(false).build());

        // containment predicates
        configOptions.addOption(Option.builder("p")
                .longOpt("predicates").argName("predicates")
                .hasArgs()
                .valueSeparator(',')
                .required(false)
                .desc("Comma-separated list of predicates to define resource containment")
                .build());

        // bagit creation
        configOptions.addOption(Option.builder("g")
                .longOpt(BAG_PROFILE_OPTION_KEY).argName("profile")
                .hasArg(true).numberOfArgs(1).argName("profile")
                .required(false)
                .desc("Export and import BagIt bags using profile [default|aptrust|metaarchive|perseids|\n" +
                      "beyondtherepository]")
                .build());

        configOptions.addOption(Option.builder("G")
                .longOpt(BAG_CONFIG_OPTION_KEY).argName("path")
                .hasArg(true).numberOfArgs(1).argName("path")
                .required(false)
                .desc("Path to the bag config file")
                .build());


        // create the description for the serialization option
        // this shows which options are available for each of the built in BagProfiles
        final String zip = "zip";
        final String tar = "tar";
        final String gzip = "gzip";
        final StringBuilder serializationDesc = new StringBuilder("Export BagIt bags into a serialized format. " +
                                                                  "Available formats depend on the bag profile " +
                                                                  "specified.");
        for (BagProfile.BuiltIn builtIn : BagProfile.BuiltIn.values()) {
            try {
                final BagProfile profile = new BagProfile(builtIn);
                // map from the long form types to a short form
                // e.g. application/zip -> zip
                final Set<String> normalizedFormats = new HashSet<>();
                for (String acceptedSerialization : profile.getAcceptedSerializations()) {
                    if (TAR_TYPES.contains(acceptedSerialization)) {
                        normalizedFormats.add(tar);
                    } else if (ZIP_TYPES.contains(acceptedSerialization)) {
                        normalizedFormats.add(zip);
                    } else if (GZIP_TYPES.contains(acceptedSerialization)) {
                        normalizedFormats.add(gzip);
                    }
                }
                serializationDesc.append("\n\t").append(builtIn.getIdentifier()).append(": ").append(normalizedFormats);
            } catch (IOException e) {
                logger.warn("Unable to get BagProfile for built in profile {}", builtIn);
            }
        }
        configOptions.addOption(Option.builder("s")
                               .longOpt("bag-serialization").argName("format")
                               .hasArg(true).numberOfArgs(1).argName("format")
                               .required(false)
                               .desc(serializationDesc.toString())
                               .build());

        configOptions.addOption(Option.builder("R")
                .longOpt("repositoryRoot").argName("uri")
                .hasArg(true).numberOfArgs(1).argName("uri")
                .required(false)
                .desc("When exporting, use this URI as the repository root; " +
                        "if not given, export will attempt to automatically determine the repository root")
                .build());

        // username option
        final Option userOption = Option.builder("u")
                .longOpt("user")
                .hasArg(true).numberOfArgs(1).argName("user")
                .desc("username:password for fedora basic authentication").build();
        configOptions.addOption(userOption);
        configFileOptions.addOption(userOption);

        // Config file option
        configFileOptions.addOption(Option.builder("c")
                .longOpt("config")
                .hasArg(true).numberOfArgs(1).argName("config")
                .desc("Path to config file")
                .required(true).build());

        configOptions.addOption(Option.builder("a")
                .longOpt("auditLog")
                .desc("Enable audit log creation, disabled by default")
                .required(false).build());

    }

    /**
     * Parse command line arguments into a Config object
     *
     * @param args command line arguments
     * @return the parsed config file or command line args.
     */
    protected Config parseConfiguration(final String[] args) {
        // first see if they've specified a config file
        CommandLine c = null;
        Config config = null;
        try {
            c = parseConfigArgs(configFileOptions, args);
            config = parseConfigFileOptions(c);
            addSharedOptions(c, config);
        } catch (final ParseException ignore) {
            logger.debug("Command line argments weren't valid for specifying a config file.");
        }
        if (config == null) {
            // check for presence of the help flag
            if (helpFlagged(args)) {
                printHelp(null);
            }

            try {
                c = parseConfigArgs(configOptions, args);
                config = parseConfigurationArgs(c);
                addSharedOptions(c, config);
            } catch (final ParseException e) {
                printHelp("Error parsing args: " + e.getMessage());
            }
        }

        validateConfig(config);

        // Write command line options to disk
        saveConfig(config);

        return config;
    }

    /**
     * Validation routines for checking interdependent configuration parameters.
     * @param config
     */
    private void validateConfig(final Config config) {
        // verify bagit config
        if (config.getBagProfile() != null &&
                config.getBagConfigPath() == null) {
            throw new RuntimeException("A bagit config path must be set when using a bagit profile.");
        }

        if (config.getBagProfile() == null &&
                config.getBagConfigPath() != null) {
            throw new RuntimeException("A bagit profile must be set when you set a bagit config.");
        }

    }

    /**
     * Check for a help flag
     *
     * @param args command line arguments
     * @return whether the help flag was found.
     */
    private boolean helpFlagged(final String[] args) {
        return Stream.of(args).anyMatch(x -> x.equals("-h") || x.equals("--help"));
    }

    /**
     * Parse command line options based on the provide Options
     *
     * @param configOptions valid set of Options
     * @param args command line arguments
     * @return the list of option and values
     * @throws ParseException if invalid/missing option is found
     */
    private static CommandLine parseConfigArgs(final Options configOptions, final String[] args) throws ParseException {
        return new DefaultParser().parse(configOptions, args);
    }

    /**
     * This method tries to parse the configuration file, if that option was provided
     *
     * @param cmd from command line
     * @return Config or null if no config file option was provided
     */
    private Config parseConfigFileOptions(final CommandLine cmd) {
        final Config config = retrieveConfig(new File(cmd.getOptionValue('c')));
        addSharedOptions(cmd, config);
        return config;
    }

    /**
     * This method parses the provided configFile into its equivalent command-line args
     *
     * @param configFile containing config args
     * @return Array of args
     */
    protected Config retrieveConfig(final File configFile) {
        if (!configFile.exists()) {
            printHelp("Configuration file does not exist: " + configFile);
        }
        logger.debug("Loading configuration file: {}", configFile);
        try {
            final YamlReader reader = new YamlReader(new FileReader(configFile));
            @SuppressWarnings("unchecked")
            final Map<String, String> configVars = (HashMap<String, String>) reader.read();
            return configFromFile(configVars);

        } catch (final IOException | java.text.ParseException e) {
            throw new RuntimeException("Unable to read configuration file due to: " + e.getMessage(), e);
        }
    }

    /**
     * This method parses the command-line args
     *
     * @param cmd command line options
     * @return Config
     */
    private Config parseConfigurationArgs(final CommandLine cmd) {
        final Config config = new Config();

        // Inspect Mode option
        final String mode = cmd.getOptionValue('m');
        if (!mode.equalsIgnoreCase("import") && !mode.equalsIgnoreCase("export")) {
            printHelp("Invalid 'mode' option: " + mode);
        }

        config.setMode(mode);
        config.setResource(cmd.getOptionValue('r'));
        config.setBaseDirectory(cmd.getOptionValue('d'));
        config.setIncludeBinaries(cmd.hasOption('b'));
        config.setRetrieveExternal(cmd.hasOption('x'));
        config.setRetrieveInbound(cmd.hasOption('i'));
        config.setOverwriteTombstones(cmd.hasOption('t'));
        config.setLegacy(cmd.hasOption("L"));
        config.setIncludeVersions(cmd.hasOption('V'));

        if (cmd.getOptionValue('R') != null) {
            config.setRepositoryRoot(cmd.getOptionValue('R'));
        }

        if (cmd.getOptionValue('w') != null) {
            config.setWriteConfig(cmd.getOptionValue('w'));
        }
        final String rdfLanguage = cmd.getOptionValue('l');
        if (rdfLanguage != null) {
            config.setRdfLanguage(rdfLanguage);
        }
        if (cmd.getOptionValues('M') != null) {
            if (cmd.getOptionValues('M').length != 2) {
                throw new IllegalArgumentException("The map should contain the export and import baseURLs, "
                        + "separated by a comma");
            }
            config.setMap(cmd.getOptionValues('M'));
        }
        if (cmd.getOptionValues('p') != null) {
            config.setPredicates(cmd.getOptionValues('p'));
        }

        config.setBagProfile(cmd.getOptionValue('g'));
        config.setBagConfigPath(cmd.getOptionValue('G'));
        config.setBagSerialization(cmd.getOptionValue('s'));

        config.setAuditLog(cmd.hasOption('a'));

        return config;
    }



    /**
     * This method add/updates the values of any options that may be
     * valid in either scenario (config file or fully command line)
     *
     * @param cmd a parsed command line
     * @return Config the config which may be updated
     */
    private void addSharedOptions(final CommandLine cmd, final Config config) {
        final String user = cmd.getOptionValue("user");
        if (user != null) {
            if (user.indexOf(':') == -1) {
                printHelp("user option must be in the format username:password");
            } else {
                config.setUsername(user.substring(0, user.indexOf(':')));
                config.setPassword(user.substring(user.indexOf(':') + 1));
            }
        }
    }

    /**
     * This method writes the configuration file to disk.  The current
     * implementation omits the user/password information.
     * @param config to be persisted
     */
    private void saveConfig(final Config config) {
        final File configFile = config.getWriteConfig();

        // Write config to file
        if (configFile != null) {
            try {
                final YamlWriter writer = new YamlWriter(new FileWriter(configFile));
                logger.debug("YAML output is ({})", config.getMap().toString());
                writer.write(config.getMap());
                writer.close();
                logger.info("Saved configuration to: {}", configFile.getPath());

            } catch (final IOException e) {
                throw new RuntimeException("Unable to write configuration file due to: " + e.getMessage(), e);
            }
        }
    }

    /**
     * Parse command-line arguments.
     * @param args Command-line arguments
     * @return A configured Importer or Exporter instance.
    **/
    public TransferProcess parse(final String[] args) {
        final Config config = parseConfiguration(args);
        if (config.isImport()) {
            return new Importer(config, clientBuilder());
        } else if (config.isExport()) {
            return new Exporter(config, clientBuilder());
        }
        throw new IllegalArgumentException("Invalid mode parameter");
    }

    /**
     * Get a new client
     *
     * @return
     */
    private FcrepoClient.FcrepoClientBuilder clientBuilder() {
        return FcrepoClient.client();
    }

    /**
     * Print help/usage information
     *
     * @param message the message or null for none
     */
    private void printHelp(final String message) {
        final HelpFormatter formatter = new HelpFormatter();
        final PrintWriter writer = new PrintWriter(System.out);
        if (message != null) {
            writer.println("\n-----------------------\n" + message + "\n-----------------------\n");
        }

        writer.println("Running Import/Export Utility from command line arguments");
        formatter.printHelp(writer, 80, "java -jar import-export-driver.jar", "", configOptions, 4, 4, "", true);

        writer.println("\n--- or ---\n");

        writer.println("Running Import/Export Utility from configuration file");
        formatter.printHelp(writer, 80, "java -jar import-export-driver.jar", "", configFileOptions, 4, 4, "", true);

        writer.println("\n");
        writer.flush();

        if (message != null) {
            throw new RuntimeException(message);
        } else {
            throw new RuntimeException();
        }
    }

    /**
     * Static constructor using Yaml hashmap
     *
     * @param configVars config vars from Yaml file
     * @return Config object with values from Yaml
     * @throws java.text.ParseException If the Yaml does not parse correctly.
     */
    public static Config configFromFile(final Map<String, String> configVars) throws java.text.ParseException {
        final Config c = new Config();
        int lineNumber = 0;
        for (Map.Entry<String, String> entry : configVars.entrySet()) {
            logger.debug("config map entry is ({}) and value ({})", entry.getKey(), entry.getValue());
            lineNumber += 1;
            if (entry.getKey().equalsIgnoreCase("mode")) {
                if (entry.getValue().equalsIgnoreCase("import") || entry.getValue().equalsIgnoreCase("export")) {
                    c.setMode(entry.getValue());
                } else {
                    throw new java.text.ParseException(
                        String.format("Invalid value for \"mode\": %1$s", entry.getValue()), lineNumber);
                }
            } else if (entry.getKey().equalsIgnoreCase("resource")) {
                c.setResource(entry.getValue());
            } else if (entry.getKey().equalsIgnoreCase("map")) {
                if (entry.getValue().split(",").length != 2) {
                    throw new java.text.ParseException("The map should contain the export and import baseURLs, "
                            + "separated by a comma", lineNumber);
                }
                c.setMap(entry.getValue().split(","));
            } else if (entry.getKey().equalsIgnoreCase("dir")) {
                c.setBaseDirectory(entry.getValue());
            } else if (entry.getKey().equalsIgnoreCase("rdfLang")) {
                c.setRdfLanguage(entry.getValue());
            } else if (entry.getKey().trim().equalsIgnoreCase("binaries")) {
                c.setIncludeBinaries(parseBoolean("binaries", entry.getValue(), lineNumber));
            } else if (entry.getKey().trim().equalsIgnoreCase("external")) {
                c.setRetrieveExternal(parseBoolean("external", entry.getValue(), lineNumber));
            } else if (entry.getKey().trim().equalsIgnoreCase("inbound")) {
                c.setRetrieveInbound(parseBoolean("inbound", entry.getValue(), lineNumber));
            } else if (entry.getKey().trim().equalsIgnoreCase("writeConfig")) {
                c.setWriteConfig(entry.getValue());
            } else if (entry.getKey().trim().equalsIgnoreCase("overwriteTombstones")) {
                c.setOverwriteTombstones(parseBoolean("overwriteTombstones", entry.getValue(), lineNumber));
            } else if (entry.getKey().trim().equalsIgnoreCase("legacyMode")) {
                c.setLegacy(parseBoolean("legacyMode", entry.getValue(), lineNumber));
            } else if (entry.getKey().trim().equalsIgnoreCase("versions")) {
                c.setIncludeVersions(parseBoolean("versions", entry.getValue(), lineNumber));
            } else if (entry.getKey().equalsIgnoreCase(BAG_PROFILE_OPTION_KEY)) {
                c.setBagProfile(entry.getValue().toLowerCase());
            } else if (entry.getKey().equalsIgnoreCase(BAG_CONFIG_OPTION_KEY)) {
                c.setBagConfigPath(entry.getValue().toLowerCase());
            } else if (entry.getKey().equalsIgnoreCase("predicates")) {
                c.setPredicates(entry.getValue().split(","));
            } else if (entry.getKey().equalsIgnoreCase("auditLog")) {
                c.setAuditLog(parseBoolean("auditLog", entry.getValue(), lineNumber));
            } else {
                throw new java.text.ParseException(String.format("Unknown configuration key: %1$s", entry.getKey()),
                    lineNumber);
            }
        }
        return c;
    }

    private static boolean parseBoolean(final String key, final String value,
            final int lineNumber) throws java.text.ParseException {
        if (value.equalsIgnoreCase("true") || value.equalsIgnoreCase("false")) {
            return Boolean.parseBoolean(value);
        } else {
            throw new java.text.ParseException(String.format(
                "configuration parameter \"%1$s\" only accepts \"true\" or \"false\", \"%2$s\" received",
                key, value), lineNumber);
        }
    }
}
