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

import static org.fcrepo.importexport.common.FcrepoConstants.CONTAINS;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

import org.fcrepo.client.FcrepoClient;
import org.fcrepo.importexport.common.Config;
import org.fcrepo.importexport.common.TransferProcess;
import org.fcrepo.importexport.exporter.Exporter;
import org.fcrepo.importexport.importer.Importer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;

import com.esotericsoftware.yamlbeans.YamlReader;
import com.esotericsoftware.yamlbeans.YamlWriter;
/**
 * Command-line arguments parser.
 *
 * @author awoods
 * @author escowles
 * @since 2016-08-29
 */
public class ArgParser {

    private static final Logger logger = getLogger(ArgParser.class);

    public static final String DEFAULT_RDF_LANG = "text/turtle";
    public static final String DEFAULT_RDF_EXT = getRDFExtension(DEFAULT_RDF_LANG);
    public static final String[] DEFAULT_PREDICATES = new String[]{ CONTAINS.toString() };

    public static final String CONFIG_FILE_NAME = "importexport.yml";

    private final Options configOptions;
    private final Options configFileOptions;

    /**
     * Constructor that creates the command line options
     */
    public ArgParser() {
        // Command Line Options
        configOptions = new Options();
        configFileOptions = new Options();

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
        configOptions.addOption(Option.builder("s")
                .longOpt("source")
                .hasArg(true).numberOfArgs(1).argName("source")
                .desc("Source (URI) data was exported from, when importing to a different Fedora URI")
                .required(false).build());

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

    protected Config parseConfiguration(final String[] args) {
        // first see if they've specified a config file
        CommandLine c = null;
        Config config = null;
        try {
            c = parseConfigFileCommandLineArgs(args);
            config = parseConfigFileOptions(c);
            addSharedOptions(c, config);
        } catch (final ParseException ignore) {
            logger.debug("Command line argments weren't valid for specifying a config file.");
        }
        if (config == null) {
            // check for presence of the help flag
            if (helpFlagged(args)) {
                printHelpWithoutHeaderMessage();
            }

            try {
                c = parseConfigArgs(args);
                config = parseConfigurationArgs(c);
                addSharedOptions(c, config);
            } catch (final ParseException e) {
                printHelp("Error parsing args: " + e.getMessage());
            }
            // Write command line options to disk
            saveConfig(config);
        }

        return config;
    }

    /**
     * @param args
     * @return
     */
    private boolean helpFlagged(final String[] args) {
        for (final String arg : args) {
            if (arg.equals("-h") || arg.equals("--help")) {
                return true;
            }
        }

        return false;
    }

    private CommandLine parseConfigFileCommandLineArgs(final String[] args) throws ParseException {
        return new DefaultParser().parse(configFileOptions, args);
    }

    private CommandLine parseConfigArgs(final String[] args) throws ParseException {
        return new DefaultParser().parse(configOptions, args);
    }

    /**
     * This method tries to parse the configuration file, if that option was provided
     *
     * @param args from command line
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
    private Config retrieveConfig(final File configFile) {
        if (!configFile.exists()) {
            printHelp("Configuration file does not exist: " + configFile);
        }

        try {
            final YamlReader reader = new YamlReader(new FileReader(configFile));
            @SuppressWarnings("unchecked")
            final Map<String, String> configVars = (HashMap<String, String>) reader.read();
            return Config.fromFile(configVars);

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

        final String rdfLanguage = cmd.getOptionValue('l');
        if (rdfLanguage != null) {
            config.setRdfLanguage(rdfLanguage);
        }
        config.setSource(cmd.getOptionValue('s'));
        config.setPredicates((cmd.getOptionValues('p') == null) ? DEFAULT_PREDICATES : cmd.getOptionValues('p'));
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
     * @param args to be persisted
     */
    private void saveConfig(final Config config) {
        final File configFile = new File(System.getProperty("java.io.tmpdir"), CONFIG_FILE_NAME);

        // Leave existing config file alone
        if (configFile.exists()) {
            logger.info("Configuration file exists, new file will NOT be created: {}", configFile.getPath());
            return;
        }

        // Write config to file
        try {
            final YamlWriter writer = new YamlWriter(new FileWriter(configFile));
            writer.write(config.getMap());

            logger.info("Saved configuration to: {}", configFile.getPath());

        } catch (final IOException e) {
            throw new RuntimeException("Unable to write configuration file due to: " + e.getMessage(), e);
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

    private FcrepoClient.FcrepoClientBuilder clientBuilder() {
        return FcrepoClient.client();
    }

    private void printHelpWithoutHeaderMessage() {
        printHelp(null);
    }

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

        throw new RuntimeException(message);
    }

}
