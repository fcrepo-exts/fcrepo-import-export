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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.fcrepo.client.FcrepoClient;
import org.fcrepo.importexport.common.Config;
import org.fcrepo.importexport.common.TransferProcess;
import org.fcrepo.importexport.exporter.Exporter;
import org.fcrepo.importexport.importer.Importer;
import org.slf4j.Logger;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Command-line arguments parser.
 *
 * @author awoods
 * @author escowles
 * @since 2016-08-29
 */
public class ArgParser {

    private static final Logger logger = getLogger(ArgParser.class);

    public static final String DEFAULT_RDF_EXT = ".ttl";
    public static final String DEFAULT_RDF_LANG = "text/turtle";
    public static final String CONFIG_FILE_NAME = "importexport.config";

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

        // Binary Directory option
        configOptions.addOption(Option.builder("b")
                .longOpt("binDir")
                .hasArg(true).numberOfArgs(1).argName("binDir")
                .desc("Directory where binaries (files) are stored")
                .required(false).build());


        // Description Directory option
        configOptions.addOption(Option.builder("d")
                .longOpt("descDir")
                .hasArg(true).numberOfArgs(1).argName("descDir")
                .desc("Directory where the RDF descriptions are stored")
                .required(true).build());

        // RDF extension option
        configOptions.addOption(Option.builder("x")
                .longOpt("rdfExt")
                .hasArg(true).numberOfArgs(1).argName("rdfExt")
                .desc("RDF filename extension (default: " + DEFAULT_RDF_EXT + ")")
                .required(false).build());

        // RDF language option
        configOptions.addOption(Option.builder("l")
                .longOpt("rdfLang")
                .hasArg(true).numberOfArgs(1).argName("rdfLang")
                .desc("RDF language (default: " + DEFAULT_RDF_LANG + ")")
                .required(false).build());

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
    }

    protected Config parseConfiguration(final String[] args) {
        // first see if they've specified a config file
        CommandLine c = null;
        Config config = null;
        try {
            c = parseConfigFileCommandLineArgs(args);
            config = parseConfigFileOptions(c);
            addSharedOptions(c, config);
        } catch (ParseException ignore) {
            logger.debug("Command line argments weren't valid for specifying a config file.");
        }
        if (config == null) {
            // check for presence of the help flag
            if (helpFlagged(args)) {
                printHelpWithoutHeaderMessage();
            }

            try {
                c = parseConfigArgs(args);
                config = this.parseConfigurationArgs(c);
                addSharedOptions(c, config);
            } catch (ParseException e) {
                printHelp("Error parsing args: " + e.getMessage());
            }
        }

        // Write command line options to disk
        saveConfig(c);



        return config;
    }

    /**
     * @param args
     * @return
     */
    private boolean helpFlagged(final String[] args) {
        for (String arg : args) {
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
        final String[] fileArgs = retrieveConfig(new File(cmd.getOptionValue('c')));
        try {
            final Config config = parseConfigurationArgs(parseConfigArgs(fileArgs));
            addSharedOptions(cmd, config);
            return config;
        } catch (ParseException e) {
            printHelp("Unable to parse config file: " + e.getMessage());
            return null;
        }
    }

    /**
     * This method parses the provided configFile into its equivalent command-line args
     *
     * @param configFile containing config args
     * @return Array of args
     */
    private String[] retrieveConfig(final File configFile) {
        if (!configFile.exists()) {
            printHelp("Configuration file does not exist: " + configFile);
        }

        final ArrayList<String> args = new ArrayList<>();
        try {
            try (final BufferedReader configReader = new BufferedReader(new FileReader(configFile))) {
                String line = configReader.readLine();
                while (line != null) {
                    args.add(line);
                    line = configReader.readLine();
                }
            }
            return args.toArray(new String[args.size()]);

        } catch (IOException e) {
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
        config.setBinaryDirectory(cmd.getOptionValue('b'));
        config.setDescriptionDirectory(cmd.getOptionValue('d'));
        config.setRdfExtension(cmd.getOptionValue('x', DEFAULT_RDF_EXT));
        config.setRdfLanguage(cmd.getOptionValue('l', DEFAULT_RDF_LANG));
        config.setSource(cmd.getOptionValue('s'));

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
    private void saveConfig(final CommandLine cmd) {
        final File configFile = new File(System.getProperty("java.io.tmpdir"), CONFIG_FILE_NAME);

        // Leave existing config file alone
        if (configFile.exists()) {
            logger.info("Configuration file exists, new file will NOT be created: {}", configFile.getPath());
            return;
        }

        // Write config to file
        try (final BufferedWriter configWriter = new BufferedWriter(new FileWriter(configFile));) {
            for (Option option : cmd.getOptions()) {
                // write out all but the username/password
                if (!option.getOpt().equals("u")) {
                    configWriter.write("-" + option.getOpt());
                    configWriter.newLine();
                    if (option.getValue() != null) {
                        configWriter.write(option.getValue());
                        configWriter.newLine();
                    }
                }
            }

            logger.info("Saved configuration to: {}", configFile.getPath());

        } catch (IOException e) {
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
