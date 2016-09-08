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
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.fcrepo.client.FcrepoClient;
import org.fcrepo.exporter.Exporter;
import org.fcrepo.importer.Importer;
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

        // Mode option
        final Option importExportOption = new Option("m", "mode", true, "Mode: [import|export]");
        importExportOption.setRequired(true);
        importExportOption.setArgs(1);
        importExportOption.setArgName("mode");
        configOptions.addOption(importExportOption);

        // Resource option
        final Option resourceOption = new Option("r", "resource", true, "Resource (URI) to import/export");
        resourceOption.setRequired(true);
        resourceOption.setArgs(1);
        resourceOption.setArgName("resource");
        configOptions.addOption(resourceOption);

        // Binary Directory option
        final Option binDirOption = new Option("b", "binDir", true, "Directory where binaries (files) are stored");
        binDirOption.setRequired(false);
        binDirOption.setArgs(1);
        binDirOption.setArgName("binDir");
        configOptions.addOption(binDirOption);

        // Description Directory option
        final Option descDirOption = new Option("d", "descDir", true, "Directory where RDF descriptions are stored");
        descDirOption.setRequired(true);
        descDirOption.setArgs(1);
        descDirOption.setArgName("descDir");
        configOptions.addOption(descDirOption);

        // RDF extension option
        final Option rdfExtOption = new Option("x", "rdfExt", true, "RDF filename extension (default: " +
                DEFAULT_RDF_EXT);
        rdfExtOption.setRequired(false);
        rdfExtOption.setArgs(1);
        rdfExtOption.setArgName("rdfExt");
        configOptions.addOption(rdfExtOption);

        // RDF language option
        final Option rdfLangOption = new Option("l", "rdfLang", true, "RDF language (default: " + DEFAULT_RDF_LANG);
        rdfLangOption.setRequired(false);
        rdfLangOption.setArgs(1);
        rdfLangOption.setArgName("rdfLang");
        configOptions.addOption(rdfLangOption);

        // Config file option
        final Option configFileOption = new Option("c", "config", true, "Path to config file");
        configFileOption.setRequired(true);
        configFileOption.setArgs(1);
        configFileOption.setArgName("config");
        configFileOptions.addOption(configFileOption);
    }

    protected Config parseConfiguration(final String[] args) {
        // Inspect Config File option
        Config config = parseConfigFileOptions(args);
        if (config == null) {
            // Inspect standard command-line options
            config = parseConfigurationArgs(args);
        }

        // Write configuration to disk
        saveConfig(args);

        return config;
    }

    /**
     * This method tries to parse the configuration file, if that option was provided
     *
     * @param args from command line
     * @return Config or null if no config file option was provided
     */
    private Config parseConfigFileOptions(final String[] args) {
        final CommandLineParser cmdParser = new DefaultParser();
        try {
            final CommandLine cmd = cmdParser.parse(configFileOptions, args);
            final String[] fileArgs = retrieveConfig(new File(cmd.getOptionValue('c')));
            return parseConfigurationArgs(fileArgs);

        } catch (final ParseException e) {
            logger.debug("Unable to parse config file: {}", e.getMessage());
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
            final BufferedReader configReader = new BufferedReader(new FileReader(configFile));
            String line = configReader.readLine();
            while (line != null) {
                args.add(line);
                line = configReader.readLine();
            }
            return args.toArray(new String[args.size()]);

        } catch (IOException e) {
            throw new RuntimeException("Unable to read configuration file due to: " + e.getMessage(), e);
        }
    }

    /**
     * This method parses the command-line args
     *
     * @param args to be parsed
     * @return Config
     */
    private Config parseConfigurationArgs(final String[] args) {
        final Config config = new Config();

        final CommandLineParser cmdParser = new DefaultParser();
        try {
            final CommandLine cmd = cmdParser.parse(configOptions, args);

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

        } catch (final ParseException e) {
            printHelp("Error parsing args: " + e.getMessage());
        }
        return config;
    }

    /**
     * This method writes the configuration file to disk
     *
     * @param args to be persisted
     */
    private void saveConfig(final String[] args) {
        final File configFile = new File(System.getProperty("java.io.tmpdir"), CONFIG_FILE_NAME);

        // Leave existing config file alone
        if (configFile.exists()) {
            logger.info("Configuration file exists, new file will NOT be created: {}", configFile.getPath());
            return;
        }

        // Write config to file
        try (final BufferedWriter configWriter = new BufferedWriter(new FileWriter(configFile));) {
            for (final String arg : args) {
                configWriter.write(arg);
                configWriter.newLine();
                configWriter.flush();
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

    private void printHelp(final String message) {
        final HelpFormatter formatter = new HelpFormatter();
        final PrintWriter writer = new PrintWriter(System.out);

        writer.println("\n-----------------------\n" + message + "\n-----------------------\n");

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
