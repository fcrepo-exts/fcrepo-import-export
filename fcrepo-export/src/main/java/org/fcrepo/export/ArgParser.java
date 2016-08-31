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

import java.net.URISyntaxException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * Command-line arguments parser.
 *
 * @author awoods
 * @author escowles
 * @since 2016-08-29
 */
public class ArgParser {

    final private Options cmdOptions;

    public static final String DEFAULT_RDF_EXT = ".ttl";
    public static final String DEFAULT_RDF_LANG = "text/turtle";

    /**
     * Constructor that creates the command line options
     */
    public ArgParser() {
        // Command Line Options
        cmdOptions = new Options();

        // Help option
        final Option helpOption = new Option("h", "help", false, "Print this message");
        helpOption.setRequired(false);
        cmdOptions.addOption(helpOption);

        // Mode option
        final Option importExportOption = new Option("m", "mode", true, "Mode: [import|export]");
        importExportOption.setRequired(true);
        importExportOption.setArgs(1);
        importExportOption.setArgName("mode");
        cmdOptions.addOption(importExportOption);

        // Resource option
        final Option resourceOption = new Option("r", "resource", true, "Resource (URI) to import/export");
        resourceOption.setRequired(true);
        resourceOption.setArgs(1);
        resourceOption.setArgName("resource");
        cmdOptions.addOption(resourceOption);

        // Binary Directory option
        final Option binDirOption = new Option("b", "binDir", true, "Directory to store binaries (files)");
        binDirOption.setRequired(false);
        binDirOption.setArgs(1);
        binDirOption.setArgName("binDir");
        cmdOptions.addOption(binDirOption);

        // Description Directory option
        final Option descDirOption = new Option("d", "descDir", true, "Directory to store RDF descriptions");
        descDirOption.setRequired(true);
        descDirOption.setArgs(1);
        descDirOption.setArgName("descDir");
        cmdOptions.addOption(descDirOption);

        // RDF extension option
        final Option rdfExtOption = new Option("x", "rdfExt", true, "RDF filename extension (default: " + DEFAULT_RDF_EXT);
        rdfExtOption.setRequired(false);
        rdfExtOption.setArgs(1);
        rdfExtOption.setArgName("rdfExt");
        cmdOptions.addOption(rdfExtOption);

        // RDF language option
        final Option rdfLangOption = new Option("l", "rdfLang", true, "RDF language (default: " + DEFAULT_RDF_LANG);
        rdfLangOption.setRequired(false);
        rdfLangOption.setArgs(1);
        rdfLangOption.setArgName("rdfLang");
        cmdOptions.addOption(rdfLangOption);
    }

    /**
     * This method parses the command line options, returning the Import/Export configuration
     *
     * @param args from the command line
     * @return Import/Export configuration
     */
    public Config parse(final String[] args) {
        final Config config = new Config();

        final CommandLineParser cmdParser = new DefaultParser();
        try {
            final CommandLine cmd = cmdParser.parse(cmdOptions, args);

            // Inspect help option
            if (cmd.hasOption('h')) {
                printHelp("User Help");
            }

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

        } catch (ParseException e) {
            printHelp("Error parsing args: " + e.getMessage());
        } catch (URISyntaxException e) {
            printHelp("Error parsing resource URI: " + e.getMessage());
        }

        return config;
    }

    private void printHelp(final String message) {
        System.out.println("\n-----------------------\n" + message + "\n-----------------------\n");

        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("Running Import/Export Utility", cmdOptions);

        throw new RuntimeException();
    }

}
