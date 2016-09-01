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
package org.fcrepo.importer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * @author awoods
 * @since 2016-08-29
 */
public class ArgParser implements org.fcrepo.importexport.ArgParser {

    final private Options cmdOptions;

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
    }

    Config parseConfiguration(final String[] args) {
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

        } catch (ParseException e) {
            printHelp("Error parsing args: " + e.getMessage());
        }

        return config;
    }

    @Override
    public Importer parse(final String[] args) {
        return new Importer(parseConfiguration(args));
    }

    private void printHelp(final String message) {
        System.out.println("\n-----------------------\n" + message + "\n-----------------------\n");

        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("Running Import/Export Utility", cmdOptions);

        throw new RuntimeException();
    }

}
