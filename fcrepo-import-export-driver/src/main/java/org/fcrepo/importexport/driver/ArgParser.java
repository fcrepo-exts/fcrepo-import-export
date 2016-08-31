package org.fcrepo.importexport.driver;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.fcrepo.importexport.Config;

public class ArgParser implements org.fcrepo.importexport.ArgParser {

	private final Options cmdOptions = new Options();
	@Override
	public Config parse(String[] args) {
        // Command Line Options

        final CommandLineParser cmdParser = new DefaultParser();
        try {
            final CommandLine cmd = cmdParser.parse(cmdOptions, args);
            // Inspect Mode option
            final String mode = cmd.getOptionValue('m');
            
            if (!mode.equalsIgnoreCase("import") && !mode.equalsIgnoreCase("export")) {
                printHelp("Invalid 'mode' option: " + mode);
            }

            // Inspect help option
            if (cmd.hasOption('h')) {
                printHelp("User Help");
            }

            org.fcrepo.importexport.ArgParser delegate = null;
            if (mode.equalsIgnoreCase("import")) {
            	delegate = new org.fcrepo.importer.ArgParser();
            } else if (mode.equalsIgnoreCase("export")) {
            	delegate = new org.fcrepo.exporter.ArgParser();
            } else {
                printHelp("Invalid 'mode' option: " + mode);
                return null;
            }

            final Config config = delegate.parse(args);
            config.setMode(mode);
            return config;
        } catch (ParseException e) {
            printHelp("Error parsing args: " + e.getMessage());
            return null;
        }
	}

    private void printHelp(final String message) {
        System.out.println("\n-----------------------\n" + message + "\n-----------------------\n");

        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("Running Import/Export Utility", cmdOptions);

        throw new RuntimeException();
    }
}
