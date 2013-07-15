package com.yeamanan.hadoop.example;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

/**
 * MaxTemperatureCommandLine class.
 * @author Yeam Anan (<yeamanan@gmail.com>)
 */
public class MaxTemperatureCommandLine {

    /**
     * Logger.
     */
    private static final Logger LOGGER =
            Logger.getLogger(MaxTemperatureCommandLine.class);

    /**
     * Command Line.
     */
    private CommandLine commandLine;

    /**
     * Constructor.
     * @param args arguments
     */
    public MaxTemperatureCommandLine(final String[] args) {
        final Options options = new Options();
        options.addOption("combiner", false, "use a combiner");
        options.addOption("input", true,
                "input file or folder (input/ by default)");
        options.addOption("output", true, "output folder (output/ by default)");
        final CommandLineParser parser = new BasicParser();
        try {
            commandLine = parser.parse(options, args);
        } catch (ParseException e) {
            LOGGER.fatal("Fatal error", e);
        }
    }

    /**
     * getInputPath() method.
     * @return input path
     */
    public final String getInputPath() {
        String inputPath = commandLine.getOptionValue("input");
        if (inputPath == null) {
            inputPath = "./input/";
        }
        return inputPath;
    }

    /**
     * hasCombinerOption() method.
     * @return output path
     */
    public final String getOutputPath() {
        String outputPath = commandLine.getOptionValue("output");
        if (outputPath == null) {
            outputPath = "./output/";
        }
        return outputPath;
    }

    /**
     * hasCombinerOption() method.
     * @return boolean
     */
    public final boolean hasCombinerOption() {
        boolean result;
        if (commandLine.hasOption("combiner")) {
            result = true;
        } else {
            result = false;
        }
        return result;
    }

}
