/*
 * Copyright 2015-2016 Etsy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.etsy.arbiter;

import com.etsy.arbiter.config.Config;
import com.etsy.arbiter.config.ConfigurationMerger;
import com.etsy.arbiter.exception.ConfigurationException;
import com.etsy.arbiter.util.YamlReader;
import com.google.common.collect.Lists;
import org.apache.commons.cli.*;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Entry point for Arbiter
 *
 * @author Andrew Johnson
 */
public class Arbiter {
    private Arbiter() { }

    public static void main(String[] args) throws ParseException, ConfigurationException, IOException, ParserConfigurationException, TransformerException {
        Options options = getOptions();

        CommandLineParser cmd = new GnuParser();
        CommandLine parsed = cmd.parse(options, args);

        if (parsed.hasOption("h")) {
            printUsage(options);
        }

        if (!parsed.hasOption("i")) {
            throw new ParseException("Missing required argument: i");
        }

        if (!parsed.hasOption("o")) {
            throw new ParseException("Missing required argument: o");
        }

        String[] configFiles = parsed.getOptionValues("c");
        String[] lowPrecedenceConfigFiles = parsed.getOptionValues("l");

        String[] inputFiles = parsed.getOptionValues("i");
        String outputDir = parsed.getOptionValue("o");

        List<Config> parsedConfigFiles = readConfigFiles(configFiles, false);
        parsedConfigFiles.addAll(readConfigFiles(lowPrecedenceConfigFiles, true));
        Config merged = ConfigurationMerger.mergeConfiguration(parsedConfigFiles);

        List<Workflow> workflows = readWorkflowFiles(inputFiles);

        boolean generateGraphviz = parsed.hasOption("g");
        String graphvizFormat = parsed.getOptionValue("g", "svg");

        OozieWorkflowGenerator generator = new OozieWorkflowGenerator(merged);
        generator.generateOozieWorkflows(outputDir, workflows, generateGraphviz, graphvizFormat);
    }

    /**
     * Reads in a list of workflow files
     *
     * @param files The list of files to read
     * @return A list of Workflow objects corresponding to the given files
     */
    public static List<Workflow> readWorkflowFiles(String[] files) {
        if (files == null) {
            return Lists.newArrayList();
        }

        YamlReader<Workflow> reader = new YamlReader<>(Workflow.getYamlConstructor());

        ArrayList<Workflow> result = Lists.newArrayList();

        for (String file : files) {
            File f = new File(file);
            result.add(reader.read(f));
        }

        return result;
    }

    /**
     * Reads in a list of configuration files
     *
     * @param files The list of files to read
     * @param lowPrecedence Whether or not these configurations should be marked as low-priority
     * @return A List of Config objects corresponding to the given files
     */
    public static List<Config> readConfigFiles(String[] files, boolean lowPrecedence) {
        if (files == null) {
            return Lists.newArrayList();
        }

        YamlReader<Config> reader = new YamlReader<>(Config.getYamlConstructor());
        
        ArrayList<Config> result = Lists.newArrayList();
        
        for (String file : files) {
            File f = new File(file);
            Config c = reader.read(f);
            c.setLowPrecedence(lowPrecedence);
            result.add(c);
        }
        
        return result;
    }

    /**
     * Prints the usage for Arbiter
     *
     * @param options The CLI options
     */
    private static void printUsage(Options options) {
        HelpFormatter help = new HelpFormatter();
        help.printHelp("arbiter", options);
        System.exit(0);
    }

    /**
     * Construct the CLI Options
     *
     * @return The Options object representing the supported CLI options
     */
    @SuppressWarnings("static-access")
    private static Options getOptions() {
        Option config = OptionBuilder
                .withArgName("config")
                .withLongOpt("config")
                .hasArgs()
                .withDescription("Configuration file")
                .create("c");

        Option lowPrecedenceConfig = OptionBuilder
                .withArgName("lowPrecedenceConfig")
                .withLongOpt("low-priority-config")
                .hasArgs()
                .withDescription("Low-priority configuration file")
                .create("l");

        Option inputFile = OptionBuilder
                .withArgName("input")
                .withLongOpt("input")
                .hasArgs()
                .withDescription("Input Arbiter workflow file")
                .create("i");

        Option outputDir = OptionBuilder
                .withArgName("output")
                .withLongOpt("output")
                .hasArg()
                .withDescription("Output directory")
                .create("o");

        Option help = OptionBuilder
                .withArgName("help")
                .withLongOpt("help")
                .withDescription("Print usage")
                .create("h");

        Option graphviz = OptionBuilder
                .withArgName("graphviz")
                .withLongOpt("graphviz")
                .hasOptionalArg()
                .withDescription("Generate the Graphviz DOT file and PNG")
                .create("g");

        Options options = new Options();
        options.addOption(config)
                .addOption(lowPrecedenceConfig)
                .addOption(inputFile)
                .addOption(outputDir)
                .addOption(help)
                .addOption(graphviz);

        return options;
    }
}
