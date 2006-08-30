/**
 * Copyright 2005 The Apache Software Foundation
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

package org.apache.nutch.util;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;

/**
 * This is a verbatim copy of org.apache.hadoop.util.ToolBase, with
 * a temporary workaround for Hadoop's ToolBase.doMain() deficiency.
 * This class should be removed when HADOOP-488 is fixed, and all its
 * uses should be replaced with org.apache.hadoop.util.ToolBase.
 * 
 * @author Andrzej Bialecki
 */
public abstract class ToolBase implements Tool {
    public static final Log LOG = LogFactory.getLog(
            "org.apache.nutch.util.ToolBase");
    public Configuration conf;

    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    public Configuration getConf() {
        return conf;
    }
    
    /*
     * Specify properties of each generic option
     */
    static private Options buildGeneralOptions() {
        Option fs = OptionBuilder.withArgName("local|namenode:port")
                                 .hasArg()
                                 .withDescription("specify a namenode")
                                 .create("fs");
        Option jt = OptionBuilder.withArgName("local|jobtracker:port")
                                 .hasArg()
                                 .withDescription("specify a job tracker")
                                 .create("jt");
        Option oconf = OptionBuilder.withArgName("configuration file")
                .hasArg()
                .withDescription("specify an application configuration file" )
                .create("conf");
        Option property = OptionBuilder.withArgName("property=value")
                              .hasArgs()
                              .withArgPattern("=", 1)
                              .withDescription("use value for given property")
                              .create('D');
        Options opts = new Options();
        opts.addOption(fs);
        opts.addOption(jt);
        opts.addOption(oconf);
        opts.addOption(property);
        
        return opts;
    }
    
    /*
     * Modify configuration according user-specified generic options
     * @param conf Configuration to be modified
     * @param line User-specified generic options
     */
    static private void processGeneralOptions( Configuration conf,
                                               CommandLine line ) {
        if(line.hasOption("fs")) {
            conf.set("fs.default.name", line.getOptionValue("fs"));
        }
        
        if(line.hasOption("jt")) {
            conf.set("mapred.job.tracker", line.getOptionValue("jt"));
        }
        if(line.hasOption("conf")) {
            conf.addFinalResource(new Path(line.getOptionValue("conf")));
        }
        if(line.hasOption('D')) {
            String[] property = line.getOptionValues('D');
            for(int i=0; i<property.length-1; i=i+2) {
                if(property[i]!=null)
                    conf.set(property[i], property[i+1]);
            }
         }           
    }
 
    /**
     * Parse the user-specified options, get the generic options, and modify
     * configuration accordingly
     * @param conf Configuration to be modified
     * @param args User-specified arguments
     * @return Commoand-specific arguments
     */
    static private String[] parseGeneralOptions( Configuration conf, 
                 String[] args ) {
        Options opts = buildGeneralOptions();
        CommandLineParser parser = new GnuParser();
        try {
          CommandLine line = parser.parse( opts, args, true );
          processGeneralOptions( conf, line );
          return line.getArgs();
        } catch(ParseException e) {
          LOG.warn("options parsing failed: "+e.getMessage());

          HelpFormatter formatter = new HelpFormatter();
          formatter.printHelp("general options are: ", opts);
        }
        return args;
    }

    /**
     * Work as a main program: execute a command and handle exception if any
     * @param conf Application default configuration
     * @param args User-specified arguments
     * @throws Exception
     */
    public final int doMain(Configuration conf, String[] args) throws Exception {
        String [] commandOptions = parseGeneralOptions(conf, args);
        setConf(conf);
        return this.run(commandOptions);
    }

}
