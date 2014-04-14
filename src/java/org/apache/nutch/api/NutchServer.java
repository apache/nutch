/*******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.apache.nutch.api;

import java.util.List;
import java.util.logging.Level;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.lang.StringUtils;
import org.apache.nutch.api.JobStatus.State;
import org.restlet.Component;
import org.restlet.data.Protocol;
import org.restlet.data.Reference;
import org.restlet.representation.Representation;
import org.restlet.resource.ClientResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NutchServer {
  private static final Logger LOG = LoggerFactory.getLogger(NutchServer.class);

  private static final String LOCALHOST = "localhost";
  private static final String DEFAULT_LOG_LEVEL = "INFO";
  private static final Integer DEFAULT_PORT = 8081;
  
  private static String logLevel = DEFAULT_LOG_LEVEL;
  private static Integer port = DEFAULT_PORT;
  
  private static final String CMD_HELP = "help";
  private static final String CMD_STOP = "stop";
  private static final String CMD_PORT = "port";
  private static final String CMD_LOG_LEVEL = "log";

  
  private Component component;
  private NutchApp app;
  private boolean running;

  /**
   * Public constructor which accepts the port we wish to run the server on as
   * well as the logging granularity. If the latter option is not provided via
   * {@link org.apache.nutch.api.NutchServer#main(String[])} then it defaults to
   * 'INFO' however best attempts should always be made to specify a logging
   * level.
   */
  public NutchServer() {
    // Create a new Component.
    component = new Component();
    component.getLogger().setLevel(Level.parse(logLevel));

    // Add a new HTTP server listening on defined port.
    component.getServers().add(Protocol.HTTP, port);

    // Attach the application.
    app = new NutchApp();
    component.getDefaultHost().attach("/nutch", app); 

    NutchApp.server = this;
  }

  /**
   * Convenience method to determine whether a Nutch server is running.
   * 
   * @return true if a server instance is running.
   */
  public boolean isRunning() {
    return running;
  }

  /**
   * Starts the Nutch server printing some logging to the log file.
   * 
   * @throws Exception
   */
  public void start() throws Exception {
    LOG.info("Starting NutchServer on port: {} with logging level: {} ...",
        port, logLevel);
    component.start();
    LOG.info("Started NutchServer on port {}", port);
    running = true;
    NutchApp.started = System.currentTimeMillis();
  }

  /**
   * Safety and convenience method to determine whether or not it is safe to
   * shut down the server. We make this assertion by consulting the
   * {@link org.apache.nutch.api.NutchApp#jobMgr} for a list of jobs with
   * {@link org.apache.nutch.api.JobStatus#state} equal to 'RUNNING'.
   * 
   * @return true if there are no jobs running or false if there are jobs with
   *         running state.
   * @throws Exception
   */
  public boolean canStop() throws Exception {
    List<JobStatus> jobs = NutchApp.jobMgr.list(null, State.RUNNING);
    return jobs.isEmpty();
  }

  /**
   * Stop the Nutch server.
   * 
   * @param force
   *          boolean method to effectively kill jobs regardless of state.
   * @return true if no server is running or if the shutdown was successful.
   *         Return false if there are running jobs and the force switch has not
   *         been activated.
   * @throws Exception
   */
  public boolean stop(boolean force) throws Exception {
    if (!NutchApp.server.running) {
      return true;
    }
    if (!NutchApp.server.canStop() && !force) {
      LOG.warn("Running jobs - can't stop now.");
      return false;
    }
    LOG.info("Stopping NutchServer on port {}...", port);
    component.stop();
    LOG.info("Stopped NutchServer on port {}", port);
    running = false;
    return true;
  }

  public static void main(String[] args) throws Exception {
    CommandLineParser parser = new PosixParser();
    Options options = createOptions();
    CommandLine commandLine = parser.parse(options, args);

    if (commandLine.hasOption(CMD_HELP)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("NutchServer", options, true);
      return;
    }
    
    if (commandLine.hasOption(CMD_LOG_LEVEL)) {
      logLevel = commandLine.getOptionValue(CMD_LOG_LEVEL);
    }
    
    if (commandLine.hasOption(CMD_PORT)) {
      port = Integer.parseInt(commandLine.getOptionValue(CMD_PORT));
    }

    if (commandLine.hasOption(CMD_STOP)) {
      String stopParameter = commandLine.getOptionValue(CMD_STOP);
      boolean force = StringUtils.equals(Params.FORCE, stopParameter);
      stopRemoteServer(force);
      return;
    }
    
    startServer();
  }
  
  private static void startServer() throws Exception {
    NutchServer server = new NutchServer();
    server.start();
  }
  
  private static void stopRemoteServer(boolean force) throws Exception {
    Reference reference = new Reference(Protocol.HTTP, LOCALHOST, port);
    reference.setPath("/nutch/admin/stop");
    
    if (force) {
      reference.addQueryParameter(Params.FORCE, Params.TRUE);
    }
    
    ClientResource clientResource = new ClientResource(reference);
    Representation response = clientResource.get();
    LOG.info("Server response: {} ", response.getText());
  }

  private static Options createOptions() {
    Options options = new Options();
    OptionBuilder.hasArg();
    OptionBuilder.withArgName("logging level");
    OptionBuilder.withDescription("Select a logging level for the NutchServer: \n"
        + "ALL|CONFIG|FINER|FINEST|INFO|OFF|SEVERE|WARNING");
    options.addOption(OptionBuilder.create(CMD_LOG_LEVEL));

    OptionBuilder.withDescription("Stop running NutchServer. "
        + "true value forces the Server to stop despite running jobs e.g. kills the tasks ");
    OptionBuilder.hasOptionalArg();
    OptionBuilder.withArgName("force");
    options.addOption(OptionBuilder.create(CMD_STOP));

    OptionBuilder.withDescription("Show this help");
    options.addOption(OptionBuilder.create(CMD_HELP));

    OptionBuilder.withDescription("Port to use for restful API.");
    OptionBuilder.hasOptionalArg();
    OptionBuilder.withArgName("port number");
    options.addOption(OptionBuilder.create(CMD_PORT));
    return options;
  }
}
