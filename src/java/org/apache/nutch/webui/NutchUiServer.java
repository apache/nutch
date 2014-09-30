/**
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
 */
package org.apache.nutch.webui;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.util.StringUtils;
import org.apache.wicket.protocol.http.WicketFilter;
import org.apache.wicket.spring.SpringWebApplicationFactory;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.DefaultServlet;
import org.mortbay.jetty.servlet.FilterHolder;
import org.springframework.web.context.ContextLoaderListener;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.request.RequestContextListener;
import org.springframework.web.context.support.AnnotationConfigWebApplicationContext;

public class NutchUiServer {
  private static final String APP_FACTORY_NAME = SpringWebApplicationFactory.class
      .getName();
  private static final String CONFIG_LOCATION = "org.apache.nutch.webui";
  private static final String CMD_PORT = "port";
  private static Integer port = 8080;

  public static void main(String[] args) throws Exception {
    CommandLineParser parser = new GnuParser();
    Options options = createWebAppOptions();
    CommandLine commandLine = null;
    HelpFormatter formatter = new HelpFormatter();
    try {
      commandLine = parser.parse(options, args);
    }  catch (Exception e) {
      formatter.printHelp("NutchUiServer", options, true);
      StringUtils.stringifyException(e);
    }

    if (commandLine.hasOption("help")) {
      formatter.printHelp("NutchUiServer", options, true);
      return;
    }
    if (commandLine.hasOption(CMD_PORT)) {
      port = Integer.parseInt(commandLine.getOptionValue(CMD_PORT));
    }
    startServer();
  }

  private static void startServer() throws Exception, InterruptedException {
    Server server = new Server(port);
    Context context = new Context(server, "/", Context.SESSIONS);
    context.addServlet(DefaultServlet.class, "/*");

    context.addEventListener(new ContextLoaderListener(getContext()));
    context.addEventListener(new RequestContextListener());

    WicketFilter filter = new WicketFilter();
    filter.setFilterPath("/");
    FilterHolder holder = new FilterHolder(filter);
    holder.setInitParameter("applicationFactoryClassName", APP_FACTORY_NAME);
    context.addFilter(holder, "/*", Handler.DEFAULT);

    server.setHandler(context);
    server.start();
    server.join();
  }

  private static WebApplicationContext getContext() {
    AnnotationConfigWebApplicationContext context = new AnnotationConfigWebApplicationContext();
    context.setConfigLocation(CONFIG_LOCATION);
    return context;
  }

  private static Options createWebAppOptions() {
    Options options = new Options();
    Option helpOpt = new Option("h", "help", false, "show this help message");
    OptionBuilder.withDescription("Port to run the WebApplication on.");
    OptionBuilder.hasOptionalArg();
    OptionBuilder.withArgName("port number");
    options.addOption(OptionBuilder.create(CMD_PORT));
    options.addOption(helpOpt);
    return options;
  }

}
