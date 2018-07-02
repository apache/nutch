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

package org.apache.nutch.net;

import org.apache.hadoop.util.ToolRunner;

import org.apache.nutch.util.AbstractChecker;
import org.apache.nutch.util.NutchConfiguration;

/**
 * Checks one given filter or all filters.
 * 
 * @author John Xing
 */
public class URLFilterChecker extends AbstractChecker {

  private URLFilters filters = null;

  public int run(String[] args) throws Exception {
    usage = "Usage: URLFilterChecker [-Dproperty=value]... [-filterName filterName] (-stdin | -listen <port> [-keepClientCnxOpen]) \n"
        + "\n  -filterName\tURL filter plugin name (eg. urlfilter-regex) to check,"
        + "\n             \t(if not given all configured URL filters are applied)"
        + "\n  -stdin     \ttool reads a list of URLs from stdin, one URL per line"
        + "\n  -listen <port>\trun tool as Telnet server listening on <port>\n";

    // Print help when no args given
    if (args.length < 1) {
      System.err.println(usage);
      System.exit(-1);
    }

    int numConsumed;
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-filterName")) {
        getConf().set("plugin.includes", args[++i]);
      } else if ((numConsumed = super.parseArgs(args, i)) > 0) {
        i += numConsumed - 1;
      } else {
        System.err.println("ERROR: Not a recognized argument: " + args[i]);
        System.err.println(usage);
        System.exit(-1);
      }
    }

    // Print active filter list
    filters = new URLFilters(getConf());
    System.out.print("Checking combination of these URLFilters: ");
    for (URLFilter filter : filters.getFilters()) {
      System.out.print(filter.getClass().getSimpleName() + " ");
    }
    System.out.println("");

    // Start listening
    return super.run();
  }

  protected int process(String line, StringBuilder output) throws Exception {
    String out = filters.filter(line);
    if (out != null) {
      output.append("+");
      output.append(out);
    } else {
      output.append("-");
      output.append(line);
    }
    return 0;
  }

  public static void main(String[] args) throws Exception {
    final int res = ToolRunner.run(NutchConfiguration.create(),
        new URLFilterChecker(), args);
    System.exit(res);
  }
}
