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
package org.apache.nutch.tools;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;

import org.apache.nutch.util.NutchConfiguration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Tool to list properties and their values set by the current Nutch
 * configuration
 */
public class ShowProperties extends Configured implements Tool {

  private String format3cols = "%-32s  %24s  %20s";

  @Override
  public int run(String[] arg0) {
    Configuration conf = getConf();
    List<Entry<String, String>> list = new ArrayList<>();
    conf.iterator().forEachRemaining(list::add);
    Collections.sort(list, (a, b) -> a.getKey().compareTo(b.getKey()));
    System.out.println(
        String.format(format3cols, "conf.name", "conf.value", "substituted.value"));
    System.out.println(
        "================================================================================");
    for (Entry<String, String> e : list) {
      String key = e.getKey();
      String val = e.getValue();
      String substitutedVal = conf.get(key);
      if (val.equals(substitutedVal)) {
        String format = String.format("%%-%ds  %%%ds", key.length(),
            (80 - 2 - key.length()));
        System.out.println(String.format(format, key, val));
      } else {
        String format = String.format("%%-%ds  %%%ds  %%18s", key.length(),
            (60 - 2 - key.length()));
        System.out
            .println(String.format(format, key, val, substitutedVal));
      }
    }
    return 0;
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(NutchConfiguration.create(),
        new ShowProperties(), args));
  }

}
