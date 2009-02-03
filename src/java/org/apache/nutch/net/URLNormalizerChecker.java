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

import org.apache.nutch.plugin.Extension;
import org.apache.nutch.plugin.ExtensionPoint;
import org.apache.nutch.plugin.PluginRepository;

import org.apache.hadoop.conf.Configuration;

import org.apache.nutch.util.NutchConfiguration;

import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * Checks one given normalizer or all normalizers.
 */
public class URLNormalizerChecker {

  private Configuration conf;

  public URLNormalizerChecker(Configuration conf) {
      this.conf = conf;
  }

  private void checkOne(String normalizerName, String scope) throws Exception {
    URLNormalizer normalizer = null;

    ExtensionPoint point =
      PluginRepository.get(conf).getExtensionPoint(URLNormalizer.X_POINT_ID);

    if (point == null)
      throw new RuntimeException(URLNormalizer.X_POINT_ID+" not found.");

    Extension[] extensions = point.getExtensions();

    for (int i = 0; i < extensions.length; i++) {
      Extension extension = extensions[i];
      normalizer = (URLNormalizer)extension.getExtensionInstance();
      if (normalizer.getClass().getName().equals(normalizerName)) {
        break;
      } else {
        normalizer = null;
      }
    }

    if (normalizer == null)
      throw new RuntimeException("URLNormalizer "+normalizerName+" not found.");

    System.out.println("Checking URLNormalizer " + normalizerName);

    BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
    String line;
    while ((line = in.readLine()) != null) {
      String out = normalizer.normalize(line, scope);
      System.out.println(out);
    }
  }

  private void checkAll(String scope) throws Exception {
    System.out.println("Checking combination of all URLNormalizers available");

    BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
    String line;
    URLNormalizers normalizers = new URLNormalizers(conf, scope);
    while((line = in.readLine()) != null) {
      String out = normalizers.normalize(line, scope);
      System.out.println(out);
    }
  }

  public static void main(String[] args) throws Exception {

    String usage = "Usage: URLNormalizerChecker [-normalizer <normalizerName>] [-scope <scope>]"
      + "\n\tscope can be one of: default,partition,generate_host_count,fetcher,crawldb,linkdb,inject,outlink";

    String normalizerName = null;
    String scope = URLNormalizers.SCOPE_DEFAULT;
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-normalizer")) {
        normalizerName = args[++i];
      } else if (args[i].equals("-scope")) {
        scope = args[++i];
      } else {
        System.err.println(usage);
        System.exit(-1);
      }
    }

    URLNormalizerChecker checker = new URLNormalizerChecker(NutchConfiguration.create());
    if (normalizerName != null) {
      checker.checkOne(normalizerName, scope);
    } else {
      checker.checkAll(scope);
    }

    System.exit(0);
  }
}
