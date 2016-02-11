/*
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
package org.apache.nutch.urlfilter.ignoreexempt;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.net.URLExemptionFilter;
import org.apache.nutch.util.NutchConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.regex.Pattern;
import java.util.List;
import java.util.ArrayList;


/**
 * This implementation of {@link org.apache.nutch.net.URLExemptionFilter} uses regex configuration
 * to check if URL is eligible for exemption from 'db.ignore.external'.
 * When this filter is enabled, urls will be checked against configured sequence of regex rules.
 *<p>
 * The exemption rule file defaults to db-ignore-external-exemptions.txt in the classpath but can be
 * overridden using the property  <code>"db.ignore.external.exemptions.file" in ./conf/nutch-*.xml</code>
 *</p>
 *
 * The exemption rules are specified in plain text file where each line is a rule.
 *
 * When the url matches regex it is exempted from 'db.ignore.external...'<br/>
 * <h3>Examples:</h3>
 * <ol>
 *   <li>
 *     <b>Exempt urls ending with .jpg or .png or gif</b>
 *      <br/><code>.*\.(jpg|JPG|png$|PNG|gif|GIF)$</code><br/><br/>
 *   </li>
 * </ol>
 </pre>
 *
 * @since Feb 10, 2016
 * @version 1
 * @see URLExemptionFilter
 */
public class ExemptionUrlFilter implements URLExemptionFilter {

  public static final String DB_IGNORE_EXTERNAL_EXEMPTIONS_FILE = "db.ignore.external.exemptions.file";
  private static final Logger LOG = LoggerFactory.getLogger(ExemptionUrlFilter.class);
  private static ExemptionUrlFilter INSTANCE;

  private List<Pattern> exemptions;
  private Configuration conf;
  private boolean enabled;

  public static ExemptionUrlFilter getInstance() {
    if(INSTANCE == null) {
      synchronized (ExemptionUrlFilter.class) {
        if (INSTANCE == null) {
          INSTANCE = new ExemptionUrlFilter();
          INSTANCE.setConf(NutchConfiguration.create());
        }
      }
    }
    return INSTANCE;
  }

  public boolean isEnabled() {
    return enabled;
  }

  public List<Pattern> getExemptions() {
    return exemptions;
  }

  @Override
  public boolean filter(String fromUrl, String toUrl) {
    //this implementation doesnt do anything with fromUrl
    if (exemptions != null) {
      for (Pattern pattern : exemptions) {
        if (pattern.matcher(toUrl).matches()) {
          return true; //If a regex matches, then exempted
        }
      }
    }
    //not exempted
    return false;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    LOG.info("Ignore exemptions enabled");
    String fileName = this.conf.get(DB_IGNORE_EXTERNAL_EXEMPTIONS_FILE);
    InputStream stream = this.conf.getConfResourceAsInputStream(fileName);
    if (stream == null) {
      throw new RuntimeException("Couldn't find config file :" + fileName);
    }
    try {
      this.exemptions = new ArrayList<Pattern>();
      List<String> lines = IOUtils.readLines(stream);
      for (String line : lines) {
        line = line.trim();
        if (line.startsWith("#") || line.isEmpty()) {
          continue; //Skip : comment line or empty line
        }
        Pattern compiled = Pattern.compile(line);
        LOG.info("Regex :: {} ", line);
        exemptions.add(compiled);
      }
      LOG.info("Read {} rules from {}", exemptions.size(), fileName);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public Configuration getConf() {
    return this.conf;
  }

  public static void main(String[] args) {
    if (args.length != 1) {
      System.out.println("Error: Invalid Args");
      System.out.println("Usage: " + ExemptionUrlFilter.class.getName() + " <url>");
      return;
    }
    String url = args[0];
    System.out.println(ExemptionUrlFilter.getInstance().filter(null, url));
  }
}
