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
package org.apache.nutch.urlfilter.fast;

import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.net.URLFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.URI;
import java.util.regex.Pattern;

/**
 * Filters URLs based on a file of regular expressions and host/domains using the
 * {@link java.util.regex Java Regex implementation}.
 */
public class FastURLFilter implements URLFilter {
  private final static Logger LOG = LoggerFactory.getLogger(FastURLFilter.class);
  private Configuration conf;
  public static final String URLFILTER_FAST_FILE = "urlfilter.fast.file";
  private Multimap<String, Rule> hostRules = LinkedHashMultimap.create();
  private Multimap<String, Rule> domainRules = LinkedHashMultimap.create();
  int cnt = 0;

  public FastURLFilter() {}

  public void setConf(Configuration conf) {
    this.conf = conf;
    reloadRules();
  }

  public Configuration getConf() {
    return this.conf;
  }

  public String filter(String url) {
    try {
      URI uri = new URI(url);
      String hostname = uri.getHost();

      for (Rule rule : hostRules.get(hostname)) {
        if (rule.match(uri)) {
          return null;
        }
      }

      String[] domainParts = hostname.split("\\.");
      String domain = null;
      for (int i=domainParts.length - 1; i >= 0; i--) {
        domain = domainParts[i] + (domain == null ? "" : "." + domain);

        for (Rule rule : domainRules.get(domain)) {
          if (rule.match(uri)) {
            return null;
          }
        }
      }
    } catch (Exception e) {
      return null;
    }


    return url;
  }

  /* Format:
     Host www.imdb.com
         DenyPath /keyword/(?:.*?/){2,}  # permutation of keywords separated by /
         DenyPath /some/other/path

     Domain gravatar.com
        DenyPath .*     # Deny everything from *.gravatar.com and gravatar.com

     Domain myspace.com
        DenyPathQuery /resource/.*?action=add
   */
  public void reloadRules() {
    domainRules.clear();
    hostRules.clear();

    String fileRules = conf.get(URLFILTER_FAST_FILE);
    BufferedReader reader = new BufferedReader(conf.getConfResourceAsReader(fileRules));

    String current = null;
    boolean host = false;

    String line;
    try {
      while((line = reader.readLine()) != null) {
        line = line.trim();

        if (line.indexOf("#") != -1) {
          line = line.substring(0, line.indexOf("#")).trim();
        }

        if (StringUtils.isBlank(line)) {
          continue;
        }

        if (line.startsWith("Host ")) {
          host = true;
          current =  line.split("\\s")[1];
        } else if (line.startsWith("Domain ")) {
          host = false;
          current = line.split("\\s")[1];
        } else {
          if (current == null) {
            continue;
          }

          if (line.startsWith("DenyPath ")) {
            String rule = line.split("\\s")[1];

            if (host) {
              hostRules.put(current, new DenyPathRule(rule));
            } else {
              domainRules.put(current, new DenyPathRule(rule));
            }
          } else if (line.startsWith("DenyPathQuery ")) {
            String rule = line.split("\\s")[1];

            if (host) {
              hostRules.put(current, new DenyPathQueryRule(rule));
            } else {
              domainRules.put(current, new DenyPathQueryRule(rule));
            }
          }
        }
      }
    } catch (IOException e) {
      LOG.warn("Caught exception while reading rules file");
    }
  }


  public static class Rule {
    protected Pattern pattern;

    Rule() {}

    public Rule(String regex) {
      pattern = Pattern.compile(regex);
    }

    public boolean match(URI uri) {
      return pattern.matcher(uri.toString()).find();
    }

    public String toString() {
       return pattern.toString();
    }
  }

  public static class DenyPathRule extends Rule {
    public DenyPathRule(String regex) {
      super(regex);
    }

    public boolean match(URI uri) {
      String haystack = uri.getRawPath();
      if (haystack == null) {
        haystack = "";
      }

      return pattern.matcher(haystack).find();
    }
  }

  public static class DenyPathQueryRule extends Rule {
    public DenyPathQueryRule(String regex) {
      super(regex);
    }

    public boolean match(URI uri) {
      String haystack = uri.getRawPath();
      if (haystack == null) {
        haystack = "";
      }

      String query = uri.getRawQuery();
      if (query != null) {
        haystack += "?" + query;
      }

      return pattern.matcher(haystack).find();
    }
  }
}