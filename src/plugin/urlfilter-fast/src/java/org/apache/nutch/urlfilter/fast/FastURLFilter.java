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
import java.io.Reader;
import java.net.URI;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Filters URLs based on a file of regular expressions using host/domains
 * matching first. The default policy is to accept a URL if no matches are
 * found.
 *
 * Rule Format:
 * 
 * <pre>
 * Host www.example.org
 *   DenyPath /path/to/be/excluded
 *   DenyPath /some/other/path/excluded
 *
 * # Deny everything from *.example.com and example.com
 * Domain example.com
 *   DenyPath .*
 *
 * Domain example.org
 *   DenyPathQuery /resource/.*?action=exclude
 * </pre>
 * 
 * <code>Host</code> rules are evaluated before <code>Domain</code> rules. For
 * <code>Host</code> rules the entire host name of a URL must match while domain
 * names are considered as matches if the domain is a suffix of the host name
 * (consisting of complete host name parts). Shorter domain suffixes are checked
 * first, a single dot &quot;<code>.</code>&quot; as &quot;domain name&quot; can
 * be used to specify global rules applied to every URL.
 * 
 * E.g., for "www.example.com" rules are looked up in the following order:
 * <ol>
 * <li>check "www.example.com" whether host-based rules exist and whether one of
 * them matches</li>
 * <li>check "www.example.com" for domain-based rules</li>
 * <li>check "example.com" for domain-based rules</li>
 * <li>check "com" for domain-based rules</li>
 * <li>check for global rules (domain name is ".")</li>
 * </ol>
 * 
 * For rules either the URL path (<code>DenyPath</code>) or path and query
 * (<code>DenyPathQuery</code>) are checked whether one of the given
 * {@link java.util.regex Java Regular expression} matches the beginning of the
 * URL path (and query).
 * 
 * Rules are applied in the order of their definition. For better performance,
 * regular expressions which are simpler or match more URLs should be defined
 * earlier.
 * 
 * Comments in the rule file start with the <code>#</code> character and reach
 * until the end of the line.
 */
public class FastURLFilter implements URLFilter {
  private final static Logger LOG = LoggerFactory.getLogger(FastURLFilter.class);
  private Configuration conf;
  public static final String URLFILTER_FAST_FILE = "urlfilter.fast.file";
  private Multimap<String, Rule> hostRules = LinkedHashMultimap.create();
  private Multimap<String, Rule> domainRules = LinkedHashMultimap.create();

  private static final Pattern CATCH_ALL_RULE = Pattern
      .compile("^\\s*DenyPath\\s+\\.\\*\\s*$");

  public FastURLFilter() {}

  FastURLFilter(Reader rules) throws IOException, PatternSyntaxException {
    reloadRules(rules);
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
    try {
      reloadRules();
    } catch (Exception e) {
      LOG.error(e.getMessage());
      throw new RuntimeException(e.getMessage(), e);
    }
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

      int start = 0;
      int pos;
      while ((pos = hostname.indexOf('.', start)) != -1) {
        start = pos + 1;
        String domain = hostname.substring(start);
        for (Rule rule : domainRules.get(domain)) {
          if (rule.match(uri)) {
            return null;
          }
        }
      }

      // Finally match the Domain '.' which allows us to build simple global rules
      for (Rule rule : domainRules.get(".")) {
        if (rule.match(uri)) {
          return null;
        }
      }
    } catch (Exception e) {
      return null;
    }


    return url;
  }

  public void reloadRules() throws IOException {
    String fileRules = conf.get(URLFILTER_FAST_FILE);
    reloadRules(conf.getConfResourceAsReader(fileRules));
  }

  private void reloadRules(Reader rules) throws IOException {
    domainRules.clear();
    hostRules.clear();

    BufferedReader reader = new BufferedReader(rules);

    String current = null;
    boolean host = false;
    int lineno = 0;

    String line;
    try {
      while((line = reader.readLine()) != null) {
        lineno++;
        line = line.trim();

        if (line.indexOf("#") != -1) {
          // strip comments
          line = line.substring(0, line.indexOf("#")).trim();
        }

        if (StringUtils.isBlank(line)) {
          continue;
        }

        if (line.startsWith("Host")) {
          host = true;
          current =  line.split("\\s+")[1];
        } else if (line.startsWith("Domain")) {
          host = false;
          current = line.split("\\s+")[1];
        } else {
          if (current == null) {
            continue;
          }

          Rule rule = null;
          try {
            if (line.startsWith("DenyPathQuery")) {
              rule = new DenyPathQueryRule(line.split("\\s+")[1]);
            } else if (line.startsWith("DenyPath")) {
              if (CATCH_ALL_RULE.matcher(line).matches()) {
                rule = DenyPathEntirelyRule.getInstance();
              } else {
                rule = new DenyPathRule(line.split("\\s+")[1]);
              }
            } else {
              continue;
            }
          } catch (IndexOutOfBoundsException e) {
            LOG.warn("Problem reading rule on line " + lineno + ": " + line);
          }

          if (host) {
            LOG.trace("Adding host rule [{}] [{}]", current, rule);
            hostRules.put(current, rule);
          } else {
            LOG.trace("Adding domain rule [{}] [{}]", current, rule);
            domainRules.put(current, rule);
          }
        }
      }
    } catch (IOException e) {
      LOG.warn("Caught exception while reading rules file at line " + lineno);
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

  /** Rule for &quot;DenyPath .*&quot; */
  public static class DenyPathEntirelyRule extends Rule {

    private static Rule instance = new DenyPathEntirelyRule(".*");

    private DenyPathEntirelyRule(String regex) {
      super(regex);
    }

    public static Rule getInstance() {
      return instance;
    }

    public boolean match(URI uri) {
      return true;
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
