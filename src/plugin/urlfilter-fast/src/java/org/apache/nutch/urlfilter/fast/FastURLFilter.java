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

import java.lang.invoke.MethodHandles;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.net.URL;
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
 * <code>Host</code> rules the entire host name of a URL must match while the
 * domain names in <code>Domain</code> rules are considered as matches if the
 * domain is a suffix of the host name (consisting of complete host name parts).
 * Shorter domain suffixes are checked first, a single dot
 * &quot;<code>.</code>&quot; as &quot;domain name&quot; can be used to specify
 * global rules applied to every URL.
 * 
 * E.g., for "www.example.com" the rules given above are looked up in the
 * following order:
 * <ol>
 * <li>check "www.example.com" whether host-based rules exist and whether one of
 * them matches</li>
 * <li>check "www.example.com" for domain-based rules</li>
 * <li>check "example.com" for domain-based rules</li>
 * <li>check "com" for domain-based rules</li>
 * <li>check for global rules (&quot;<code>Domain .</code>&quot;)</li>
 * </ol>
 * The first matching rule will reject the URL and no further rules are checked.
 * If no rule matches the URL is accepted. URLs without a host name (e.g.,
 * <code>file:/path/file.txt</code> are checked for global rules only. URLs
 * which fail to be parsed as {@link java.net.URL} are always rejected.
 * 
 * For rules either the URL path (<code>DenyPath</code>) or path and query
 * (<code>DenyPathQuery</code>) are checked whether the given
 * {@link java.util.regex Java Regular expression} is found (see
 * {@link java.util.regex.Matcher#find()}) in the URL path (and query).
 * 
 * Rules are applied in the order of their definition. For better performance,
 * regular expressions which are simpler/faster or match more URLs should be
 * defined earlier.
 * 
 * Comments in the rule file start with the <code>#</code> character and reach
 * until the end of the line.
 * 
 * The rules file is defined via the property <code>urlfilter.fast.file</code>,
 * the default name is <code>fast-urlfilter.txt</code>.
 */
public class FastURLFilter implements URLFilter {

  protected static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  private Configuration conf;
  public static final String URLFILTER_FAST_FILE = "urlfilter.fast.file";
  private Multimap<String, Rule> hostRules = LinkedHashMultimap.create();
  private Multimap<String, Rule> domainRules = LinkedHashMultimap.create();

  private static final Pattern CATCH_ALL_RULE = Pattern
      .compile("^\\s*DenyPath(?:Query)?\\s+\\.[*?]\\s*$");

  public FastURLFilter() {}

  FastURLFilter(Reader rules) throws IOException, PatternSyntaxException {
    reloadRules(rules);
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    try {
      reloadRules();
    } catch (Exception e) {
      LOG.error(e.getMessage());
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  @Override
  public Configuration getConf() {
    return this.conf;
  }

  @Override
  public String filter(String url) {

    URL u;

    try {
      u = new URL(url);
    } catch (Exception e) {
      LOG.debug("Rejected {} because failed to parse as URL: {}", url,
          e.getMessage());
      return null;
    }

    String hostname = u.getHost();

    // first check for host-specific rules
    for (Rule rule : hostRules.get(hostname)) {
      if (rule.match(u)) {
        return null;
      }
    }

    // also look up domain rules for host name
    for (Rule rule : domainRules.get(hostname)) {
      if (rule.match(u)) {
        return null;
      }
    }

    // check suffixes of host name from longer to shorter:
    // subdomains, domain, top-level domain
    int start = 0;
    int pos;
    while ((pos = hostname.indexOf('.', start)) != -1) {
      start = pos + 1;
      String domain = hostname.substring(start);
      for (Rule rule : domainRules.get(domain)) {
        if (rule.match(u)) {
          return null;
        }
      }
    }

    // finally check "global" rules defined for `Domain .`
    for (Rule rule : domainRules.get(".")) {
      if (rule.match(u)) {
        return null;
      }
    }

    // no reject rules found
    return url;
  }

  public void reloadRules() throws IOException {
    String fileRules = conf.get(URLFILTER_FAST_FILE);
    try (Reader reader = conf.getConfResourceAsReader(fileRules)) {
      reloadRules(reader);
    }
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
            if (CATCH_ALL_RULE.matcher(line).matches()) {
              rule = DenyAllRule.getInstance();
            } else if (line.startsWith("DenyPathQuery")) {
              rule = new DenyPathQueryRule(line.split("\\s+")[1]);
            } else if (line.startsWith("DenyPath")) {
                rule = new DenyPathRule(line.split("\\s+")[1]);
            } else {
              LOG.warn("Problem reading rule on line {}: {}", lineno, line);
              continue;
            }
          } catch (Exception e) {
            LOG.warn("Problem reading rule on line {}: {} - {}", lineno, line, e.getMessage());
            continue;
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
      LOG.warn("Caught exception while reading rules file at line {}: {}",
          lineno, e.getMessage());
      throw e;
    }
  }

  public static class Rule {
    protected Pattern pattern;

    Rule() {}

    public Rule(String regex) {
      pattern = Pattern.compile(regex);
    }

    public boolean match(URL url) {
      return pattern.matcher(url.toString()).find();
    }

    public String toString() {
       return pattern.toString();
    }
  }

  public static class DenyPathRule extends Rule {
    public DenyPathRule(String regex) {
      super(regex);
    }

    public boolean match(URL url) {
      String haystack = url.getPath();
      return pattern.matcher(haystack).find();
    }
  }

  /** Rule for <code>DenyPath .*</code> or <code>DenyPath .?</code> */
  public static class DenyAllRule extends Rule {

    private static Rule instance = new DenyAllRule(".");

    private DenyAllRule(String regex) {
      super(regex);
    }

    public static Rule getInstance() {
      return instance;
    }

    public boolean match(URL url) {
      return true;
    }
  }

  public static class DenyPathQueryRule extends Rule {
    public DenyPathQueryRule(String regex) {
      super(regex);
    }

    public boolean match(URL url) {
      String haystack = url.getFile();
      return pattern.matcher(haystack).find();
    }
  }
}
