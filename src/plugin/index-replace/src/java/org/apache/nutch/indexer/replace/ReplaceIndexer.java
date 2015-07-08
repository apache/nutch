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
package org.apache.nutch.indexer.replace;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.IndexingFilter;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.indexer.NutchField;
import org.apache.nutch.parse.Parse;

/**
 * Do pattern replacements on selected field contents prior to indexing.
 * 
 * To use this plugin, add <code>index-replace</code> to your
 * <code>plugin.includes</code>. Example:
 * 
 * <pre>
 *   &lt;property>
 *    &lt;name>plugin.includes&lt;/name>
 *    &lt;value>protocol-(http)|urlfilter-regex|parse-(html|tika|metatags)|index-(basic|anchor|metadata|replace)|urlnormalizer-(pass|regex|basic)|indexer-solr&lt;/value>
 *   &lt;/property>
 * </pre>
 *
 * And then add the <code>index.replace.regexp</code> property to
 * <code>conf/nutch-site.xml</code>. This contains a list of replacement
 * instructions per field name, one per line. eg.
 * 
 * <pre>
 *   fieldname=/regexp/replacement/[flags]
 * </pre>
 * 
 * <pre>
 *   &lt;property>
 *    &lt;name>index.replace.regexp&lt;/name>
 *    &lt;value>
 *      hostmatch=.*\\.com
 *      title=/search/replace/2
 *    &lt;/value>
 *   &lt;/property>
 * </pre>
 * 
 * <code>hostmatch=</code> and <code>urlmatch=</code> lines indicate the match
 * pattern for a host or url. The field replacements that follow this line will
 * apply only to pages from the matching host or url. Replacements run in the
 * order specified. Field names may appear multiple times if multiple
 * replacements are needed.
 * 
 * The property format is defined in greater detail in
 * <code>conf/nutch-default.xml</code>.
 *
 * @author Peter Ciuffetti
 * @see <a
 *      href="https://issues.apache.org/jira/browse/NUTCH-2058">NUTCH-2058</a>
 */
public class ReplaceIndexer implements IndexingFilter {

  private static final Log LOG = LogFactory.getLog(ReplaceIndexer.class
      .getName());

  /** Special field name signifying the start of a host-specific match set */
  private static final String HOSTMATCH = "hostmatch";
  /** Special field name signifying the start of a url-specific match set */
  private static final String URLMATCH = "urlmatch";

  private static Map<Pattern, List<FieldReplacer>> FIELDREPLACERS_BY_HOST = new LinkedHashMap<Pattern, List<FieldReplacer>>();
  private static Map<Pattern, List<FieldReplacer>> FIELDREPLACERS_BY_URL = new LinkedHashMap<Pattern, List<FieldReplacer>>();

  private static Pattern LINE_SPLIT = Pattern.compile("(^.+$)+",
      Pattern.MULTILINE);
  private static Pattern NAME_VALUE_SPLIT = Pattern.compile("(.*?)=(.*)");

  private Configuration conf;

  /**
   * {@inheritDoc}
   */
  public void setConf(Configuration conf) {
    this.conf = conf;
    FIELDREPLACERS_BY_HOST.clear();
    FIELDREPLACERS_BY_URL.clear();
    String value = conf.get("index.replace.regexp", null);
    if (value != null) {
      LOG.debug("Parsing index.replace.regexp property");
      this.parseConf(value);
    }
  }

  /**
   * {@inheritDoc}
   */
  public Configuration getConf() {
    return this.conf;
  }

  /**
   * Parse the property value into a set of maps that store a list of
   * replacements by field for each host and url configured into the property.
   * 
   * @param propertyValue
   */
  private void parseConf(String propertyValue) {
    if (propertyValue == null || propertyValue.trim().length() == 0) {
      return;
    }

    // At the start, all replacements apply globally to every host.
    Pattern hostPattern = Pattern.compile(".*");
    Pattern urlPattern = null;

    // Split the property into lines
    Matcher lineMatcher = LINE_SPLIT.matcher(propertyValue);
    while (lineMatcher.find()) {
      String line = lineMatcher.group();
      if (line != null && line.length() > 0) {

        // Split the line into field and value
        Matcher nameValueMatcher = NAME_VALUE_SPLIT.matcher(line.trim());
        if (nameValueMatcher.find()) {
          String fieldName = nameValueMatcher.group(1).trim();
          String value = nameValueMatcher.group(2);
          if (fieldName != null && value != null) {
            // Check if the field name is one of our special cases.
            if (HOSTMATCH.equals(fieldName)) {
              urlPattern = null;
              try {
                hostPattern = Pattern.compile(value);
              } catch (PatternSyntaxException pse) {
                LOG.error("hostmatch pattern does not compile: "
                    + pse.getMessage());
                // Deactivate this invalid match set by making it match no host.
                hostPattern = Pattern.compile("willnotmatchanyhost");
              }
            } else if (URLMATCH.equals(fieldName)) {
              try {
                urlPattern = Pattern.compile(value);
              } catch (PatternSyntaxException pse) {
                LOG.error("urlmatch pattern does not compile: "
                    + pse.getMessage());
                // Deactivate this invalid match set by making it match no url.
                urlPattern = Pattern.compile("willnotmatchanyurl");
              }
            } else if (value.length() > 3) {
              String toFieldName = fieldName;
              // If the fieldname has a colon, this indicates a different target field.
              if (fieldName.indexOf(':') > 0) {
                toFieldName = fieldName.substring(fieldName.indexOf(':') + 1);
                fieldName = fieldName.substring(0, fieldName.indexOf(':'));
              }
              String sep = value.substring(0, 1);

              // Divide the value into pattern / replacement / flags.
              value = value.substring(1);
              if (!value.contains(sep)) {
                LOG.error("Pattern '" + line
                    + "', not parseable.  Missing separator " + sep);
                continue;
              }
              String pattern = value.substring(0, value.indexOf(sep));
              value = value.substring(pattern.length() + 1);
              String replacement = value;
              if (value.contains(sep)) {
                replacement = value.substring(0, value.indexOf(sep));
              }
              int flags = 0;
              if (value.length() > replacement.length() + 1) {
                value = value.substring(replacement.length() + 1).trim();
                try {
                  flags = Integer.parseInt(value);
                } catch (NumberFormatException e) {
                  LOG.error("Pattern " + line + ", has invalid flags component");
                  continue;
                }
              }
              Integer iFlags = (flags > 0) ? new Integer(flags) : null;

              // Make a FieldReplacer out of these params.
              FieldReplacer fr = new FieldReplacer(fieldName, toFieldName, pattern,
                  replacement, iFlags);

              // Add this field replacer to the list for this host or URL.
              if (urlPattern != null) {
                List<FieldReplacer> lfp = FIELDREPLACERS_BY_URL.get(urlPattern);
                if (lfp == null) {
                  lfp = new ArrayList<FieldReplacer>();
                }
                lfp.add(fr);
                FIELDREPLACERS_BY_URL.put(urlPattern, lfp);
              } else {
                List<FieldReplacer> lfp = FIELDREPLACERS_BY_HOST
                    .get(hostPattern);
                if (lfp == null) {
                  lfp = new ArrayList<FieldReplacer>();
                }
                lfp.add(fr);
                FIELDREPLACERS_BY_HOST.put(hostPattern, lfp);
              }
            }
          }
        }
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  public NutchDocument filter(NutchDocument doc, Parse parse, Text url,
      CrawlDatum datum, Inlinks inlinks) throws IndexingException {

    if (doc != null) {
      if (FIELDREPLACERS_BY_HOST.size() > 0) {
        this.doReplace(doc, "host", FIELDREPLACERS_BY_HOST);
      }

      if (FIELDREPLACERS_BY_URL.size() > 0) {
        this.doReplace(doc, "url", FIELDREPLACERS_BY_URL);
      }
    }

    return doc;
  }

  /**
   * Iterates through the replacement map provided, to update the fields in the
   * Nutch Document.
   * 
   * @param doc
   *          the document we are modifying
   * @param keyName
   *          either "host" or "url" -- the field that determines the
   *          replacement set used
   * @param replaceMap
   *          the list of FieldReplacers that applies to this keyName.
   */
  private void doReplace(NutchDocument doc, String keyName,
      Map<Pattern, List<FieldReplacer>> replaceMap) {

    if (doc == null || replaceMap.size() == 0) {
      return;
    }

    Collection<String> docFieldNames = doc.getFieldNames();
    NutchField keyField = doc.getField(keyName);
    if (keyField == null) {
      // This document doesn't have the key field; no work to do.
      return;
    }

    List<Object> keyFieldValues = keyField.getValues();
    if (keyFieldValues.size() == 0) {
      // This document doesn't have any values for the key field; no work to do.
      return;
    }

    // For every value of the keyField (one expected)
    for (Object oKeyFieldValue : keyFieldValues) {
      if (oKeyFieldValue != null && oKeyFieldValue instanceof java.lang.String) {
        String keyFieldValue = (String) oKeyFieldValue;

        // For each pattern that we have a replacement list for...
        for (Map.Entry<Pattern, List<FieldReplacer>> entries : replaceMap
            .entrySet()) {
          // If this key is a match for a replacement set...
          if (entries.getKey().matcher(keyFieldValue).find()) {

            // For each field we will replace for this key...
            for (FieldReplacer fp : entries.getValue()) {
              String fieldName = fp.getFieldName();

              // Does this document contain the FieldReplacer's field?
              if (docFieldNames.contains(fieldName)) {
                NutchField docField = doc.getField(fieldName);
                List<Object> fieldValues = docField.getValues();
                ArrayList<String> newFieldValues = new ArrayList<String>();

                // For each value of the field, match against our
                // replacer...
                for (Object oFieldValue : fieldValues) {
                  if (oFieldValue != null
                      && oFieldValue instanceof java.lang.String) {
                    String fieldValue = (String) oFieldValue;
                    String newValue = fp.replace(fieldValue);
                    newFieldValues.add(newValue);
                  }
                }

                // Remove the target field and add our replaced values.
                String targetFieldName = fp.getToFieldName();
                doc.removeField(targetFieldName);
                for (String newFieldValue : newFieldValues) {
                  doc.add(targetFieldName, newFieldValue);
                }
              }
            }
          }
        }
      }
    }
  }
}
