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

package org.apache.nutch.parse.xsl;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;

import javax.xml.bind.JAXB;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamSource;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.nutch.parse.xsl.xml.rule.Rules;
import org.apache.nutch.parse.xsl.xml.rule.TRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manage a set of Transformers. It allows to avoid having several instances of
 * Transformers with XSL to load each time for performance matter. The decision
 * to use a given Transformer is determined by a set DO NOT make this class a
 * singleton otherwise it will produce thread safety problems related to Xsl
 * transformers not thread safe.
 * 
 * @see Transformer
 * 
 */
public class RulesManager {

  /** All the rules used to determine which xsl parser to use */
  protected Rules rules = null;

  /**
   * Transformer factory. Thread-local because {@link TransformerFactory}
   * "is NOT guaranteed to be thread safe".
   */
  protected ThreadLocal<TransformerFactory> factory = new ThreadLocal<TransformerFactory>() {
    @Override
    protected TransformerFactory initialValue() {
      return TransformerFactory.newInstance();
    }
  };

  /** A RuleTransformer holds transformations defined in one XSLT file. */
  protected class RuleTransformer {

    String xslFile;
    byte[] xslSource;
    ThreadLocal<Transformer> transformer = new ThreadLocal<Transformer>() {
      @Override
      protected Transformer initialValue() {
        ByteArrayInputStream input = new ByteArrayInputStream(xslSource);
        StreamSource streamSource = new StreamSource(input);
        Transformer t = null;
        try {
          t = factory.get().newTransformer(streamSource);
        } catch (TransformerConfigurationException e) {
          LOG.warn("Failed to create transformer for xsl file {}: {}", xslFile,
              StringUtils.stringifyException(e));
        }
        return t;
      }
    };

    public RuleTransformer(Configuration conf, String xslFile)
        throws IOException {
      this.xslFile = xslFile;
      InputStream stream = conf.getConfResourceAsInputStream(xslFile);
      xslSource = IOUtils.toByteArray(stream);
    }

    public Transformer getTransformer() {
      return transformer.get();
    }

  }

  /** A map containing all transformers given their file name as key */
  protected Map<String, RuleTransformer> transformers = new HashMap<String, RuleTransformer>();

  /** The XSLT file to use for transformation */
  public static final String CONF_XML_RULES = "parser.xsl.rulesFile";

  private static final Logger LOG = LoggerFactory.getLogger(RulesManager.class);

  /**
   * Default constructor forbidden.
   */
  @SuppressWarnings("unused")
  private RulesManager() {
  }

  /**
   * Instantiates an object using the Nutch/Hadoop {@link Configuration}
   * containing the property defining the rules. All rules and transformation
   * files are load from the class path.
   * 
   * @param conf
   *          configuration
   */
  public RulesManager(Configuration conf) {

    String rulesFile = conf.get(RulesManager.CONF_XML_RULES);
    if (rulesFile != null) {
      Reader rulesXmlReader = conf.getConfResourceAsReader(rulesFile);

      if (rulesXmlReader != null) {
        LOG.debug("Reading parse-xsl rules file `{}'", rulesFile);
        rules = JAXB.unmarshal(rulesXmlReader, Rules.class);

        // load transformation files
        for (TRule rule : rules.getRule()) {
          final String xslFile = rule.getTransformer().getFile();

          if (xslFile != null) {
            LOG.debug("Reading parse-xsl transformation file `{}'", xslFile);
            try {
              RuleTransformer rt = new RuleTransformer(conf, xslFile);
              transformers.put(xslFile, rt);
            } catch (IOException e) {
              LOG.error("Failed to read parse-xsl transformation file {}: {}",
                  xslFile, StringUtils.stringifyException(e));
            }
          }
        }

      } else {
        LOG.error(
            "Failed to open parse-xsl rules file `{}' defined by property {}",
            rulesFile, RulesManager.CONF_XML_RULES);
        LOG.error(System.getProperty("java.class.path"));
      }

    } else {
      LOG.warn("Plugin parse-xsl active but no rules file defined!");
    }
  }

  /**
   * Match URL against regular expressions to assign it to a transformer file.
   * 
   * @param url
   *          the URL to filter
   * @return the transformer file path that matches the rules or null if no rule
   *         does match
   */
  public String getTransformerFilePath(String url) {

    String xslFile = null;

    if (rules == null) {
      // no rules defined
      return xslFile;
    }

    // Search for a matching rule by applying defined regex
    // The first matching rule will be applied
    for (TRule rule : rules.getRule()) {
      if (url.matches(rule.getMatches())) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(String.format("Url %s is matching regex rule %s", url,
              rule.getMatches()));
        }
        xslFile = rule.getTransformer().getFile();

        break;
      }
    }
    if (xslFile == null) {
      LOG.debug("No filter found for url: {}", url);
    }

    return xslFile;
  }

  /**
   * Get the first transformer matching a URL.
   * 
   * @param url
   *          the url to filter
   * @return the transformer that suits the rules
   * @throws Exception
   */
  public Transformer getTransformer(String url) {
    Transformer transformer = null;
    String xslFile = getTransformerFilePath(url);
    if (xslFile != null) {
      return transformers.get(xslFile).getTransformer();
    }
    return transformer;
  }

  /**
   * Check whether a URL matches any rule.
   * 
   * @param url
   *          the URL to test match in rules file
   * @return true if the URL is matching any rule.
   * @throws Exception
   */
  public boolean matches(String url) throws Exception {
    return this.getTransformerFilePath(url) != null;
  }

  /**
   * @return the current set of rules defined in the xml file
   */
  public Rules getRules() {
    return rules;
  }

}
