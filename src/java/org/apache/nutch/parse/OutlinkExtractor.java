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
package org.apache.nutch.parse;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;

/**
 * Extractor to extract {@link org.apache.nutch.parse.Outlink}s / URLs from
 * plain text using Regular Expressions.
 * 
 * @see <a
 *      href="http://wiki.java.net/bin/view/Javapedia/RegularExpressions">Comparison
 *      of different regexp-Implementations </a>
 * @see <a href="http://regex.info/java.html">Overview about Java Regexp APIs
 *      </a>
 * 
 * @author Stephan Strittmatter - http://www.sybit.de
 *
 * @since 0.7
 */
public class OutlinkExtractor {
  private static final Log LOG = LogFactory.getLog(OutlinkExtractor.class);

  private static final Outlink[] NO_LINKS = new Outlink[0];

  /**
   * Regex pattern to get URLs within a plain text.
   * 
   * @see <a
   *      href="http://www.truerwords.net/articles/ut/urlactivation.html">http://www.truerwords.net/articles/ut/urlactivation.html
   *      </a>
   */
  private static final String URL_PATTERN = "([A-Za-z][A-Za-z0-9+.-]{1,120}:[A-Za-z0-9/](([A-Za-z0-9$_.+!*,;/?:@&~=-])|%[A-Fa-f0-9]{2}){1,333}(#([a-zA-Z0-9][a-zA-Z0-9$_.+!*,;/?:@&~=%-]{0,1000}))?)";

  static final Pattern urlPattern = Pattern.compile(URL_PATTERN);

  /**
   * Extracts outlinks from a plain text.
   * </p>
   * @param plainText
   * 
   * @return Array of <code>Outlink</code> s within found in plainText
   */
  public static Outlink[] getOutlinks(final String plainText, Configuration conf){
    return getOutlinks(plainText, null, conf);
  }

  
  /**
   * Extracts outlinks from a plain text.
   * </p>
   * @param plainText text to extract urls from
   * 
   * @return Array of <code>Outlink</code> s found in plainText
   */
  public static Outlink[] getOutlinks(final String plainText, String anchor,
      Configuration conf) {
    
    if(plainText == null){
      return NO_LINKS;
    }

    final ArrayList<Outlink> outlinks = new ArrayList<Outlink>();
    Outlink[] retval;
    Outlink link;

    Matcher m = urlPattern.matcher(plainText);
    while (m.find()) {

      try {
        link = new Outlink(m.toMatchResult().group(), anchor, conf);
        outlinks.add(link);
      } catch (MalformedURLException ex) {
        // if it is a malformed URL we just throw it away and continue with
        // extraction.
        if (LOG.isDebugEnabled()) {
          LOG.debug("extracted malformed url:" + m.toMatchResult().group(), ex);
        }
      }

    }

    if (outlinks.size() > 0) {
      retval = outlinks.toArray(new Outlink[outlinks.size()]);
    } else {
      retval = NO_LINKS;
    }

    return retval;
  }

}
