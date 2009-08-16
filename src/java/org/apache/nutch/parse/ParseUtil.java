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
package org.apache.nutch.parse;

// Commons Logging imports
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

// Nutch Imports
import org.apache.nutch.parse.ParseException;
import org.apache.nutch.parse.ParserNotFound;
import org.apache.nutch.util.hbase.WebTableRow;

// Hadoop imports
import org.apache.hadoop.conf.Configuration;

/**
 * A Utility class containing methods to simply perform parsing utilities such
 * as iterating through a preferred list of {@link Parser}s to obtain
 * {@link Parse} objects.
 *
 * @author mattmann
 * @author J&eacute;r&ocirc;me Charron
 * @author S&eacute;bastien Le Callonnec
 */
public class ParseUtil {
  
  /* our log stream */
  public static final Log LOG = LogFactory.getLog(ParseUtil.class);
  private ParserFactory parserFactory;
  
  /**
   * 
   * @param conf
   */
  public ParseUtil(Configuration conf) {
    this.parserFactory = new ParserFactory(conf);
  }
  
  /**
   * Performs a parse by iterating through a List of preferred {@link Parser}s
   * until a successful parse is performed and a {@link Parse} object is
   * returned. If the parse is unsuccessful, a message is logged to the
   * <code>WARNING</code> level, and an empty parse is returned.
   *
   * @throws ParseException If no suitable parser is found to perform the parse.
   */
  public Parse parse(String url, WebTableRow row) throws ParseException {
    Parser[] parsers = null;
    
    String contentType = row.getContentType();
    
    try {
      parsers = this.parserFactory.getParsers(contentType, url);
    } catch (ParserNotFound e) {
      if (LOG.isWarnEnabled()) {
        LOG.warn("No suitable parser found when trying to parse content " + url +
               " of type " + contentType);
      }
      throw new ParseException(e.getMessage());
    }
    
    Parse parse = null;
    for (int i=0; i<parsers.length; i++) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Parsing [" + url + "] with [" + parsers[i] + "]");
      }
      parse = parsers[i].getParse(url, row);
      if (parse != null)
        return parse;
    }
   
    if (LOG.isWarnEnabled()) { 
      LOG.warn("Unable to successfully parse content " + url +
               " of type " + contentType);
    }
    return null;
  }
}
