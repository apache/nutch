/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import org.apache.nutch.protocol.Content;

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
  private Configuration conf;
  private ParserFactory parserFactory;
  
  /**
   * 
   * @param conf
   */
  public ParseUtil(Configuration conf) {
    this.conf = conf;
    this.parserFactory = new ParserFactory(conf);
  }
  
  /**
   * Performs a parse by iterating through a List of preferred {@link Parser}s
   * until a successful parse is performed and a {@link Parse} object is
   * returned. If the parse is unsuccessful, a message is logged to the
   * <code>WARNING</code> level, and an empty parse is returned.
   *
   * @param content The content to try and parse.
   * @return A {@link Parse} object containing the parsed data.
   * @throws ParseException If no suitable parser is found to perform the parse.
   */
  public Parse parse(Content content) throws ParseException {
    Parser[] parsers = null;
    
    try {
      parsers = this.parserFactory.getParsers(content.getContentType(), "");
    } catch (ParserNotFound e) {
      LOG.warn("No suitable parser found when trying to parse content " +
               content);
      throw new ParseException(e.getMessage());
    }
    
    Parse parse = null;
    for (int i=0; i<parsers.length; i++) {
      LOG.debug("Parsing [" + content.getUrl() + "] with [" + parsers[i] + "]");
      parse = parsers[i].getParse(content);
      if ((parse != null) && (parse.getData().getStatus().isSuccess())) {
        return parse;
      }
    }
    
    LOG.warn("Unable to successfully parse content " + content.getUrl() +
             " of type " + content.getContentType());

    ParseStatus ps = (parse.getData() != null) ? parse.getData().getStatus() : null;
    return (ps == null) ? new ParseStatus().getEmptyParse(this.conf)
                        : ps.getEmptyParse(this.conf);
  }
    
  /**
   * Method parses a {@link Content} object using the {@link Parser} specified
   * by the parameter <code>extId</code>, i.e., the Parser's extension ID.
   * If a suitable {@link Parser} is not found, then a <code>WARNING</code>
   * level message is logged, and a ParseException is thrown. If the parse is
   * uncessful for any other reason, then a <code>WARNING</code> level
   * message is logged, and a <code>ParseStatus.getEmptyParse()</code> is
   * returned.
   *
   * @param extId The extension implementation ID of the {@link Parser} to use
   *              to parse the specified content.
   * @param content The content to parse.
   *
   * @return A {@link Parse} object if the parse is successful, otherwise,
   *         a <code>ParseStatus.getEmptyParse()</code>.
   *
   * @throws ParseException If there is no suitable {@link Parser} found
   *                        to perform the parse.
   */
  public Parse parseByExtensionId(String extId, Content content)
  throws ParseException {
    Parse parse = null;
    Parser p = null;
    
    try {
      p = this.parserFactory.getParserById(extId);
    } catch (ParserNotFound e) {
      LOG.warn("No suitable parser found when trying to parse content " +
               content);
      throw new ParseException(e.getMessage());
    }
    
    parse = p.getParse(content);
    
    if (parse != null && parse.getData().getStatus().isSuccess()) {
      return parse;
    } else {
      LOG.warn("Unable to successfully parse content " + content.getUrl() +
               " of type " + content.getContentType());
      return new ParseStatus().getEmptyParse(this.conf);
    }
  }  
  
}
