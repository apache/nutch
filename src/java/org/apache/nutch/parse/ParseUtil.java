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

import java.lang.invoke.MethodHandles;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.protocol.Content;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

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
  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());
  private ParserFactory parserFactory;
  /** Parser timeout set to 30 sec by default. Set -1 to deactivate **/
  private int maxParseTime = 30;
  private ExecutorService executorService;

  /**
   * 
   * @param conf
   */
  public ParseUtil(Configuration conf) {
    this.parserFactory = new ParserFactory(conf);
    maxParseTime = conf.getInt("parser.timeout", 30);
    executorService = Executors.newCachedThreadPool(new ThreadFactoryBuilder()
        .setNameFormat("parse-%d").setDaemon(true).build());
  }

  /**
   * Performs a parse by iterating through a List of preferred {@link Parser}s
   * until a successful parse is performed and a {@link Parse} object is
   * returned. If the parse is unsuccessful, a message is logged to the
   * <code>WARNING</code> level, and an empty parse is returned.
   * 
   * @param content
   *          The content to try and parse.
   * @return &lt;key, {@link Parse}&gt; pairs.
   * @throws ParseException
   *           If no suitable parser is found to perform the parse.
   */
  public ParseResult parse(Content content) throws ParseException {
    Parser[] parsers = null;

    try {
      parsers = this.parserFactory.getParsers(content.getContentType(),
          content.getUrl() != null ? content.getUrl() : "");
    } catch (ParserNotFound e) {
      if (LOG.isWarnEnabled()) {
        LOG.warn("No suitable parser found when trying to parse content "
            + content.getUrl() + " of type " + content.getContentType());
      }
      throw new ParseException(e.getMessage());
    }

    ParseResult parseResult = null;
    for (int i = 0; i < parsers.length; i++) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Parsing [" + content.getUrl() + "] with [" + parsers[i]
            + "]");
      }
      if (maxParseTime != -1)
        parseResult = runParser(parsers[i], content);
      else
        parseResult = parsers[i].getParse(content);

      if (parseResult != null && !parseResult.isEmpty())
        return parseResult;
    }

    if (LOG.isWarnEnabled()) {
      LOG.warn("Unable to successfully parse content " + content.getUrl()
          + " of type " + content.getContentType());
    }
    return new ParseStatus(new ParseException(
        "Unable to successfully parse content")).getEmptyParseResult(
        content.getUrl(), null);
  }

  /**
   * Method parses a {@link Content} object using the {@link Parser} specified
   * by the parameter <code>extId</code>, i.e., the Parser's extension ID. If a
   * suitable {@link Parser} is not found, then a <code>WARNING</code> level
   * message is logged, and a ParseException is thrown. If the parse is
   * uncessful for any other reason, then a <code>WARNING</code> level message
   * is logged, and a <code>ParseStatus.getEmptyParse()</code> is returned.
   * 
   * @param extId
   *          The extension implementation ID of the {@link Parser} to use to
   *          parse the specified content.
   * @param content
   *          The content to parse.
   * 
   * @return &lt;key, {@link Parse}&gt; pairs if the parse is successful,
   *         otherwise, a single &lt;key,
   *         <code>ParseStatus.getEmptyParse()</code>&gt; pair.
   * 
   * @throws ParseException
   *           If there is no suitable {@link Parser} found to perform the
   *           parse.
   */
  public ParseResult parseByExtensionId(String extId, Content content)
      throws ParseException {
    Parser p = null;

    try {
      p = this.parserFactory.getParserById(extId);
    } catch (ParserNotFound e) {
      if (LOG.isWarnEnabled()) {
        LOG.warn("No suitable parser found when trying to parse content "
            + content.getUrl() + " of type " + content.getContentType());
      }
      throw new ParseException(e.getMessage());
    }

    ParseResult parseResult = null;
    if (maxParseTime != -1)
      parseResult = runParser(p, content);
    else
      parseResult = p.getParse(content);
    if (parseResult != null && !parseResult.isEmpty()) {
      return parseResult;
    } else {
      if (LOG.isWarnEnabled()) {
        LOG.warn("Unable to successfully parse content " + content.getUrl()
            + " of type " + content.getContentType());
      }
      return new ParseStatus(new ParseException(
          "Unable to successfully parse content")).getEmptyParseResult(
          content.getUrl(), null);
    }
  }

  private ParseResult runParser(Parser p, Content content) {
    ParseCallable pc = new ParseCallable(p, content);
    Future<ParseResult> task = executorService.submit(pc);
    ParseResult res = null;
    try {
      res = task.get(maxParseTime, TimeUnit.SECONDS);
    } catch (Exception e) {
      LOG.warn("Error parsing " + content.getUrl() + " with " + p, e);
      task.cancel(true);
    } finally {
      pc = null;
    }
    return res;
  }

}
