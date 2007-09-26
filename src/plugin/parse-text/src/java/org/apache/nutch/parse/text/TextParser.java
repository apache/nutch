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

package org.apache.nutch.parse.text;

import org.apache.nutch.protocol.Content;
import org.apache.nutch.parse.*;
import org.apache.nutch.util.*;

import org.apache.hadoop.conf.Configuration;

public class TextParser implements Parser {

  private Configuration conf;
  
  /**
   * Encoding to be used when character set isn't specified
   * as HTTP header.
   */
  private String defaultEncoding;

  /**
   * Parses plain text document. This code uses configured default encoding
   * {@code parser.character.encoding.default} if character set isn't specified
   * as HTTP header.
   */
  public ParseResult getParse(Content content) {
    EncodingDetector detector = new EncodingDetector(conf);
    detector.autoDetectClues(content, false);
    String encoding = detector.guessEncoding(content, defaultEncoding);
    String text;
    try {
      text = new String(content.getContent(), encoding);
    } catch (java.io.UnsupportedEncodingException e) {
      return new ParseStatus(e)
          .getEmptyParseResult(content.getUrl(), getConf());
    }
    
    ParseData parseData = new ParseData(ParseStatus.STATUS_SUCCESS, "",
        OutlinkExtractor.getOutlinks(text, getConf()), content.getMetadata());
    return ParseResult.createParseResult(content.getUrl(), new ParseImpl(text, parseData));
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
    defaultEncoding = conf.get("parser.character.encoding.default",
        "windows-1252");
  }

  public Configuration getConf() {
    return this.conf;
  }
}
