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

  public ParseResult getParse(Content content) {

    // ParseData parseData = new ParseData(ParseStatus.STATUS_SUCCESS, "", new
    // Outlink[0], metadata);

    String encoding = StringUtil.parseCharacterEncoding(content
        .getContentType());
    String text;
    if (encoding != null) { // found an encoding header
      try { // try to use named encoding
        text = new String(content.getContent(), encoding);
      } catch (java.io.UnsupportedEncodingException e) {
        return new ParseStatus(e).getEmptyParseResult(content.getUrl(), getConf());
      }
    } else {
      // FIXME: implement charset detector. This code causes problem when
      // character set isn't specified in HTTP header.
      text = new String(content.getContent()); // use default encoding
    }
    ParseData parseData = new ParseData(ParseStatus.STATUS_SUCCESS, "",
        OutlinkExtractor.getOutlinks(text, getConf()), content.getMetadata());
    parseData.setConf(this.conf);
    return ParseResult.createParseResult(content.getUrl(), new ParseImpl(text, parseData));
    
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  public Configuration getConf() {
    return this.conf;
  }
}
