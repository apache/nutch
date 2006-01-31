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

package org.apache.nutch.parse.text;

import java.util.Properties;

import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.ContentProperties;
import org.apache.nutch.parse.*;
import org.apache.nutch.util.*;

public class TextParser implements Parser {
  private NutchConf nutchConf;

  public Parse getParse(Content content) {
    // copy content meta data through
    ContentProperties metadata = new ContentProperties();
    metadata.putAll(content.getMetadata());

    // ParseData parseData = new ParseData(ParseStatus.STATUS_SUCCESS, "", new
    // Outlink[0], metadata);

    String encoding = StringUtil.parseCharacterEncoding(content
        .getContentType());
    String text;
    if (encoding != null) { // found an encoding header
      try { // try to use named encoding
        text = new String(content.getContent(), encoding);
      } catch (java.io.UnsupportedEncodingException e) {
        return new ParseStatus(e).getEmptyParse(getConf());
      }
    } else {
      // FIXME: implement charset detector. This code causes problem when
      // character set isn't specified in HTTP header.
      text = new String(content.getContent()); // use default encoding
    }
    ParseData parseData = new ParseData(ParseStatus.STATUS_SUCCESS, "",
        OutlinkExtractor.getOutlinks(text, getConf()), metadata);
    parseData.setConf(this.nutchConf);
    return new ParseImpl(text, parseData);
    
  }

  public void setConf(NutchConf conf) {
    this.nutchConf = conf;
  }

  public NutchConf getConf() {
    return this.nutchConf;
  }
}
