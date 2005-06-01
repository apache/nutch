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

package org.apache.nutch.parse.msword;

import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.LogFormatter;
import org.apache.nutch.parse.ParseStatus;
import org.apache.nutch.parse.Parser;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseImpl;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.ParseException;

import java.util.Properties;
//import java.util.logging.Logger;

import java.io.ByteArrayInputStream;

/**
 * parser for mime type application/msword.
 * It is based on org.apache.poi.*. We have to see how well it performs.
 *
 * @author John Xing
 *
 * Note on 20040614 by Xing:
 * Some codes are stacked here for convenience (see inline comments).
 * They may be moved to more appropriate places when new codebase
 * stabilizes, especially after code for indexing is written.
 *
 * @author Andy Hedges
 * code to extract all msword properties.
 *
 */

public class MSWordParser implements Parser {
//  public static final Logger LOG =
//    LogFormatter.getLogger("org.apache.nutch.parse.msword");

  public MSWordParser () {}

  public Parse getParse(Content content) {

    // check that contentType is one we can handle
    String contentType = content.getContentType();
    if (contentType != null && !contentType.startsWith("application/msword"))
      return new ParseStatus(ParseStatus.FAILED, ParseStatus.FAILED_INVALID_FORMAT,
        "Content-Type not application/msword: " + contentType).getEmptyParse();

    String text = null;
    String title = null;
    Properties properties = null;

    try {

      byte[] raw = content.getContent();

      String contentLength = content.get("Content-Length");
      if (contentLength != null
            && raw.length != Integer.parseInt(contentLength)) {
          return new ParseStatus(ParseStatus.FAILED, ParseStatus.FAILED_TRUNCATED,
                  "Content truncated at " + raw.length
            +" bytes. Parser can't handle incomplete msword file.").getEmptyParse();
      }

      WordExtractor extractor = new WordExtractor();

      // collect text
      text = extractor.extractText(new ByteArrayInputStream(raw));

      // collect meta info
      properties = extractor.extractProperties(new ByteArrayInputStream(raw));

      extractor = null;

    } catch (ParseException e) {
      return new ParseStatus(e).getEmptyParse();
    } catch (FastSavedException e) {
      return new ParseStatus(e).getEmptyParse();
    } catch (PasswordProtectedException e) {
      return new ParseStatus(e).getEmptyParse();
    } catch (Exception e) { // run time exception
      return new ParseStatus(ParseStatus.FAILED,
              "Can't be handled as msword document. " + e).getEmptyParse();
    } finally {
      // nothing so far
    }

    // collect meta data
    Properties metadata = new Properties();
    metadata.putAll(content.getMetadata()); // copy through

    if(properties != null) {
      title = properties.getProperty("Title");
      properties.remove("Title");
      metadata.putAll(properties);
    }

    if (text == null)
      text = "";

    if (title == null)
      title = "";

    // collect outlink
    Outlink[] outlinks = new Outlink[0];

    ParseData parseData = new ParseData(ParseStatus.STATUS_SUCCESS, title, outlinks, metadata);
    return new ParseImpl(text, parseData);
    // any filter?
    //return HtmlParseFilters.filter(content, parse, root);
  }

}
