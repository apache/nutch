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

package org.apache.nutch.parse.zip;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.ArrayList;
import java.util.List;

import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseException;
import org.apache.nutch.parse.ParseImpl;
import org.apache.nutch.parse.ParseStatus;
import org.apache.nutch.parse.Parser;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.ContentProperties;
import org.apache.hadoop.util.LogFormatter;
import org.apache.hadoop.conf.Configuration;

/**
 * ZipParser class based on MSPowerPointParser class by Stephan Strittmatter.
 * Nutch parse plugin for zip files - Content Type : application/zip
 * 
 * @author Rohit Kulkarni & Ashish Vaidya
 */
public class ZipParser implements Parser {

  private static final Logger LOG = LogFormatter.getLogger(ZipParser.class
      .getName());
  private Configuration conf;

  /** Creates a new instance of ZipParser */
  public ZipParser() {
  }

  public Parse getParse(final Content content) {

    String resultText = null;
    String resultTitle = null;
    Outlink[] outlinks = null;
    List outLinksList = new ArrayList();
    Properties properties = null;

    try {
      final String contentLen = content.get("Content-Length");
      final int len = Integer.parseInt(contentLen);
      System.out.println("ziplen: " + len);
      final byte[] contentInBytes = content.getContent();
      final ByteArrayInputStream bainput = new ByteArrayInputStream(
          contentInBytes);
      final InputStream input = bainput;

      if (contentLen != null && contentInBytes.length != len) {
        return new ParseStatus(ParseStatus.FAILED,
            ParseStatus.FAILED_TRUNCATED, "Content truncated at "
                + contentInBytes.length
                + " bytes. Parser can't handle incomplete pdf file.")
            .getEmptyParse(getConf());
      }

      ZipTextExtractor extractor = new ZipTextExtractor(getConf());

      // extract text
      resultText = extractor.extractText(new ByteArrayInputStream(
          contentInBytes), content.getUrl(), outLinksList);

    } catch (Exception e) {
      return new ParseStatus(ParseStatus.FAILED,
          "Can't be handled as Zip document. " + e).getEmptyParse(getConf());
    }

    // collect meta data
    final ContentProperties metadata = new ContentProperties();
    metadata.putAll(content.getMetadata()); // copy through

    if (resultText == null) {
      resultText = "";
    }

    if (resultTitle == null) {
      resultTitle = "";
    }

    outlinks = (Outlink[]) outLinksList.toArray(new Outlink[0]);
    final ParseData parseData = new ParseData(ParseStatus.STATUS_SUCCESS,
        resultTitle, outlinks, metadata);
    parseData.setConf(this.conf);

    LOG.finest("Zip file parsed sucessfully !!");
    return new ParseImpl(resultText, parseData);
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  public Configuration getConf() {
    return this.conf;
  }

}
