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

package org.apache.nutch.parse.zip;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.Parser;
import org.apache.nutch.protocol.Content;
import org.apache.hadoop.conf.Configuration;

/**
 * ZipParser class based on MSPowerPointParser class by Stephan Strittmatter.
 * Nutch parse plugin for zip files - Content Type : application/zip
 * 
 * @author Rohit Kulkarni & Ashish Vaidya
 */
public class ZipParser implements Parser {

  private static final Logger LOG = LoggerFactory.getLogger(ZipParser.class);
  private Configuration conf;

  /** Creates a new instance of ZipParser */
  public ZipParser() {
  }

  public ParseResult getParse(final Content content) {

    String resultText = null;
    String resultTitle = null;
    Outlink[] outlinks = null;
    List outLinksList = new ArrayList();
    Properties properties = null;

    try {
      final String contentLen = content.getMetadata().get(Response.CONTENT_LENGTH);
      final int len = Integer.parseInt(contentLen);
      if (LOG.isDebugEnabled()) { LOG.debug("ziplen: " + len); }
      final byte[] contentInBytes = content.getContent();
      final ByteArrayInputStream bainput = new ByteArrayInputStream(
          contentInBytes);
      final InputStream input = bainput;

      if (contentLen != null && contentInBytes.length != len) {
        return new ParseStatus(ParseStatus.FAILED,
            ParseStatus.FAILED_TRUNCATED, "Content truncated at "
                + contentInBytes.length
                + " bytes. Parser can't handle incomplete pdf file.")
            .getEmptyParseResult(content.getUrl(), getConf());
      }

      ZipTextExtractor extractor = new ZipTextExtractor(getConf());

      // extract text
      resultText = extractor.extractText(new ByteArrayInputStream(
          contentInBytes), content.getUrl(), outLinksList);

    } catch (Exception e) {
      return new ParseStatus(ParseStatus.FAILED,
          "Can't be handled as Zip document. " + e).getEmptyParseResult(content.getUrl(), getConf());
    }

    if (resultText == null) {
      resultText = "";
    }

    if (resultTitle == null) {
      resultTitle = "";
    }

    outlinks = (Outlink[]) outLinksList.toArray(new Outlink[0]);
    final ParseData parseData = new ParseData(ParseStatus.STATUS_SUCCESS,
                                              resultTitle, outlinks,
                                              content.getMetadata());

    if (LOG.isTraceEnabled()) { LOG.trace("Zip file parsed sucessfully !!"); }
    return ParseResult.createParseResult(content.getUrl(), new ParseImpl(resultText, parseData));
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  public Configuration getConf() {
    return this.conf;
  }

}
