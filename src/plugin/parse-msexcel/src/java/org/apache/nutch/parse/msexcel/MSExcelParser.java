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
package org.apache.nutch.parse.msexcel;

// JDK imports
import java.io.ByteArrayInputStream;
import java.util.Properties;
import java.util.logging.Logger;

// Hadoop imports
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.LogFormatter;

// Nutch imports
import org.apache.nutch.metadata.DublinCore;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.OutlinkExtractor;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseImpl;
import org.apache.nutch.parse.ParseStatus;
import org.apache.nutch.parse.Parser;
import org.apache.nutch.protocol.Content;

/**
 * An Excel document parser.
 *
 * @author Rohit Kulkarni & Ashish Vaidya
 * @author J&eacute;r&ocirc;me Charron
 */
public class MSExcelParser implements Parser {
  
  private Configuration conf;
  
  private static final Logger LOG = LogFormatter.getLogger(MSExcelParser.class.getName());

  /** Creates a new instance of MSExcelParser */
  public MSExcelParser() { }
  
  public Parse getParse(Content content) {
    
    String text = null;
    String title = null;
    Properties properties = null;
    
    try {
      byte[] raw = content.getContent();
      String contentLength = content.getMetadata().get(Metadata.CONTENT_LENGTH);
      if ((contentLength != null) &&
          (raw.length != Integer.parseInt(contentLength))) {
        return new ParseStatus(ParseStatus.FAILED,
                               ParseStatus.FAILED_TRUNCATED,
                               "Content truncated at " + raw.length +" bytes. " +
                               "Parser can't handle incomplete msexcelfile.")
                               .getEmptyParse(this.conf);
      }

      ExcelExtractor extractor = new ExcelExtractor();      
      // Extract text
      text = extractor.extractText(new ByteArrayInputStream(raw));
      // Extract properties
      properties = extractor.extractProperties(new ByteArrayInputStream(raw));
      
      //currently returning empty outlinks array
      //outlinks = this.fetchOutlinks(resultText);
      
    } catch (Exception e) {
      return new ParseStatus(ParseStatus.FAILED,
                             "Can't be handled as msexcel document. " + e)
                             .getEmptyParse(this.conf);
    } finally {
      // nothing so far
    }
    
    // collect meta data
    Metadata metadata = new Metadata();
    title = properties.getProperty(DublinCore.TITLE);
    properties.remove(DublinCore.TITLE);
    metadata.setAll(properties);

    if (text == null) { text = ""; }
    if (title == null) { title = ""; }

    // collect outlink
    Outlink[] outlinks = OutlinkExtractor.getOutlinks(text, this.conf);

    ParseData parseData = new ParseData(ParseStatus.STATUS_SUCCESS, title,
                                        outlinks, content.getMetadata(),
                                        metadata);
    parseData.setConf(this.conf);
    return new ParseImpl(text, parseData);
  }


  /* ---------------------------- *
   * <implemenation:Configurable> *
   * ---------------------------- */
  
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  public Configuration getConf() {
    return this.conf;
  }

  /* ----------------------------- *
   * </implemenation:Configurable> *
   * ----------------------------- */

}
