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
package org.apache.nutch.parse.ms;

// JDK imports
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

// Commons Logging imports
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

// Hadoop imports
import org.apache.hadoop.conf.Configuration;

// Nutch imports
import org.apache.nutch.metadata.DublinCore;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.OutlinkExtractor;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseImpl;
import org.apache.nutch.parse.ParseResult;
import org.apache.nutch.parse.ParseStatus;
import org.apache.nutch.parse.Parser;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.LogUtil;
import org.apache.nutch.util.NutchConfiguration;


/**
 * A generic Microsoft document parser.
 *
 * @author J&eacute;r&ocirc;me Charron
 */
public abstract class MSBaseParser implements Parser {
  
  private Configuration conf;
  
  protected static final Log LOG = LogFactory.getLog(MSBaseParser.class);


  /**
   * Parses a Content with a specific {@link MSExtractor Microsoft document
   * extractor}.
   */
  protected ParseResult getParse(MSExtractor extractor, Content content) {
    
    String text = null;
    String title = null;
    Outlink[] outlinks = null;
    Properties properties = null;
    
    try {
      byte[] raw = content.getContent();
      String contentLength = content.getMetadata().get(Metadata.CONTENT_LENGTH);
      if ((contentLength != null) &&
          (raw.length != Integer.parseInt(contentLength))) {
        return new ParseStatus(ParseStatus.FAILED,
                               ParseStatus.FAILED_TRUNCATED,
                               "Content truncated at " + raw.length +" bytes. " +
                               "Parser can't handle incomplete file.")
                               .getEmptyParseResult(content.getUrl(), this.conf);
      }
      extractor.extract(new ByteArrayInputStream(raw));
      text = extractor.getText();
      properties = extractor.getProperties();
      outlinks = OutlinkExtractor.getOutlinks(text, content.getUrl(), getConf());
      
    } catch (Exception e) {
      return new ParseStatus(ParseStatus.FAILED,
                             "Can't be handled as Microsoft document. " + e)
                             .getEmptyParseResult(content.getUrl(), this.conf);
    }
    
    // collect meta data
    Metadata metadata = new Metadata();
    if (properties != null) {
      title = properties.getProperty(DublinCore.TITLE);
      properties.remove(DublinCore.TITLE);
      metadata.setAll(properties);
    }

    if (text == null) { text = ""; }
    if (title == null) { title = ""; }

    ParseData parseData = new ParseData(ParseStatus.STATUS_SUCCESS, title,
                                        outlinks, content.getMetadata(),
                                        metadata);
    return ParseResult.createParseResult(content.getUrl(), 
                                         new ParseImpl(text, parseData));
  }

  
  /**
   * Main for testing. Pass a ms document as argument
   */
  public static void main(String mime, MSBaseParser parser, String args[]) {
    if (args.length < 1) {
      System.err.println("Usage:");
      System.err.println("\t" + parser.getClass().getName() + " <file>");
      System.exit(1);
    }

    String file = args[0];
    byte[] raw = getRawBytes(new File(file));

    Metadata meta = new Metadata();
    meta.set(Response.CONTENT_LENGTH, "" + raw.length);
    Content content = new Content(file, file, raw, mime, meta,
                                  NutchConfiguration.create());

    System.out.println(parser.getParse(content).get(file).getText());
  }

  private final static byte[] getRawBytes(File f) {
    try {
      if (!f.exists())
        return null;
      FileInputStream fin = new FileInputStream(f);
      byte[] buffer = new byte[(int) f.length()];
      fin.read(buffer);
      fin.close();
      return buffer;
    } catch (Exception err) {
      err.printStackTrace(LogUtil.getErrorStream(LOG));
      return null;
    }

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
