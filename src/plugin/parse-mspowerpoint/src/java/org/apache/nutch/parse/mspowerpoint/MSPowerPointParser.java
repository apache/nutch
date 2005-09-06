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

package org.apache.nutch.parse.mspowerpoint;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;
import java.util.logging.Logger;

import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.OutlinkExtractor;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseImpl;
import org.apache.nutch.parse.ParseStatus;
import org.apache.nutch.parse.Parser;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.LogFormatter;

/**
 * Nutch-Parser for parsing MS PowerPoint slides ( mime type:
 * application/vnd.ms-powerpoint).
 * <p>
 * It is based on org.apache.poi.*.
 * 
 * @author Stephan Strittmatter - http://www.sybit.de
 * @see <a href="http://jakarta.apache.org/poi">Jakarta POI</a>
 * @version 1.0
 */
public class MSPowerPointParser implements Parser {

  /** associated Mime type for PowerPoint files (application/vnd.ms-powerpoint) */
  public static final String MIME_TYPE = "application/vnd.ms-powerpoint";

  private static final Logger LOG = LogFormatter
      .getLogger(MSPowerPointParser.class.getName());

  /**
   * 
   */
  public MSPowerPointParser() {
  }

  /**
   * Main for testing. Pass a ppt-file as argument
   * 
   * @param args
   */
  public static void main(String args[]) {
    if (args.length < 1) {
      System.err.println("Useage:");
      System.err.println("\tMSPowerPointParser <file>");
      System.exit(1);
    }

    String file = args[0];
    MSPowerPointParser ppe = new MSPowerPointParser();

    byte[] raw = getRawBytes(new File(file));

    Properties prop = new Properties();
    prop.setProperty("Content-Length", "" + raw.length);

    Content content = new Content(file, file, raw, MIME_TYPE, prop);

    System.out.println(ppe.getParse(content).getText());
  }

  /**
   * Parses the MS PowerPoint file.
   * 
   * @see org.apache.nutch.parse.Parser#getParse(Content)
   */
  public Parse getParse(final Content content) {

    // check that contentType is one we can handle
    final String contentType = content.getContentType();

    if (contentType != null && !contentType.startsWith(MIME_TYPE)) {
      return new ParseStatus(ParseStatus.FAILED,
          ParseStatus.FAILED_INVALID_FORMAT, "Content-Type is not ["
              + MIME_TYPE + "] was: " + contentType).getEmptyParse();
    }

    String plainText = null;
    String title = null;
    Outlink[] outlinks = null;
    Properties properties = null;

    try {
      final String contentLen = content.get("Content-Length");
      final byte[] raw = content.getContent();

      if (contentLen != null && raw.length != Integer.parseInt(contentLen)) {
        return new ParseStatus(
            ParseStatus.FAILED,
            ParseStatus.FAILED_TRUNCATED,
            "Content truncated at "
                + raw.length
                + " bytes. Please increase <protocol>.content.limit at nutch-default.xml. "
                + "Parser can't handle incomplete PowerPoint files.")
            .getEmptyParse();
      }

      final PPTExtractor extractor = new PPTExtractor(new ByteArrayInputStream(
          raw));

      plainText = extractor.getText();
      properties = extractor.getProperties();
      outlinks = OutlinkExtractor.getOutlinks(plainText, content.getUrl());

    } catch (Exception e) {
      LOG.throwing(this.getClass().getName(), "getParse", e);
      return new ParseStatus(e).getEmptyParse();
    }

    // collect meta data
    final Properties metadata = new Properties();
    metadata.putAll(content.getMetadata()); // copy through

    if (properties != null) {
      title = properties.getProperty("Title");
      properties.remove("Title");
      metadata.putAll(properties);
    }

    if (plainText == null) {
      plainText = "";
    }

    if (title == null) {
      title = "";
    }

    final ParseStatus status = new ParseStatus(ParseStatus.SUCCESS);
    final ParseData parseData = new ParseData(status, title, outlinks, metadata);

    LOG.finest("PowerPoint file parsed sucessful.");
    return new ParseImpl(plainText, parseData);
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
      err.printStackTrace();
      return null;
    }

  }
}