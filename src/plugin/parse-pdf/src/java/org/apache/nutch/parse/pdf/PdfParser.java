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

package org.apache.nutch.parse.pdf;

import org.pdfbox.encryption.DocumentEncryption;
import org.pdfbox.pdfparser.PDFParser;
import org.pdfbox.pdmodel.PDDocument;
import org.pdfbox.pdmodel.PDDocumentInformation;
import org.pdfbox.util.PDFTextStripper;

import org.pdfbox.exceptions.CryptographyException;
import org.pdfbox.exceptions.InvalidPasswordException;

// Commons Logging imports
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.nutch.protocol.Content;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.net.protocols.Response;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.parse.ParseResult;
import org.apache.nutch.parse.ParseStatus;
import org.apache.nutch.parse.Parser;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseImpl;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.OutlinkExtractor;
import org.apache.nutch.util.LogUtil;

import java.text.SimpleDateFormat;
import java.util.Calendar;

import java.io.ByteArrayInputStream;
import java.io.IOException;

/*********************************************
 * parser for mime type application/pdf.
 * It is based on org.pdfbox.*. We have to see how well it does the job.
 * 
 * @author John Xing
 *
 * Note on 20040614 by Xing:
 * Some codes are stacked here for convenience (see inline comments).
 * They may be moved to more appropriate places when new codebase
 * stabilizes, especially after code for indexing is written.
 *
 *********************************************/

public class PdfParser implements Parser {
  public static final Log LOG = LogFactory.getLog("org.apache.nutch.parse.pdf");
  private Configuration conf;

  public ParseResult getParse(Content content) {

    // in memory representation of pdf file
    PDDocument pdf = null;

    String text = null;
    String title = null;
    Metadata metadata = new Metadata();

    try {

      byte[] raw = content.getContent();

      String contentLength = content.getMetadata().get(Response.CONTENT_LENGTH);
      if (contentLength != null
            && raw.length != Integer.parseInt(contentLength)) {
          return new ParseStatus(ParseStatus.FAILED, ParseStatus.FAILED_TRUNCATED,
                  "Content truncated at "+raw.length
            +" bytes. Parser can't handle incomplete pdf file.").getEmptyParseResult(content.getUrl(), getConf());
      }

      PDFParser parser = new PDFParser(new ByteArrayInputStream(raw));
      parser.parse();

      pdf = parser.getPDDocument();

      if (pdf.isEncrypted()) {
        DocumentEncryption decryptor = new DocumentEncryption(pdf);
        //Just try using the default password and move on
        decryptor.decryptDocument("");
      }

      // collect text
      PDFTextStripper stripper = new PDFTextStripper();
      text = stripper.getText(pdf);

      // collect title
      PDDocumentInformation info = pdf.getDocumentInformation();
      title = info.getTitle();
      // more useful info, currently not used. please keep them for future use.
      metadata.add(Metadata.PAGE_COUNT, String.valueOf(pdf.getPageCount()));
      metadata.add(Metadata.AUTHOR, info.getAuthor());
      metadata.add(Metadata.SUBJECT, info.getSubject());
      metadata.add(Metadata.KEYWORDS, info.getKeywords());
      metadata.add(Metadata.CREATOR, info.getCreator());
      metadata.add(Metadata.PUBLISHER, info.getProducer());
      
      //TODO: Figure out why we get a java.io.IOException: Error converting date:1-Jan-3 18:15PM
      //error here
      
      //metadata.put(DATE, dcDateFormatter.format(info.getCreationDate().getTime()));
      //metadata.put(LAST_MODIFIED, dcDateFormatter.format(info.getModificationDate().getTime()));

    } catch (CryptographyException e) {
      return new ParseStatus(ParseStatus.FAILED,
              "Error decrypting document. " + e).getEmptyParseResult(content.getUrl(), getConf());
    } catch (InvalidPasswordException e) {
      return new ParseStatus(ParseStatus.FAILED,
              "Can't decrypt document - invalid password. " + e).getEmptyParseResult(content.getUrl(), getConf());
    } catch (Exception e) { // run time exception
        if (LOG.isWarnEnabled()) {
          LOG.warn("General exception in PDF parser: "+e.getMessage());
          e.printStackTrace(LogUtil.getWarnStream(LOG));        
        }
      return new ParseStatus(ParseStatus.FAILED,
              "Can't be handled as pdf document. " + e).getEmptyParseResult(content.getUrl(), getConf());
    } finally {
      try {
        if (pdf != null)
          pdf.close();
        } catch (IOException e) {
          // nothing to do
        }
    }

    if (text == null)
      text = "";

    if (title == null)
      title = "";

    // collect outlink
    Outlink[] outlinks = OutlinkExtractor.getOutlinks(text, getConf());

    ParseData parseData = new ParseData(ParseStatus.STATUS_SUCCESS, title,
                                        outlinks, content.getMetadata(),
                                        metadata);
    return ParseResult.createParseResult(content.getUrl(), new ParseImpl(text, parseData));
    // any filter?
    //return HtmlParseFilters.filter(content, parse, root);
  }

  // format date
  // currently not used. please keep it for future use.
  private String formatDate(Calendar date) {
    String retval = null;
    if(date != null) {
      SimpleDateFormat formatter = new SimpleDateFormat();
      retval = formatter.format(date.getTime());
    }
    return retval;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  public Configuration getConf() {
    return this.conf;
  }

}
