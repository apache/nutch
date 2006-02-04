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

package org.apache.nutch.parse.pdf;

import org.pdfbox.encryption.DocumentEncryption;
import org.pdfbox.pdfparser.PDFParser;
import org.pdfbox.pdmodel.PDDocument;
import org.pdfbox.pdmodel.PDDocumentInformation;
import org.pdfbox.util.PDFTextStripper;

import org.pdfbox.exceptions.CryptographyException;
import org.pdfbox.exceptions.InvalidPasswordException;

import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.ContentProperties;
import org.apache.hadoop.util.LogFormatter;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.parse.ParseStatus;
import org.apache.nutch.parse.Parser;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseImpl;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.OutlinkExtractor;
import org.apache.nutch.parse.ParseException;

import java.text.SimpleDateFormat;
import java.util.Calendar;

import java.util.Properties;
import java.util.logging.Logger;

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
  public static final Logger LOG =
    LogFormatter.getLogger("org.apache.nutch.parse.pdf");
  private Configuration conf;

  public PdfParser () {
    // redirect org.apache.log4j.Logger to java's native logger, in order
    // to, at least, suppress annoying log4j warnings.
    // Note on 20040614 by Xing:
    // log4j is used by pdfbox. This snippet'd better be moved
    // to a common place shared by all parsers that use log4j.
    org.apache.log4j.Logger rootLogger =
      org.apache.log4j.Logger.getRootLogger();

    rootLogger.setLevel(org.apache.log4j.Level.INFO);

    org.apache.log4j.Appender appender = new org.apache.log4j.WriterAppender(
      new org.apache.log4j.SimpleLayout(),
      org.apache.hadoop.util.LogFormatter.getLogStream(
        this.LOG, java.util.logging.Level.INFO));

    rootLogger.addAppender(appender);
  }

  public Parse getParse(Content content) {

    // in memory representation of pdf file
    PDDocument pdf = null;

    String text = null;
    String title = null;

    try {

      byte[] raw = content.getContent();

      String contentLength = content.get("Content-Length");
      if (contentLength != null
            && raw.length != Integer.parseInt(contentLength)) {
          return new ParseStatus(ParseStatus.FAILED, ParseStatus.FAILED_TRUNCATED,
                  "Content truncated at "+raw.length
            +" bytes. Parser can't handle incomplete pdf file.").getEmptyParse(getConf());
      }

      PDFParser parser = new PDFParser(
        new ByteArrayInputStream(raw));
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
      // pdf.getPageCount();
      // info.getAuthor()
      // info.getSubject()
      // info.getKeywords()
      // info.getCreator()
      // info.getProducer()
      // info.getTrapped()
      // formatDate(info.getCreationDate())
      // formatDate(info.getModificationDate())

    } catch (CryptographyException e) {
      return new ParseStatus(ParseStatus.FAILED,
              "Error decrypting document. " + e).getEmptyParse(getConf());
    } catch (InvalidPasswordException e) {
      return new ParseStatus(ParseStatus.FAILED,
              "Can't decrypt document - invalid password. " + e).getEmptyParse(getConf());
    } catch (Exception e) { // run time exception
      return new ParseStatus(ParseStatus.FAILED,
              "Can't be handled as pdf document. " + e).getEmptyParse(getConf());
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

    // collect meta data
    ContentProperties metadata = new ContentProperties();
    metadata.putAll(content.getMetadata()); // copy through

    ParseData parseData = new ParseData(ParseStatus.STATUS_SUCCESS, title, outlinks, metadata);
    parseData.setConf(this.conf);
    return new ParseImpl(text, parseData);
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
