/*  Copyright 2004 Ryan Ackley
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.nutch.parse.msword;

import org.apache.poi.hpsf.*;
import org.apache.poi.hwpf.model.*;
import org.apache.poi.hwpf.sprm.*;
import org.apache.poi.poifs.eventfilesystem.*;
import org.apache.poi.poifs.filesystem.*;
import org.apache.poi.util.LittleEndian;
import org.apache.nutch.metadata.Metadata;

import java.util.*;
import java.io.*;

/**
 * This class extracts the text from a Word 6.0/95/97/2000/XP word doc
 *
 * @author Ryan Ackley
 *
 * @author Andy Hedges
 * code to extract all msword properties.
 *
 */
public class WordExtractor {

  /**
   * Constructor
   */
  public WordExtractor()
  {
  }

  /**
   * Gets the text from a Word document.
   *
   * @param in The InputStream representing the Word file.
   */
  public String extractText(InputStream in) throws Exception
  {
    ArrayList text = new ArrayList();
    POIFSFileSystem fsys = new POIFSFileSystem(in);

    // load our POIFS document streams.
    DocumentEntry headerProps =
        (DocumentEntry)fsys.getRoot().getEntry("WordDocument");
    DocumentInputStream din = fsys.createDocumentInputStream("WordDocument");
    byte[] header = new byte[headerProps.getSize()];


    din.read(header);
    din.close();

    int info = LittleEndian.getShort(header, 0xa);
    if ((info & 0x4) != 0)
    {
      throw new FastSavedException("Fast-saved files are unsupported at this time");
    }
    if ((info & 0x100) != 0)
    {
      throw new PasswordProtectedException("This document is password protected");
    }

    // determine the version of Word this document came from.
    int nFib = LittleEndian.getShort(header, 0x2);
    switch (nFib)
    {
      case 101:
      case 102:
      case 103:
      case 104:
        // this is a Word 6.0 doc send it to the extractor for that version.
        Word6Extractor oldExtractor = new Word6Extractor();
        return oldExtractor.extractText(header);
    }

    //Get the information we need from the header
    boolean useTable1 = (info & 0x200) != 0;

    //get the location of the piece table
    int complexOffset = LittleEndian.getInt(header, 0x1a2);

    // determine which table stream we must use.
    String tableName = null;
    if (useTable1)
    {
      tableName = "1Table";
    }
    else
    {
      tableName = "0Table";
    }

    DocumentEntry table = (DocumentEntry)fsys.getRoot().getEntry(tableName);
    byte[] tableStream = new byte[table.getSize()];

    din = fsys.createDocumentInputStream(tableName);

    din.read(tableStream);
    din.close();

    int chpOffset = LittleEndian.getInt(header, 0xfa);
    int chpSize = LittleEndian.getInt(header, 0xfe);
    int fcMin = LittleEndian.getInt(header, 0x18);
    CHPBinTable cbt = new CHPBinTable(header, tableStream, chpOffset, chpSize, fcMin);

    // load our text pieces and our character runs
    ComplexFileTable cft = new ComplexFileTable(header, tableStream, complexOffset, fcMin);
    TextPieceTable tpt = cft.getTextPieceTable();
    List textPieces = tpt.getTextPieces();

    // make the POIFS objects available for garbage collection
    din = null;
    fsys = null;
    table = null;
    headerProps = null;

    List textRuns = cbt.getTextRuns();
    Iterator runIt = textRuns.iterator();
    Iterator textIt = textPieces.iterator();

    TextPiece currentPiece = (TextPiece)textIt.next();
    int currentTextStart = currentPiece.getStart();
    int currentTextEnd = currentPiece.getEnd();

    WordTextBuffer finalTextBuf = new WordTextBuffer();

    // iterate through all text runs extract the text only if they haven't been
    // deleted
    while (runIt.hasNext())
    {
      CHPX chpx = (CHPX)runIt.next();
      boolean deleted = isDeleted(chpx.getGrpprl());
      if (deleted)
      {
        continue;
      }

      int runStart = chpx.getStart();
      int runEnd = chpx.getEnd();

      while (runStart >= currentTextEnd)
      {
        currentPiece = (TextPiece) textIt.next ();
        currentTextStart = currentPiece.getStart ();
        currentTextEnd = currentPiece.getEnd ();
      }

      if (runEnd < currentTextEnd)
      {
        String str = currentPiece.substring(runStart - currentTextStart, runEnd - currentTextStart);
        finalTextBuf.append(str);
      }
      else if (runEnd > currentTextEnd)
      {
        while (runEnd > currentTextEnd)
        {
          String str = currentPiece.substring(runStart - currentTextStart,
                                   currentTextEnd - currentTextStart);
          finalTextBuf.append(str);
          if (textIt.hasNext())
          {
            currentPiece = (TextPiece) textIt.next ();
            currentTextStart = currentPiece.getStart ();
            runStart = currentTextStart;
            currentTextEnd = currentPiece.getEnd ();
          }
          else
          {
            return finalTextBuf.toString();
          }
        }
        String str = currentPiece.substring(0, runEnd - currentTextStart);
        finalTextBuf.append(str);
      }
      else
      {
        String str = currentPiece.substring(runStart - currentTextStart, runEnd - currentTextStart);
        if (textIt.hasNext())
        {
          currentPiece = (TextPiece) textIt.next();
          currentTextStart = currentPiece.getStart();
          currentTextEnd = currentPiece.getEnd();
        }
        finalTextBuf.append(str);
      }
    }
    return finalTextBuf.toString();
  }

  /**
   * Used to determine if a run of text has been deleted.
   *
   * @param grpprl The list of sprms for a particular run of text.
   * @return true if this run of text has been deleted.
   */
  private boolean isDeleted(byte[] grpprl)
  {
    SprmIterator iterator = new SprmIterator(grpprl,0);
    while (iterator.hasNext())
    {
      SprmOperation op = iterator.next();
      // 0 is the operation that signals a FDelRMark operation
      if (op.getOperation() == 0 && op.getOperand() != 0)
      {
        return true;
      }
    }
    return false;
  }

  public Properties extractProperties(InputStream in)
                      throws IOException {

    PropertiesBroker propertiesBroker = new PropertiesBroker();
    POIFSReader reader = new POIFSReader();
    reader.registerListener(new PropertiesReaderListener(propertiesBroker),
                            "\005SummaryInformation");
    reader.read(in);
    return propertiesBroker.getProperties();
  }

  class PropertiesReaderListener
    implements POIFSReaderListener {

    private PropertiesBroker propertiesBroker;
    private Properties metaData = new Properties();

    public PropertiesReaderListener(PropertiesBroker propertiesBroker) {
      this.propertiesBroker = propertiesBroker;
    }

    public void processPOIFSReaderEvent(POIFSReaderEvent event) {

      SummaryInformation si = null;
      Properties properties = new Properties();

      try {
        si = (SummaryInformation)PropertySetFactory.create(event.getStream());
      } catch (Exception ex) {
        properties = null;
      }

      Date tmp = null;

      String title = si.getTitle();
      String applicationName = si.getApplicationName();
      String author = si.getAuthor();
      int charCount = si.getCharCount();
      String comments = si.getComments();
      Date createDateTime = si.getCreateDateTime();
      long editTime = si.getEditTime();
      String keywords = si.getKeywords();
      String lastAuthor = si.getLastAuthor();
      Date lastPrinted = si.getLastPrinted();
      Date lastSaveDateTime = si.getLastSaveDateTime();
      int pageCount = si.getPageCount();
      String revNumber = si.getRevNumber();
      int security = si.getSecurity();
      String subject = si.getSubject();
      String template = si.getTemplate();
      int wordCount = si.getWordCount();

      /*Dates are being stored in millis since the epoch to aid
      localization*/
      if(title != null)
        properties.setProperty(Metadata.TITLE, title);
      if(applicationName != null)
        properties.setProperty(Metadata.APPLICATION_NAME, applicationName);
      if(author != null)
        properties.setProperty(Metadata.AUTHOR, author);
      if(charCount != 0)
        properties.setProperty(Metadata.CHARACTER_COUNT, charCount + "");
      if(comments != null)
        properties.setProperty(Metadata.COMMENTS, comments);
      if(createDateTime != null)
        properties.setProperty(Metadata.DATE,
                               Metadata.DATE_FORMAT.format(createDateTime));
      if(editTime != 0)
        properties.setProperty(Metadata.LAST_MODIFIED, editTime + "");
      if(keywords != null)
        properties.setProperty(Metadata.KEYWORDS, keywords);
      if(lastAuthor != null)
        properties.setProperty(Metadata.LAST_AUTHOR, lastAuthor);
      if(lastPrinted != null)
        properties.setProperty(Metadata.LAST_PRINTED, lastPrinted.getTime() + "");
      if(lastSaveDateTime != null)
        properties.setProperty(Metadata.LAST_SAVED, lastSaveDateTime.getTime() + "");
      if(pageCount != 0)
        properties.setProperty(Metadata.PAGE_COUNT, pageCount + "");
      if(revNumber != null)
        properties.setProperty(Metadata.REVISION_NUMBER, revNumber);
      if(security != 0)
        properties.setProperty(Metadata.RIGHTS, security + "");
      if(subject != null)
        properties.setProperty(Metadata.SUBJECT, subject);
      if(template != null)
        properties.setProperty(Metadata.TEMPLATE, template);
      if(wordCount != 0)
        properties.setProperty(Metadata.WORD_COUNT, wordCount + "");
      propertiesBroker.setProperties(properties);

      //si.getThumbnail(); // can't think of a sensible way of turning this into a string.
    }
  }

  class PropertiesBroker {

    private Properties properties;
    private int timeoutMillis = 2 * 1000;


    public synchronized Properties getProperties() {

      long start = new Date().getTime();
      long now = start;

      while (properties == null && now - start < timeoutMillis) {
        try {
          wait(timeoutMillis / 10);
        } catch (InterruptedException e) {}
        now = new Date().getTime();
      }

      notifyAll();

      return properties;
    }

    public synchronized void setProperties(Properties properties) {
      this.properties = properties;
      notifyAll();
    }
  }
}

