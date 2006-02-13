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

// JDK imports
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

// Jakarta POI imports
import org.apache.poi.hwpf.model.CHPBinTable;
import org.apache.poi.hwpf.model.CHPX;
import org.apache.poi.hwpf.model.ComplexFileTable;
import org.apache.poi.hwpf.model.TextPiece;
import org.apache.poi.hwpf.model.TextPieceTable;
import org.apache.poi.hwpf.sprm.SprmIterator;
import org.apache.poi.hwpf.sprm.SprmOperation;
import org.apache.poi.poifs.filesystem.DocumentEntry;
import org.apache.poi.poifs.filesystem.DocumentInputStream;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import org.apache.poi.util.LittleEndian;

// Nutch imports
import org.apache.nutch.parse.ms.MSExtractor;


/**
 * This class extracts the text from a Word 6.0/95/97/2000/XP word doc
 *
 * @author Ryan Ackley
 * @author Andy Hedges
 * @author J&eacute;r&ocirc;me Charron
 *
 */
class WordExtractor extends MSExtractor {


  /**
   * Gets the text from a Word document.
   *
   * @param in The InputStream representing the Word file.
   */
  protected String extractText(InputStream in) throws Exception {

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

}

