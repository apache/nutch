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

package org.creativecommons.nutch;

import org.apache.nutch.io.*;
import org.apache.nutch.util.LogFormatter;
import org.apache.nutch.indexer.IndexSegment;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.document.Document;

import java.io.*;
import java.util.Vector;
import java.util.logging.Logger;

/** Deletes documents in a set of Lucene indexes that do not have a Creative
 * Commons license. */
public class CCDeleteUnlicensedTool {
  private static final Logger LOG =
    LogFormatter.getLogger("org.creativecommons.nutch.CCDeleteUnlicensedTool");

  private IndexReader[] readers;

  /** Constructs a duplicate detector for the provided indexes. */
  public CCDeleteUnlicensedTool(IndexReader[] readers) {
    this.readers = readers;
  }

  /** Closes the indexes, saving changes. */
  public void close() throws IOException {
    for (int i = 0; i < readers.length; i++)
      readers[i].close();
  }

  /** Delete pages without CC licenes. */
  public int deleteUnlicensed() throws IOException {
    int deleteCount = 0;
    for (int index = 0; index < readers.length; index++) {
      IndexReader reader = readers[index];
      int readerMax = reader.maxDoc();
      for (int doc = 0; doc < readerMax; doc++) {
        if (!reader.isDeleted(doc)) {
          Document document = reader.document(doc);
          if (document.get(CCIndexingFilter.FIELD)==null){ // no CC fields
            reader.delete(doc);                   // delete it
            deleteCount++;
          }
        }
      }
    }
    return deleteCount;
  }

  /** Delete duplicates in the indexes in the named directory. */
  public static void main(String[] args) throws Exception {
    String usage = "CCDeleteUnlicensedTool <segmentsDir>";

    if (args.length != 1) {
      System.err.println("Usage: " + usage);
      return;
    } 

    String segmentsDir = args[0];

    File[] directories = new File(segmentsDir).listFiles();
    Vector vReaders=new Vector();
    int maxDoc = 0;
    for (int i = 0; i < directories.length; i++) {
      File indexDone = new File(directories[i], IndexSegment.DONE_NAME);
      if (indexDone.exists() && indexDone.isFile()){
        File indexDir = new File(directories[i], "index");
      	IndexReader reader = IndexReader.open(indexDir);
        maxDoc += reader.maxDoc();
        vReaders.add(reader);
      }
    }

    IndexReader[] readers=new IndexReader[vReaders.size()];
    for(int i = 0; vReaders.size()>0; i++) {
      readers[i]=(IndexReader)vReaders.remove(0);
    }

    CCDeleteUnlicensedTool dd = new CCDeleteUnlicensedTool(readers);
    int count = dd.deleteUnlicensed();
    LOG.info("CC: deleted "+count+" out of "+maxDoc);
    dd.close();
  }
}
