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

package org.apache.nutch.indexer;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.Arrays;

import org.apache.lucene.index.*;
import org.apache.lucene.document.*;
import org.apache.lucene.store.*;
import org.apache.lucene.search.*;

import org.apache.nutch.util.NutchConf;

/** Sort a Nutch index by page score.  Higher scoring documents are assigned
 * smaller document numbers. */
public class IndexSorter {

  private static class PostingMap implements Comparable {
    private int newDoc;
    private long offset;

    public int compareTo(Object o) {              // order by newDoc id
      return this.newDoc - ((PostingMap)o).newDoc;
    }
  }

  private static class SortedTermPositions implements TermPositions {
    private TermPositions original;
    private int[] oldToNew;

    private int docFreq;

    private PostingMap[] postingMaps = new PostingMap[0];
    private int pointer;

    private int freq;
    private int position;

    private static final String TEMP_FILE = "temp";
    private final RAMDirectory tempDir = new RAMDirectory();
    private final RAMOutputStream out =
      (RAMOutputStream)tempDir.createOutput(TEMP_FILE);
    private IndexInput in;

    public SortedTermPositions(TermPositions original, int[] oldToNew) {
      this.original = original;
      this.oldToNew = oldToNew;
    }

    public void seek(Term term) throws IOException {
      throw new UnsupportedOperationException();
    }

    public void seek(TermEnum terms) throws IOException {
      original.seek(terms);

      docFreq = terms.docFreq();
      pointer = -1;

      if (docFreq > postingMaps.length) {         // grow postingsMap
        PostingMap[] newMap = new PostingMap[docFreq];
        System.arraycopy(postingMaps, 0, newMap, 0, postingMaps.length);
        for (int i = postingMaps.length; i < docFreq; i++) {
          newMap[i] = new PostingMap();
        }
        postingMaps = newMap;
      }

      out.reset();

      int i = 0;
      while (original.next()) {
        PostingMap map = postingMaps[i++];
        map.newDoc = oldToNew[original.doc()];    // remap the newDoc id
        map.offset = out.getFilePointer();        // save pointer to buffer

        final int tf = original.freq();           // buffer tf & positions
        out.writeVInt(tf);
        int prevPosition = 0;
        for (int j = tf; j > 0; j--) {            // delta encode positions
          int p = original.nextPosition();
          out.writeVInt(p - prevPosition);
          prevPosition = p;
        }
      }
      out.flush();
      docFreq = i;                                // allow for deletions
      
      Arrays.sort(postingMaps, 0, docFreq);       // resort by mapped doc ids

      // NOTE: this might be substantially faster if RAMInputStream were public
      // and supported a reset() operation.
      in = tempDir.openInput(TEMP_FILE);
    }
        
    public boolean next() throws IOException {
      pointer++;
      if (pointer < docFreq) {
        in.seek(postingMaps[pointer].offset);
        freq = in.readVInt();
        position = 0;
        return true;
      }
      return false;
    }
      
    public int doc() { return postingMaps[pointer].newDoc; }
    public int freq() { return freq; }

    public int nextPosition() throws IOException {
      int positionIncrement = in.readVInt();
      position += positionIncrement;
      return position;
    }

    public int read(int[] docs, int[] freqs) {
      throw new UnsupportedOperationException();
    }
    public boolean skipTo(int target) {
      throw new UnsupportedOperationException();
    }

    public void close() throws IOException {
      original.close();
    }

  }

  private static class SortingReader extends FilterIndexReader {
    
    private int[] oldToNew;
    private int[] newToOld;

    public SortingReader(IndexReader oldReader, int[] oldToNew) {
      super(oldReader);
      this.oldToNew = oldToNew;
      
      this.newToOld = new int[oldReader.maxDoc()];
      int oldDoc = 0;
      while (oldDoc < oldToNew.length) {
        int newDoc = oldToNew[oldDoc];
        if (newDoc != -1) {
          newToOld[newDoc] = oldDoc;
        }
        oldDoc++;
      }
    }

    public Document document(int n) throws IOException {
      return super.document(newToOld[n]);
    }

    public boolean isDeleted(int n) {
      return false;
    }

    public byte[] norms(String f) throws IOException {
      throw new UnsupportedOperationException();
    }

    public void norms(String f, byte[] norms, int offset) throws IOException {
      byte[] oldNorms = super.norms(f);
      int oldDoc = 0;
      while (oldDoc < oldNorms.length) {
        int newDoc = oldToNew[oldDoc];
        if (newDoc != -1) {
          norms[newDoc] = oldNorms[oldDoc];
        }
        oldDoc++;
      }
    }

    protected void doSetNorm(int d, String f, byte b) throws IOException {
      throw new UnsupportedOperationException();
    }

    public TermDocs termDocs() throws IOException {
      throw new UnsupportedOperationException();
    }
    
    public TermPositions termPositions() throws IOException {
      return new SortedTermPositions(super.termPositions(), oldToNew);
    }

    protected void doDelete(int n) throws IOException { 
      throw new UnsupportedOperationException();
    }

  }

  private static class DocScore implements Comparable {
    private int oldDoc;
    private float score;

    public int compareTo(Object o) {              // order by score, oldDoc
      DocScore that = (DocScore)o;
      if (this.score == that.score) {
        return this.oldDoc - that.oldDoc;
      } else {
        return this.score < that.score ? 1 : -1 ;
      }
    }
  }

  private File directory;

  public IndexSorter(File directory) {
    this.directory = directory;
  }

  public void sort(int termIndexInterval) throws IOException {
    IndexReader reader = IndexReader.open(new File(directory, "index"));

    SortingReader sorter = new SortingReader(reader, oldToNew(reader));
    IndexWriter writer = new IndexWriter(new File(directory, "index-sorted"),
                                         null, true);
    writer.setTermIndexInterval
      (termIndexInterval);
    writer.setUseCompoundFile(false);
    writer.addIndexes(new IndexReader[] { sorter });
    writer.close();
  }

  private static int[] oldToNew(IndexReader reader) throws IOException {
    int readerMax = reader.maxDoc();
    DocScore[] newToOld = new DocScore[readerMax];

    // use site, an indexed, un-tokenized field to get boost
    byte[] boosts = reader.norms("site");          

    for (int oldDoc = 0; oldDoc < readerMax; oldDoc++) {
      float score;
      if (reader.isDeleted(oldDoc)) {
        score = 0.0f;
      } else {
        score = Similarity.decodeNorm(boosts[oldDoc]);
      }
      DocScore docScore = new DocScore();
      docScore.oldDoc = oldDoc;
      docScore.score = score;
      newToOld[oldDoc] = docScore;
    }
    Arrays.sort(newToOld);

    int[] oldToNew = new int[readerMax];
    for (int newDoc = 0; newDoc < readerMax; newDoc++) {
      DocScore docScore = newToOld[newDoc];
      oldToNew[docScore.oldDoc] = docScore.score > 0.0f ? newDoc : -1;
    }    
    return oldToNew;
  }

  /** */
  public static void main(String[] args) throws Exception {
    File directory;
      
    String usage = "IndexSorter directory";

    if (args.length < 1) {
      System.err.println("Usage: " + usage);
      return;
    }

    directory = new File(args[0]);

    IndexSorter sorter = new IndexSorter(directory);

    Date start = new Date();
    NutchConf nutchConf = new NutchConf();
    int termIndexInterval = nutchConf.getInt("indexer.termIndexInterval", 128);
    sorter.sort(termIndexInterval);

    Date end = new Date();

    System.out.print(end.getTime() - start.getTime());
    System.out.println(" total milliseconds");
  }

}
