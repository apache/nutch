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
import java.util.BitSet;
import java.util.Date;

import org.apache.lucene.index.FilterIndexReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.index.TermPositions;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.util.PriorityQueue;

/** */
public class IndexOptimizer {
  public static final String DONE_NAME = "optimize.done";

  private static final float IDF_THRESHOLD = 6.0f;
  private static final float FRACTION = 0.1f;

  private static class FilterTermDocs implements TermDocs {
    protected TermDocs in;

    public FilterTermDocs(TermDocs in) { this.in = in; }

    public void seek(Term term) throws IOException { in.seek(term); }
    public void seek(TermEnum e) throws IOException { in.seek(e); }
    public int doc() { return in.doc(); }
    public int freq() { return in.freq(); }
    public boolean next() throws IOException { return in.next(); }
    public int read(int[] docs, int[] freqs) throws IOException {
      return in.read(docs, freqs);
    }
    public boolean skipTo(int i) throws IOException { return in.skipTo(i); }
    public void close() throws IOException { in.close(); } 
  }

  private static class FilterTermPositions
     extends FilterTermDocs implements TermPositions {

    public FilterTermPositions(TermPositions in) { super(in); }

    public int nextPosition() throws IOException {
      return ((TermPositions)in).nextPosition();
    }
  }

  private static class FilterTermEnum extends TermEnum {
    protected TermEnum in;

    public FilterTermEnum(TermEnum in) { this.in = in; }

    public boolean next() throws IOException { return in.next(); }
    public Term term() { return in.term(); }
    public int docFreq() { return in.docFreq(); }
    public void close() throws IOException { in.close(); }
  }

  private static class OptimizingTermEnum extends FilterTermEnum {
    private IndexReader reader;
    private Similarity similarity;

    public OptimizingTermEnum(IndexReader reader, Similarity similarity)
      throws IOException {
      super(reader.terms());
      this.reader = reader;
      this.similarity = similarity;
    }

    public boolean next() throws IOException {
      while (in.next()) {
        float idf = similarity.idf(in.docFreq(), reader.maxDoc());

        if (idf <= IDF_THRESHOLD)
          return true;
      }
      return false;
    }
  }
    
  private static class ScoreDocQueue extends PriorityQueue {
    ScoreDocQueue(int size) {
      initialize(size);
    }
    
    protected final boolean lessThan(Object a, Object b) {
      ScoreDoc hitA = (ScoreDoc)a;
      ScoreDoc hitB = (ScoreDoc)b;
      if (hitA.score == hitB.score)
        return hitA.doc > hitB.doc; 
      else
        return hitA.score < hitB.score;
    }
  }

  private static class OptimizingTermPositions extends FilterTermPositions {
    private IndexReader reader;
    private TermDocs termDocs;
    private int docFreq;
    private ScoreDocQueue sdq;
    private BitSet docs;
    private Similarity similarity;

    public OptimizingTermPositions(IndexReader reader, Similarity similarity)
      throws IOException {
      super(reader.termPositions());
      this.reader = reader;
      this.termDocs = reader.termDocs();
      this.similarity = similarity;
      this.sdq = new ScoreDocQueue((int)Math.ceil(reader.maxDoc() * FRACTION));
      this.docs = new BitSet(reader.maxDoc());
    }

    public void seek(TermEnum e) throws IOException {
      super.seek(e);
      termDocs.seek(e);

      byte[] norms = reader.norms(e.term().field());

      sdq.clear();
      float minScore = 0.0f;
      int count = (int)Math.ceil(e.docFreq() * FRACTION);
      System.out.println("Optimizing " + e.term()
                         + " from " + e.docFreq() 
                         + " to " + count); 
      while (termDocs.next()) {
        int doc = termDocs.doc();
        float score =
          similarity.tf(termDocs.freq()) * similarity.decodeNorm(norms[doc]);

        if (score > minScore) {
          sdq.put(new ScoreDoc(doc, score));
          if (sdq.size() > count) {               // if sdq overfull
            sdq.pop();                            // remove lowest in sdq
            minScore = ((ScoreDoc)sdq.top()).score; // reset minScore
          }
        }
      }

      docs.clear();
      while (sdq.size() != 0) {
        docs.set(((ScoreDoc)sdq.pop()).doc);
      }

    }        
        
    public boolean next() throws IOException {
      while (in.next()) {
        if (docs.get(in.doc()))
          return true;
      }
      return false;
    }
      
  }

  private static class OptimizingReader extends FilterIndexReader {
    private Similarity similarity = new NutchSimilarity();

    
    public OptimizingReader(IndexReader reader) {
      super(reader);
    }

    // don't copy any per-document data
    public int numDocs() { return 0; }
    public int maxDoc() { return 0; }

    // filter out low frequency terms
    public TermEnum terms() throws IOException {
      return new OptimizingTermEnum(in, similarity);
    }

    // filter out low-scoring postings
    public TermPositions termPositions() throws IOException {
      return new OptimizingTermPositions(in, similarity);
    }

    public boolean hasDeletions() { return false; }
  }


  private File directory;

  public IndexOptimizer(File directory) {
    this.directory = directory;
  }

  public void optimize() throws IOException {
    IndexReader reader = IndexReader.open(new File(directory, "index"));
    OptimizingReader optimizer = new OptimizingReader(reader);
    IndexWriter writer = new IndexWriter(new File(directory, "index-opt"),
                                         null, true);
    writer.addIndexes(new IndexReader[] { optimizer });
  }

  /** */
  public static void main(String[] args) throws Exception {
    File directory;
      
    String usage = "IndexOptimizer directory";

    if (args.length < 1) {
      System.err.println("Usage: " + usage);
      return;
    }

    directory = new File(args[0]);

    IndexOptimizer optimizer = new IndexOptimizer(directory);

    Date start = new Date();

    optimizer.optimize();

    Date end = new Date();

    System.out.print(end.getTime() - start.getTime());
    System.out.println(" total milliseconds");
  }

}
