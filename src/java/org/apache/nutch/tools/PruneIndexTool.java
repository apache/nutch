/*
 * Created on Nov 2, 2004
 * Author: Andrzej Bialecki <ab@getopt.org>
 *
 */
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

package org.apache.nutch.tools;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.util.BitSet;
import java.util.StringTokenizer;
import java.util.Vector;

// Commons Logging imports
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.util.NutchConfiguration;

import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.HitCollector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

/**
 * This tool prunes existing Nutch indexes of unwanted content. The main method
 * accepts a list of segment directories (containing indexes). These indexes will
 * be pruned of any content that matches one or more query from a list of Lucene
 * queries read from a file (defined in standard config file, or explicitly
 * overridden from command-line). Segments should already be indexed, if some
 * of them are missing indexes then these segments will be skipped.
 * 
 * <p>NOTE 1: Queries are expressed in Lucene's QueryParser syntax, so a knowledge
 * of available Lucene document fields is required. This can be obtained by reading sources
 * of <code>index-basic</code> and <code>index-more</code> plugins, or using tools
 * like <a href="http://www.getopt.org/luke">Luke</a>. During query parsing a
 * WhitespaceAnalyzer is used - this choice has been made to minimize side effects of
 * Analyzer on the final set of query terms. You can use {@link org.apache.nutch.searcher.Query#main(String[])}
 * method to translate queries in Nutch syntax to queries in Lucene syntax.<br>
 * If additional level of control is required, an instance of {@link PruneChecker} can
 * be provided to check each document before it's deleted. The results of all
 * checkers are logically AND-ed, which means that any checker in the chain
 * can veto the deletion of the current document. Two example checker implementations
 * are provided - PrintFieldsChecker prints the values of selected index fields,
 * StoreUrlsChecker stores the URLs of deleted documents to a file. Any of them can
 * be activated by providing respective command-line options.
 * </p>
 * <p>The typical command-line usage is as follows:<br>
 * <blockquote>
 * <code>PruneIndexTool index_dir -dryrun -queries queries.txt -showfields url,title</code><br>
 * This command will just print out fields of matching documents.<br>
 * <code>PruneIndexTool index_dir -queries queries.txt</code><br>
 * This command will actually remove all matching entries, according to the
 * queries read from <code>queries.txt</code> file.
 * </blockquote></p>
 * <p>NOTE 2: This tool removes matching documents ONLY from segment indexes (or
 * from a merged index). In particular it does NOT remove the pages and links
 * from WebDB. This means that unwanted URLs may pop up again when new segments
 * are created. To prevent this, use your own {@link org.apache.nutch.net.URLFilter},
 * or PruneDBTool (under construction...).</p>
 * <p>NOTE 3: This tool uses a low-level Lucene interface to collect all matching
 * documents. For large indexes and broad queries this may result in high memory
 * consumption. If you encounter OutOfMemory exceptions, try to narrow down your
 * queries, or increase the heap size.</p>
 * 
 * @author Andrzej Bialecki &lt;ab@getopt.org&gt;
 */
public class PruneIndexTool implements Runnable {
  public static final Log LOG = LogFactory.getLog(PruneIndexTool.class);
  /** Log the progress every LOG_STEP number of processed documents. */
  public static int LOG_STEP = 50000;
  
  /**
   * This interface can be used to implement additional checking on matching
   * documents.
   * @author Andrzej Bialecki &lt;ab@getopt.org&gt;
   */
  public static interface PruneChecker {
    /**
     * Check whether this document should be pruned. NOTE: this method
     * MUST NOT modify the IndexReader.
     * @param reader index reader to read documents from
     * @param docNum document ID
     * @return true if the document should be deleted, false otherwise.
     */
    public boolean isPrunable(Query q, IndexReader reader, int docNum) throws Exception;
    /**
     * Close the checker - this could involve flushing output files or somesuch.
     */
    public void close();
  }

  /**
   * This checker's main function is just to print out
   * selected field values from each document, just before
   * they are deleted.
   * 
   * @author Andrzej Bialecki &lt;ab@getopt.org&gt;
   */
  public static class PrintFieldsChecker implements PruneChecker {
    private PrintStream ps = null;
    private String[] fields = null;
    
    /**
     * 
     * @param ps an instance of PrintStream to print the information to
     * @param fields a list of Lucene index field names. Values from these
     * fields will be printed for every matching document.
     */
    public PrintFieldsChecker(PrintStream ps, String[] fields) {
      this.ps = ps;
      this.fields = fields;
    }

    public void close() {
      ps.flush();
    }
    
    public boolean isPrunable(Query q, IndexReader reader, int docNum) throws Exception {
      Document doc = reader.document(docNum);
      StringBuffer sb = new StringBuffer("#" + docNum + ":");
      for (int i = 0; i < fields.length; i++) {
        String[] values = doc.getValues(fields[i]);
        sb.append(" " + fields[i] + "=");
        if (values != null) {
          for (int k = 0; k < values.length; k++) {
            sb.append("[" + values[k] + "]");
          }
        } else sb.append("[null]");
      }
      ps.println(sb.toString());
      return true;
    }
  }

  /**
   * This checker's main function is just to store
   * the URLs of each document to be deleted in a text file.
   * 
   * @author Andrzej Bialecki &lt;ab@getopt.org&gt;
   */
  public static class StoreUrlsChecker implements PruneChecker {
    private BufferedWriter output = null;
    private boolean storeHomeUrl = false;
    
    /**
     * Store the list in a file
     * @param out name of the output file
     */
    public StoreUrlsChecker(File out, boolean storeHomeUrl) throws Exception {
      this.output = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(out), "UTF-8"));
      this.storeHomeUrl = storeHomeUrl;
    }
    
    public void close() {
      try {
        output.flush();
        output.close();
      } catch (Exception e) {
        if (LOG.isWarnEnabled()) {
          LOG.warn("Error closing: " + e.getMessage());
        }
      }
    }
    
    public boolean isPrunable(Query q, IndexReader reader, int docNum) throws Exception {
      Document doc = reader.document(docNum);
      String url = doc.get("url");
      output.write(url); output.write('\n');
      if (storeHomeUrl) {
        // store also the main url
        int idx = url.indexOf("://");
        if (idx != -1) {
          idx = url.indexOf('/', idx + 3);
          if (idx != -1) {
            output.write(url.substring(0, idx + 1) + "\n");
          }
        }
      }
      return true;
    }
  }

  private Query[] queries = null;
  private IndexReader reader = null;
  private IndexSearcher searcher = null;
  private PruneChecker[] checkers = null;
  private boolean dryrun = false;
  private String dr = "";
  
  /**
   * Create an instance of the tool, and open all input indexes.
   * @param indexDirs directories with input indexes. At least one valid index must
   * exist, otherwise an Exception is thrown.
   * @param queries pruning queries. Each query will be processed in turn, and the
   * length of the array must be at least one, otherwise an Exception is thrown.
   * @param checkers if not null, they will be used to perform additional
   * checks on matching documents - each checker's method {@link PruneChecker#isPrunable(Query, IndexReader, int)}
   * will be called in turn, for each matching document, and if it returns true this means that
   * the document should be deleted. A logical AND is performed on the results returned
   * by all checkers (which means that if one of them returns false, the document will
   * not be deleted).
   * @param unlock if true, and if any of the input indexes is locked, forcibly
   * unlock it. Use with care, only when you are sure that other processes don't
   * modify the index at the same time.
   * @param dryrun if set to true, don't change the index, just show what would be done.
   * If false, perform all actions, changing indexes as needed. Note: dryrun doesn't prevent
   * PruneCheckers from performing changes or causing any other side-effects.
   * @throws Exception
   */
  public PruneIndexTool(File[] indexDirs, Query[] queries, PruneChecker[] checkers,
          boolean unlock, boolean dryrun) throws Exception {
    if (indexDirs == null || queries == null)
      throw new Exception("Invalid arguments.");
    if (indexDirs.length == 0 || queries.length == 0)
      throw new Exception("Nothing to do.");
    this.queries = queries;
    this.checkers = checkers;
    this.dryrun = dryrun;
    if (dryrun) dr = "[DRY RUN] ";
    int numIdx = 0;
    if (indexDirs.length == 1) {
      Directory dir = FSDirectory.getDirectory(indexDirs[0], false);
      if (IndexReader.isLocked(dir)) {
        if (!unlock) {
          throw new Exception("Index " + indexDirs[0] + " is locked.");
        }
        if (!dryrun) {
          IndexReader.unlock(dir);
          if (LOG.isDebugEnabled()) {
            LOG.debug(" - had to unlock index in " + dir);
          }
        }
      }
      reader = IndexReader.open(dir);
      numIdx = 1;
    } else {
      Directory dir;
      Vector<IndexReader> indexes = new Vector<IndexReader>(indexDirs.length);
      for (int i = 0; i < indexDirs.length; i++) {
        try {
          dir = FSDirectory.getDirectory(indexDirs[i], false);
          if (IndexReader.isLocked(dir)) {
            if (!unlock) {
              if (LOG.isWarnEnabled()) {
                LOG.warn(dr + "Index " + indexDirs[i] + " is locked. Skipping...");
              }
              continue;
            }
            if (!dryrun) {
              IndexReader.unlock(dir);
              if (LOG.isDebugEnabled()) {
                LOG.debug(" - had to unlock index in " + dir);
              }
            }
          }
          IndexReader r = IndexReader.open(dir);
          indexes.add(r);
          numIdx++;
        } catch (Exception e) {
          if (LOG.isWarnEnabled()) {
            LOG.warn(dr + "Invalid index in " + indexDirs[i] + " - skipping...");
          }
        }
      }
      if (indexes.size() == 0) throw new Exception("No input indexes.");
      IndexReader[] readers = indexes.toArray(new IndexReader[0]);
      reader = new MultiReader(readers);
    }
    if (LOG.isInfoEnabled()) {
      LOG.info(dr + "Opened " + numIdx + " index(es) with total " +
               reader.numDocs() + " documents.");
    }
    searcher = new IndexSearcher(reader);
  }
  
  /**
   * This class collects all matching document IDs in a BitSet.
   * <p>NOTE: the reason to use this API is that the most common way of
   * performing Lucene queries (Searcher.search(Query)::Hits) does NOT
   * return all matching documents, because it skips very low scoring hits.</p>
   * 
   * @author Andrzej Bialecki &lt;ab@getopt.org&gt;
   */
  private static class AllHitsCollector extends HitCollector {
    private BitSet bits;
    
    public AllHitsCollector(BitSet bits) {
      this.bits = bits;
    }
    public void collect(int doc, float score) {
      bits.set(doc);
    }
  }
  
  /**
   * For each query, find all matching documents and delete them from all input
   * indexes. Optionally, an additional check can be performed by using {@link PruneChecker}
   * implementations.
   */
  public void run() {
    BitSet bits = new BitSet(reader.maxDoc());
    AllHitsCollector ahc = new AllHitsCollector(bits);
    boolean doDelete = false;
    for (int i = 0; i < queries.length; i++) {
      if (LOG.isInfoEnabled()) {
        LOG.info(dr + "Processing query: " + queries[i].toString());
      }
      bits.clear();
      try {
        searcher.search(queries[i], ahc);
      } catch (IOException e) {
        if (LOG.isWarnEnabled()) {
          LOG.warn(dr + " - failed: " + e.getMessage());
        }
        continue;
      }
      if (bits.cardinality() == 0) {
        if (LOG.isInfoEnabled()) {
          LOG.info(dr + " - no matching documents.");
        }
        continue;
      }
      if (LOG.isInfoEnabled()) {
        LOG.info(dr + " - found " + bits.cardinality() + " document(s).");
      }
      // Now delete all matching documents
      int docNum = -1, start = 0, cnt = 0;
      // probably faster than looping sequentially through all index values?
      while ((docNum = bits.nextSetBit(start)) != -1) {
        // don't delete the same document multiple times
        if (reader.isDeleted(docNum)) continue;
        try {
          if (checkers != null && checkers.length > 0) {
            boolean check = true;
            for (int k = 0; k < checkers.length; k++) {
              // fail if any checker returns false
              check &= checkers[k].isPrunable(queries[i], reader, docNum);
            }
            doDelete = check;
          } else doDelete = true;
          if (doDelete) {
            if (!dryrun) reader.deleteDocument(docNum);
            cnt++;
          }
        } catch (Exception e) {
          if (LOG.isWarnEnabled()) {
            LOG.warn(dr + " - failed to delete doc #" + docNum);
          }
        }
        start = docNum + 1;
      }
      if (LOG.isInfoEnabled()) {
        LOG.info(dr + " - deleted " + cnt + " document(s).");
      }
    }
    // close checkers
    if (checkers != null) {
      for (int i = 0; i < checkers.length; i++) {
        checkers[i].close();
      }
    }
    try {
      reader.close();
    } catch (IOException e) {
      if (LOG.isWarnEnabled()) {
        LOG.warn(dr + "Exception when closing reader(s): " + e.getMessage());
      }
    }
  }
  
  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      usage();
      if (LOG.isFatalEnabled()) { LOG.fatal("Missing arguments"); }
      return;
    }
    File idx = new File(args[0]);
    if (!idx.isDirectory()) {
      usage();
      if (LOG.isFatalEnabled()) { LOG.fatal("Not a directory: " + idx); }
      return;
    }
    Vector<File> paths = new Vector<File>();
    if (IndexReader.indexExists(idx)) {
      paths.add(idx);
    } else {
      // try and see if there are segments inside, with index dirs
      File[] dirs = idx.listFiles(new FileFilter() {
        public boolean accept(File f) {
          return f.isDirectory();
        }
      });
      if (dirs == null || dirs.length == 0) {
        usage();
        if (LOG.isFatalEnabled()) { LOG.fatal("No indexes in " + idx); }
        return;
      }
      for (int i = 0; i < dirs.length; i++) {
        File sidx = new File(dirs[i], "index");
        if (sidx.exists() && sidx.isDirectory() && IndexReader.indexExists(sidx)) {
          paths.add(sidx);
        }
      }
      if (paths.size() == 0) {
        usage();
        if (LOG.isFatalEnabled()) {
          LOG.fatal("No indexes in " + idx + " or its subdirs.");
        }
        return;
      }
    }
    File[] indexes = paths.toArray(new File[0]);
    boolean force = false;
    boolean dryrun = false;
    String qPath = null;
    String outPath = null;
    String fList = null;
    for (int i = 1; i < args.length; i++) {
      if (args[i].equals("-force")) {
        force = true;
      } else if (args[i].equals("-queries")) {
        qPath = args[++i];
      } else if (args[i].equals("-output")) {
        outPath = args[++i];
      } else if (args[i].equals("-showfields")) {
        fList = args[++i];
      } else if (args[i].equals("-dryrun")) {
        dryrun = true;
      } else {
        usage();
        if (LOG.isFatalEnabled()) {
          LOG.fatal("Unrecognized option: " + args[i]);
        }
        return;
      }
    }
    Vector<PruneChecker> cv = new Vector<PruneChecker>();
    if (fList != null) {
      StringTokenizer st = new StringTokenizer(fList, ",");
      Vector<String> tokens = new Vector<String>();
      while (st.hasMoreTokens()) tokens.add(st.nextToken());
      String[] fields = tokens.toArray(new String[0]);
      PruneChecker pc = new PrintFieldsChecker(System.out, fields);
      cv.add(pc);
    }
    
    if (outPath != null) {
      StoreUrlsChecker luc = new StoreUrlsChecker(new File(outPath), false);
      cv.add(luc);
    }

    PruneChecker[] checkers = null;
    if (cv.size() > 0) {
      checkers = cv.toArray(new PruneChecker[0]);
    }
    Query[] queries = null;
    InputStream is = null;
    if (qPath != null) {
      is = new FileInputStream(qPath);
    } else {
        Configuration conf = NutchConfiguration.create();
        qPath = conf.get("prune.index.tool.queries");
        is = conf.getConfResourceAsInputStream(qPath);
    }
    if (is == null) {
      if (LOG.isFatalEnabled()) {
        LOG.fatal("Can't load queries from " + qPath);
      }
      return;
    }
    try {
      queries = parseQueries(is);
    } catch (Exception e) {
      if (LOG.isFatalEnabled()) {
        LOG.fatal("Error parsing queries: " + e.getMessage());
      }
      return;
    }
    try {
      PruneIndexTool pit = new PruneIndexTool(indexes, queries, checkers, force, dryrun);
      pit.run();
    } catch (Exception e) {
      if (LOG.isFatalEnabled()) {
        LOG.fatal("Error running PruneIndexTool: " + e.getMessage());
      }
      return;
    }
  }
  
  /**
   * Read a list of Lucene queries from the stream (UTF-8 encoding is assumed).
   * There should be a single Lucene query per line. Blank lines and comments
   * starting with '#' are allowed.
   * <p>NOTE: you may wish to use {@link org.apache.nutch.searcher.Query#main(String[])}
   * method to translate queries from Nutch format to Lucene format.</p>
   * @param is InputStream to read from
   * @return array of Lucene queries
   * @throws Exception
   */
  public static Query[] parseQueries(InputStream is) throws Exception {
    BufferedReader br = new BufferedReader(new InputStreamReader(is, "UTF-8"));
    String line = null;
    QueryParser qp = new QueryParser("url", new WhitespaceAnalyzer());
    Vector<Query> queries = new Vector<Query>();
    while ((line = br.readLine()) != null) {
      line = line.trim();
      //skip blanks and comments
      if (line.length() == 0 || line.charAt(0) == '#') continue;
      Query q = qp.parse(line);
      queries.add(q);
    }
    return queries.toArray(new Query[0]);
  }
  
  private static void usage() {
    System.err.println("PruneIndexTool <indexDir | segmentsDir> [-dryrun] [-force] [-queries filename] [-output filename] [-showfields field1,field2,field3...]");
    System.err.println("\tNOTE: exactly one of <indexDir> or <segmentsDir> MUST be provided!\n");
    System.err.println("\t-dryrun\t\t\tdon't do anything, just show what would be done.");
    System.err.println("\t-force\t\t\tforce index unlock, if locked. Use with caution!");
    System.err.println("\t-queries filename\tread pruning queries from this file, instead of the");
    System.err.println("\t\t\t\tdefault defined in Nutch config files under 'prune.index.tool.queries' key.\n");
    System.err.println("\t-output filename\tstore pruned URLs in a text file");
    System.err.println("\t-showfields field1,field2...\tfor each deleted document show the values of the selected fields.");
    System.err.println("\t\t\t\tNOTE 1: this will slow down processing by orders of magnitude.");
    System.err.println("\t\t\t\tNOTE 2: only values of stored fields will be shown.");
  }
}
