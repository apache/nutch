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

import java.io.*;
import java.security.*;
import java.text.*;
import java.util.*;
import java.util.logging.*;

import org.apache.nutch.io.*;
import org.apache.nutch.fs.*;
import org.apache.nutch.util.*;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.document.Document;

/******************************************************************
 * Deletes duplicate documents in a set of Lucene indexes.
 * Duplicates have either the same contents (via MD5 hash) or the same URL.
 * 
 * @author Doug Cutting
 * @author Mike Cafarella
 ******************************************************************/
public class DeleteDuplicates {
  private static final Logger LOG =
    LogFormatter.getLogger("org.apache.nutch.indexer.DeleteDuplicates");

  /********************************************************
   * The key used in sorting for duplicates.
   *******************************************************/
  public static class IndexedDoc implements WritableComparable {
    private MD5Hash hash = new MD5Hash();
    private float score;
    private int index;                            // the segment index
    private int doc;                              // within the index
    private int urlLen;

    public void write(DataOutput out) throws IOException {
      hash.write(out);
      out.writeFloat(score);
      out.writeInt(index);
      out.writeInt(doc);
      out.writeInt(urlLen);
    }

    public void readFields(DataInput in) throws IOException {
      hash.readFields(in);
      this.score = in.readFloat();
      this.index = in.readInt();
      this.doc = in.readInt();
      this.urlLen = in.readInt();
    }

    public int compareTo(Object o) {
      throw new RuntimeException("this is never used");
    }

    /** 
     * Order equal hashes by decreasing score and increasing urlLen. 
     */
    public static class ByHashScore extends WritableComparator {
      public ByHashScore() { super(IndexedDoc.class); }
      
      public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2){
        int c = compareBytes(b1, s1, MD5Hash.MD5_LEN, b2, s2, MD5Hash.MD5_LEN);
        if (c != 0)
          return c;

        float thisScore = readFloat(b1, s1+MD5Hash.MD5_LEN);
        float thatScore = readFloat(b2, s2+MD5Hash.MD5_LEN);

        if (thisScore < thatScore)
          return 1;
        else if (thisScore > thatScore)
          return -1;
        
        int thisUrlLen = readInt(b1, s1+MD5Hash.MD5_LEN+12);
        int thatUrlLen = readInt(b2, s2+MD5Hash.MD5_LEN+12);

        return thisUrlLen - thatUrlLen;
      }
    }

    /** 
     * Order equal hashes by decreasing index and document. 
     */
    public static class ByHashDoc extends WritableComparator {
      public ByHashDoc() { super(IndexedDoc.class); }
      
      public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2){
        int c = compareBytes(b1, s1, MD5Hash.MD5_LEN, b2, s2, MD5Hash.MD5_LEN);
        if (c != 0)
          return c;

        int thisIndex = readInt(b1, s1+MD5Hash.MD5_LEN+4);
        int thatIndex = readInt(b2, s2+MD5Hash.MD5_LEN+4);

        if (thisIndex != thatIndex)
          return thatIndex - thisIndex;

        int thisDoc = readInt(b1, s1+MD5Hash.MD5_LEN+8);
        int thatDoc = readInt(b2, s2+MD5Hash.MD5_LEN+8);

        return thatDoc - thisDoc;
      }
    }
  }

  /*****************************************************
   ****************************************************/
  private interface Hasher {
    void updateHash(MD5Hash hash, Document doc);
  }

  //////////////////////////////////////////////////////
  //  DeleteDuplicates class
  //////////////////////////////////////////////////////
  private IndexReader[] readers;
  private File tempFile;

  /** 
   * Constructs a duplicate detector for the provided indexes. 
   */
  public DeleteDuplicates(IndexReader[] readers, File workingDir) throws IOException {
    this.readers = readers;
    this.tempFile = new File(workingDir, "ddup-" + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date(System.currentTimeMillis())));
  }

  /** 
   * Closes the indexes, saving changes. 
   */
  public void close() throws IOException {
    for (int i = 0; i < readers.length; i++) {
        readers[i].close();
    }
    tempFile.delete();
  }

  /** 
   * Delete pages with duplicate content hashes.  Of those with the same
   * content hash, keep the page with the highest score. 
   */
  public void deleteContentDuplicates() throws IOException {
    LOG.info("Reading content hashes...");
    computeHashes(new Hasher() {
        public void updateHash(MD5Hash hash, Document doc) {
          hash.setDigest(doc.get("digest"));
        }
      });

    LOG.info("Sorting content hashes...");
    SequenceFile.Sorter byHashScoreSorter =
      new SequenceFile.Sorter(new LocalFileSystem(), new IndexedDoc.ByHashScore(),NullWritable.class);
    byHashScoreSorter.sort(tempFile.getPath(), tempFile.getPath() + ".sorted");
    
    LOG.info("Deleting content duplicates...");
    int duplicateCount = deleteDuplicates();
    LOG.info("Deleted " + duplicateCount + " content duplicates.");
  }

  /** 
   * Delete pages with duplicate URLs.  Of those with the same
   * URL, keep the most recently fetched page. 
   */
  public void deleteUrlDuplicates() throws IOException {
    final MessageDigest digest;
    try {
      digest = MessageDigest.getInstance("MD5");
    } catch (Exception e) {
      throw new RuntimeException(e.toString());
    }

    LOG.info("Reading url hashes...");
    computeHashes(new Hasher() {
        public void updateHash(MD5Hash hash, Document doc) {
          try {
            digest.update(UTF8.getBytes(doc.get("url")));
            digest.digest(hash.getDigest(), 0, MD5Hash.MD5_LEN);
          } catch (Exception e) {
            throw new RuntimeException(e.toString());
          }
        }
      });

    LOG.info("Sorting url hashes...");
    SequenceFile.Sorter byHashDocSorter =
      new SequenceFile.Sorter(new LocalFileSystem(), new IndexedDoc.ByHashDoc(), NullWritable.class);
    byHashDocSorter.sort(tempFile.getPath(), tempFile.getPath() + ".sorted");
    
    LOG.info("Deleting url duplicates...");
    int duplicateCount = deleteDuplicates();
    LOG.info("Deleted " + duplicateCount + " url duplicates.");
  }

  /**
   * Compute hashes over all the input indices
   */
  private void computeHashes(Hasher hasher) throws IOException {
    IndexedDoc indexedDoc = new IndexedDoc();

    SequenceFile.Writer writer =
      new SequenceFile.Writer(new LocalFileSystem(), tempFile.getPath(), IndexedDoc.class, NullWritable.class);
    try {
      for (int index = 0; index < readers.length; index++) {
        IndexReader reader = readers[index];
        int readerMax = reader.maxDoc();
        indexedDoc.index = index;
        for (int doc = 0; doc < readerMax; doc++) {
          if (!reader.isDeleted(doc)) {
            Document document = reader.document(doc);
            hasher.updateHash(indexedDoc.hash, document);
            indexedDoc.score = Float.parseFloat(document.get("boost"));
            indexedDoc.doc = doc;
            indexedDoc.urlLen = document.get("url").length();
            writer.append(indexedDoc, NullWritable.get());
          }
        }
      }
    } finally {
      writer.close();
    }
  }

  /**
   * Actually remove the duplicates from the indices
   */
  private int deleteDuplicates() throws IOException {
      if (tempFile.exists()) {
          tempFile.delete();
      }
      if (!new File(tempFile.getPath() + ".sorted").renameTo(tempFile)) {
          throw new IOException("Couldn't rename!");
      }

      IndexedDoc indexedDoc = new IndexedDoc();
      SequenceFile.Reader reader = new SequenceFile.Reader(new LocalFileSystem(), tempFile.getPath());
      try {
          int duplicateCount = 0;
          MD5Hash prevHash = null;                    // previous hash
          while (reader.next(indexedDoc, NullWritable.get())) {
              if (prevHash == null) {                   // initialize prevHash
                  prevHash = new MD5Hash();
                  prevHash.set(indexedDoc.hash);
                  continue;
              }
              if (indexedDoc.hash.equals(prevHash)) {   // found a duplicate
                  readers[indexedDoc.index].delete(indexedDoc.doc); // delete it
                  duplicateCount++;
              } else {
                  prevHash.set(indexedDoc.hash);          // reset prevHash
              }
          }
          return duplicateCount;
      } finally {
          reader.close();
          tempFile.delete();
      }
  }

  /** 
   * Delete duplicates in the indexes in the named directory. 
   */
  public static void main(String[] args) throws Exception {
    //
    // Usage, arg checking
    //
    String usage = "DeleteDuplicates (-local | -ndfs <namenode:port>) [-workingdir <workingdir>] <segmentsDir>";
    if (args.length < 2) {
      System.err.println("Usage: " + usage);
      return;
    } 

    NutchFileSystem nfs = NutchFileSystem.parseArgs(args, 0);
    File workingDir = new File(new File("").getCanonicalPath());
    try {
        //
        // Build an array of IndexReaders for all the segments we want to process
        //
        int j = 0;
        if ("-workingdir".equals(args[j])) {
            j++;
            workingDir = new File(new File(args[j++]).getCanonicalPath());
        }
        workingDir = new File(workingDir, "ddup-workingdir");

        String segmentsDir = args[j++];
        File[] directories = nfs.listFiles(new File(segmentsDir));
        Vector vReaders = new Vector();
        Vector putbackList = new Vector();
        int maxDoc = 0;

        for (int i = 0; i < directories.length; i++) {
            //
            // Make sure the index has been completed
            //
            File indexDone = new File(directories[i], IndexSegment.DONE_NAME);
            if (nfs.exists(indexDone) && nfs.isFile(indexDone)) {
                //
                // Make sure the specified segment can be processed locally
                //
                File indexDir = new File(directories[i], "index");
                File tmpDir = new File(workingDir, "ddup-" + new SimpleDateFormat("yyyMMddHHmmss").format(new Date(System.currentTimeMillis())));
                File localIndexDir = nfs.startLocalOutput(indexDir, tmpDir);

                putbackList.add(indexDir);
                putbackList.add(tmpDir);

                //
                // Construct the reader
                //
                IndexReader reader = IndexReader.open(localIndexDir);
                if (reader.hasDeletions()) {
                    LOG.info("Clearing old deletions in " + indexDir + "(" + localIndexDir + ")");
                    reader.undeleteAll();
                }
                maxDoc += reader.maxDoc();
                vReaders.add(reader);
            }
        }

        //
        // Now build the DeleteDuplicates object, and complete
        //
        IndexReader[] readers = new IndexReader[vReaders.size()];
        for(int i = 0; vReaders.size()>0; i++) {
            readers[i] = (IndexReader)vReaders.remove(0);
        }

        if (workingDir.exists()) {
            FileUtil.fullyDelete(workingDir);
        }
        workingDir.mkdirs();
        DeleteDuplicates dd = new DeleteDuplicates(readers, workingDir);
        dd.deleteUrlDuplicates();
        dd.deleteContentDuplicates();
        dd.close();

        //
        // Dups have been deleted.  Now make sure they are placed back to NFS
        //
        LOG.info("Duplicate deletion complete locally.  Now returning to NFS...");
        for (Iterator it = putbackList.iterator(); it.hasNext(); ) {
            File indexDir = (File) it.next();
            File tmpDir = (File) it.next();
            nfs.completeLocalOutput(indexDir, tmpDir);
        }
        LOG.info("DeleteDuplicates complete");
        FileUtil.fullyDelete(workingDir);
    } finally {
        nfs.close();
    }
  }
}
