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

package org.apache.nutch.tools;

import java.util.*;
import java.io.*;
import java.util.logging.*;

import org.apache.nutch.io.*;
import org.apache.nutch.fetcher.*;
import org.apache.nutch.util.*;
import org.apache.nutch.db.*;
import org.apache.nutch.pagedb.*;
import org.apache.nutch.fs.*;

/** Update scores and links in a set of segments from the current information
 * in a web database. */
public class UpdateSegmentsFromDb {
  public static final Logger LOG =
    LogFormatter.getLogger("org.apache.nutch.segment.UpdateSegmentsFromDb");

  private static final String FILE = "segUpdates";
  private static final String UNSORTED = ".unsorted";
  private static final String SORTED = ".sorted";
  
  private NutchFileSystem fs;
  private String dbDir;
  private String segmentsDir;
  private String tempDir;
  private WebDBReader db;
    
  /** Used internally only. */
  public static class SegmentPage implements WritableComparable {
    private UTF8 url;
    private UTF8 segment;
    private int doc;

    public SegmentPage() {
      this.url = new UTF8();
      this.segment = new UTF8();
    }

    public void set(UTF8 url, UTF8 segment, int doc) {
      this.url = url;
      this.segment = segment;
      this.doc = doc;
    }

    public void readFields(DataInput in) throws IOException {
      url.readFields(in);
      segment.readFields(in);
      doc = in.readInt();
    }

    public void write(DataOutput out) throws IOException {
      url.write(out);
      segment.write(out);
      out.writeInt(doc);
    }

    public int compareTo(Object o) {
      return this.url.compareTo(((SegmentPage)o).url);
    }
  }

  /** Used internally only. */
  public static class ByUrlComparator extends WritableComparator {
    public ByUrlComparator() {
      super(SegmentPage.class);
    }
    
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      return compareBytes(b1, s1+2, readUnsignedShort(b1, s1),
                          b2, s2+2, readUnsignedShort(b2, s2));
    }
  }

  /** Used internally only. */
  public static class BySegmentComparator extends WritableComparator {
    public BySegmentComparator() {
      super(SegmentPage.class);
    }
    
    public int compare(WritableComparable wc1, WritableComparable wc2) {
      SegmentPage sp1 = (SegmentPage)wc1;
      SegmentPage sp2 = (SegmentPage)wc2;
      int c = sp1.segment.compareTo(sp2.segment);
      if (c != 0) {
        return c;
      } else {
        return sp1.doc - sp2.doc;
      }
    }

    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      s1 += readUnsignedShort(b1, s1) + 2;        // skip urls
      s2 += readUnsignedShort(b2, s2) + 2;

      int segmentLen1 = readUnsignedShort(b1, s1);
      int segmentLen2 = readUnsignedShort(b2, s2);
      int segmentStart1 = s1+2;
      int segmentStart2 = s2+2;

      int c = compareBytes(b1, segmentStart1, segmentLen1, // compare segment
                           b2, segmentStart2, segmentLen2);
      if (c != 0)
        return c;

      int docStart1 = segmentStart1 + segmentLen1;
      int docStart2 = segmentStart2 + segmentLen2;
      int doc1 = readInt(b1, docStart1);
      int doc2 = readInt(b2, docStart2);

      return doc1 - doc2;                         // compare doc
    }
  }


  /** Used internally only. */
  public static class Update implements Writable {
    private float score;
    private String[] anchors;

    public Update() {}

    public Update(float score, String[] anchors) {
      this.score = score;
      this.anchors = anchors;
    }

    public final void readFields(DataInput in) throws IOException {
      score = in.readFloat();                     // read score
      anchors = new String[in.readInt()];         // read anchors
      for (int i = 0; i < anchors.length; i++) {
        anchors[i] = UTF8.readString(in);
      }
    }

    public final void write(DataOutput out) throws IOException {
      out.writeFloat(score);                        // write score
      out.writeInt(anchors.length);                 // write anchors
      for (int i = 0; i < anchors.length; i++) {
        UTF8.writeString(out, anchors[i]);
      }
    }
  }
    
  /** Updates all segemnts in the named directory from the named db.*/
  public UpdateSegmentsFromDb(NutchFileSystem fs,
                             String dbDir, String segmentsDir, String tempDir)
    throws IOException {
    this.fs = fs;
    this.dbDir = dbDir;
    this.segmentsDir = segmentsDir;
    this.tempDir = tempDir;

    this.db = new WebDBReader(fs, new File(dbDir));
  }

  public void run() throws IOException {
    LOG.info("Updating " + segmentsDir + " from " + dbDir);

    SequenceFile.Writer out =
      new SequenceFile.Writer(fs, new File(tempDir,FILE+UNSORTED).toString(),
                              SegmentPage.class, NullWritable.class);
    File[] segmentDirs = fs.listFiles(new File(segmentsDir));
    for (int i = 0; i < segmentDirs.length; i++) {
      addSegment(out, segmentDirs[i]);
    }

    out.close();

    close();

    LOG.info("Done updating " + segmentsDir + " from " + dbDir);
  }

  private void addSegment(SequenceFile.Writer out, File segmentDir)
    throws IOException {

    LOG.info(" reading " + segmentDir);

    ArrayFile.Reader reader = new ArrayFile.Reader
      (fs, new File(segmentDir, FetcherOutput.DIR_NAME).toString());

    FetcherOutput fo = new FetcherOutput();
    SegmentPage sp = new SegmentPage();

    UTF8 segmentName = new UTF8(segmentDir.getName());
    try {
      while (reader.next(fo) != null) {
        sp.set(fo.getUrl(), segmentName, (int)reader.key());
        out.append(sp, NullWritable.get());
      }
    } catch (EOFException e) {
      LOG.warning("Unexpected EOF in segment " + segmentName.toString() +
                  " at entry #" + reader.key() + ".  Ignoring.");
    } finally {
      reader.close();
    }
  }

  private void close() throws IOException {
    // sort it
    LOG.info("Sorting pages by url...");
    new SequenceFile.Sorter(fs, new ByUrlComparator(), NullWritable.class)
      .sort(new File(tempDir,FILE+UNSORTED).toString(),
            new File(tempDir,FILE+SORTED).toString());

    // remove unsorted version
    new File(tempDir,FILE+UNSORTED).delete();

    // merge with DB, fetching current score and outlinks
    WebDBAnchors dbAnchors = new WebDBAnchors(db);
    SequenceFile.Reader sorted =
      new SequenceFile.Reader(fs, new File(tempDir,FILE+SORTED).toString());

    SequenceFile.Writer unsorted =
      new SequenceFile.Writer(fs, new File(tempDir,FILE+UNSORTED).toString(),
                              SegmentPage.class, Update.class);
      
    LOG.info("Getting updated scores and anchors from db...");
    SegmentPage sp = new SegmentPage();
    while (sorted.next(sp)) {
      Page page = db.getPage(sp.url.toString());
      if (page != null) {
        String[] anchors = dbAnchors.getAnchors(page.getURL());
        unsorted.append(sp, new Update(page.getScore(), anchors));
      }
    }

    sorted.close();
    new File(tempDir,FILE+SORTED).delete();

    unsorted.close();

    LOG.info("Sorting updates by segment...");
    new SequenceFile.Sorter(fs, new BySegmentComparator(), Update.class)
      .sort(new File(tempDir,FILE+UNSORTED).toString(),
            new File(tempDir,FILE+SORTED).toString());
    new File(tempDir,FILE+UNSORTED).delete();

    LOG.info("Updating segments...");
    Update update = new Update();
    UTF8 segment = new UTF8();
    ArrayFile.Reader fetcherIn = null;
    ArrayFile.Writer fetcherOut = null;
    FetcherOutput fo = new FetcherOutput();

    sorted =
      new SequenceFile.Reader(fs, new File(tempDir,FILE+SORTED).toString());

    try {
      while (sorted.next(sp, update)) {

        if (!sp.segment.equals(segment)) {        // new segment name
        
          if (fetcherIn != null) {                // close any old segment
            closeSegment(fetcherIn, fetcherOut, segment.toString());
          }
          segment.set(sp.segment);                // update segment name

          File segmentDir = new File(segmentsDir, segment.toString());

          LOG.info(" updating " + segmentDir);
        
          fetcherIn = new ArrayFile.Reader        // open new segment input
            (fs, new File(segmentDir, FetcherOutput.DIR_NAME).toString());
          fetcherOut = new ArrayFile.Writer       // open new output
            (fs, new File(segmentDir,FetcherOutput.DIR_NAME+".new").toString(),
             FetcherOutput.class);
        }
        
        do {                                      // scan fetcherIn for match

          fetcherIn.next(fo);                     // get next entry

          if (fetcherIn.key() == sp.doc) {        // if it matches
            FetchListEntry fle = fo.getFetchListEntry();
            fle.getPage().setScore(update.score); // update it
            fle.setAnchors(update.anchors);
          }
          fetcherOut.append(fo);                  // copy to new file
          
        } while (fetcherIn.key() < sp.doc);       // until we've matched
      }

      if (fetcherIn != null) {                    // close last segment
        closeSegment(fetcherIn, fetcherOut, segment.toString());
      }
    } finally {
      sorted.close();
      new File(tempDir,FILE+SORTED).delete();
      db.close();
    }
  }

  private void closeSegment(ArrayFile.Reader fetcherIn,
                            ArrayFile.Writer fetcherOut,
                            String segment) throws IOException {
    

    FetcherOutput fo = new FetcherOutput();
    while (fetcherIn.next(fo) != null) {          // copy any remaining
      fetcherOut.append(fo);
    }

    fetcherIn.close();                            // close input
    fetcherOut.close();                           // close output

    File segDir = new File(segmentsDir, segment.toString());
    fs.delete(new File(segDir, FetcherOutput.DIR_NAME));
    fs.rename(new File(segDir, FetcherOutput.DIR_NAME+".new"),
              new File(segDir, FetcherOutput.DIR_NAME));
  }

  public static void main(String[] args) throws Exception {
    String usage = "UpdateSegmentsFromDb (-local <path> | -ndfs <path> <namenode:port>) dbDir segmentsDir tempDir";
    if (args.length < 3) {
      System.out.println("usage:" + usage);
      return;
    }

    String dbDir = args[0];
    String segmentsDir = args[1];
    String tempDir = args[2];

    UpdateSegmentsFromDb updater =
      new UpdateSegmentsFromDb(NutchFileSystem.get(),
                               dbDir, segmentsDir, tempDir);
    updater.run();
                               
  }

}
