/**
 * Copyright 2006 The Apache Software Foundation
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

package org.apache.nutch.segment;

import java.io.IOException;
import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Generator;
import org.apache.nutch.fetcher.Fetcher;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseText;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.NutchConfiguration;

/**
 * This tool takes several segments and merges their data together. Only the
 * latest versions of data is retained.
 * <p>
 * Optionally, you can apply current URLFilters to remove prohibited URL-s.
 * </p>
 * <p>
 * Also, it's possible to slice the resulting segment into chunks of fixed size.
 * </p>
 * <h3>Important Notes</h3>
 * <h4>Which parts are merged?</h4>
 * <p>It doesn't make sense to merge data from segments, which are at different stages
 * of processing (e.g. one unfetched segment, one fetched but not parsed, and
 * one fetched and parsed). Therefore, prior to merging, the tool will determine
 * the lowest common set of input data, and only this data will be merged.
 * This may have some unintended consequences:
 * e.g. if majority of input segments are fetched and parsed, but one of them is unfetched,
 * the tool will fall back to just merging fetchlists, and it will skip all other data
 * from all segments.</p>
 * <h4>Merging fetchlists</h4>
 * <p>Merging segments, which contain just fetchlists (i.e. prior to fetching)
 * is not recommended, because this tool (unlike the {@link org.apache.nutch.crawl.Generator}
 * doesn't ensure that fetchlist parts for each map task are disjoint.</p>
 * <p>
 * <h4>Duplicate content</h4>
 * Merging segments removes older content whenever possible (see below). However,
 * this is NOT the same as de-duplication, which in addition removes identical
 * content found at different URL-s. In other words, running DeleteDuplicates is
 * still necessary.
 * </p>
 * <p>For some types of data (especially ParseText) it's not possible to determine
 * which version is really older. Therefore the tool always uses segment names as
 * timestamps, for all types of input data. Segment names are compared in forward lexicographic
 * order (0-9a-zA-Z), and data from segments with "higher" names will prevail.
 * It follows then that it is extremely important that segments be named in an
 * increasing lexicographic order as their creation time increases.</p>
 * <p>
 * <h4>Merging and indexes</h4>
 * Merged segment gets a different name. Since Indexer embeds segment names in
 * indexes, any indexes originally created for the input segments will NOT work with the
 * merged segment. Newly created merged segment(s) need to be indexed afresh.
 * This tool doesn't use existing indexes in any way, so if
 * you plan to merge segments you don't have to index them prior to merging.
 * 
 * 
 * @author Andrzej Bialecki
 */
public class SegmentMerger extends Configured implements Mapper, Reducer {
  private static final Log LOG = LogFactory.getLog(SegmentMerger.class);

  private static final UTF8 SEGMENT_PART_KEY = new UTF8("_PaRt_");
  private static final UTF8 SEGMENT_NAME_KEY = new UTF8("_NaMe_");
  private static final String nameMarker = SEGMENT_NAME_KEY.toString();
  private static final UTF8 SEGMENT_SLICE_KEY = new UTF8("_SlIcE_");
  private static final String sliceMarker = SEGMENT_SLICE_KEY.toString();

  private URLFilters filters = null;
  private long sliceSize = -1;
  private long curCount = 0;
  
  /**
   * Wraps inputs in an {@link ObjectWritable}, to permit merging different
   * types in reduce.
   */
  public static class ObjectInputFormat extends SequenceFileInputFormat {
    public RecordReader getRecordReader(FileSystem fs, FileSplit split, JobConf job, Reporter reporter)
            throws IOException {

      reporter.setStatus(split.toString());
      // find part name
      String dir = split.getPath().toString().replace('\\', '/');
      int idx = dir.lastIndexOf("/part-");
      if (idx == -1) {
        throw new IOException("Cannot determine segment part: " + dir);
      }
      dir = dir.substring(0, idx);
      idx = dir.lastIndexOf('/');
      if (idx == -1) {
        throw new IOException("Cannot determine segment part: " + dir);
      }
      final String part = dir.substring(idx + 1);
      // find segment name
      dir = dir.substring(0, idx);
      idx = dir.lastIndexOf('/');
      if (idx == -1) {
        throw new IOException("Cannot determine segment name: " + dir);
      }
      final String segment = dir.substring(idx + 1);

      return new SequenceFileRecordReader(job, split) {
        public synchronized boolean next(Writable key, Writable value) throws IOException {
          ObjectWritable wrapper = (ObjectWritable) value;
          try {
            wrapper.set(getValueClass().newInstance());
          } catch (Exception e) {
            throw new IOException(e.toString());
          }
          boolean res = super.next(key, (Writable) wrapper.get());
          Object o = wrapper.get();
          if (o instanceof CrawlDatum) {
            // record which part of segment this comes from
            ((CrawlDatum)o).getMetaData().put(SEGMENT_PART_KEY, new UTF8(part));
            ((CrawlDatum)o).getMetaData().put(SEGMENT_NAME_KEY, new UTF8(segment));
          } else if (o instanceof Content) {
            if (((Content)o).getMetadata() == null) {
              ((Content)o).setMetadata(new Metadata());
            }
            ((Content)o).getMetadata().set(SEGMENT_NAME_KEY.toString(), segment);
          } else if (o instanceof ParseData) {
            if (((ParseData)o).getParseMeta() == null) {
              ((ParseData)o).setParseMeta(new Metadata());
            }
            ((ParseData)o).getParseMeta().set(SEGMENT_NAME_KEY.toString(), segment);
          } else if (o instanceof ParseText) {
            String text = ((ParseText)o).getText();
            o = new ParseText(SEGMENT_NAME_KEY.toString() +
                    segment + SEGMENT_NAME_KEY.toString() + text);
            wrapper.set(o);
          } else {
            throw new IOException("Unknown value type: " + o.getClass().getName() + "(" + o + ")");
          }
          return res;
        }
      };
    }
  }

  public static class SegmentOutputFormat extends org.apache.hadoop.mapred.OutputFormatBase {
    private static final String DEFAULT_SLICE = "default";
    
    public RecordWriter getRecordWriter(final FileSystem fs, final JobConf job, final String name) throws IOException {
      return new RecordWriter() {
        MapFile.Writer c_out = null;
        MapFile.Writer f_out = null;
        MapFile.Writer pd_out = null;
        MapFile.Writer pt_out = null;
        SequenceFile.Writer g_out = null;
        SequenceFile.Writer p_out = null;
        HashMap sliceWriters = new HashMap();
        String segmentName = job.get("segment.merger.segmentName");
        
        public void write(WritableComparable key, Writable value) throws IOException {
          // unwrap
          Writable o = (Writable)((ObjectWritable)value).get();
          String slice = null;
          if (o instanceof CrawlDatum) {
            // check which output dir it should go into
            UTF8 part = (UTF8)((CrawlDatum)o).getMetaData().get(SEGMENT_PART_KEY);
            ((CrawlDatum)o).getMetaData().remove(SEGMENT_PART_KEY);
            ((CrawlDatum)o).getMetaData().remove(SEGMENT_NAME_KEY);
            if (part == null)
              throw new IOException("Null segment part, key=" + key);
            UTF8 uSlice = (UTF8)((CrawlDatum)o).getMetaData().get(SEGMENT_SLICE_KEY);
            ((CrawlDatum)o).getMetaData().remove(SEGMENT_SLICE_KEY);
            if (uSlice != null) slice = uSlice.toString();
            String partString = part.toString();
            if (partString.equals(CrawlDatum.GENERATE_DIR_NAME)) {
              g_out = ensureSequenceFile(slice, CrawlDatum.GENERATE_DIR_NAME);
              g_out.append(key, o);
            } else if (partString.equals(CrawlDatum.FETCH_DIR_NAME)) {
              f_out = ensureMapFile(slice, CrawlDatum.FETCH_DIR_NAME, CrawlDatum.class);
              f_out.append(key, o);
            } else if (partString.equals(CrawlDatum.PARSE_DIR_NAME)) {
              p_out = ensureSequenceFile(slice, CrawlDatum.PARSE_DIR_NAME);
              p_out.append(key, o);
            } else {
              throw new IOException("Cannot determine segment part: " + partString);
            }
          } else if (o instanceof Content) {
            slice = ((Content)o).getMetadata().get(sliceMarker);
            ((Content)o).getMetadata().remove(sliceMarker);
            ((Content)o).getMetadata().remove(nameMarker);
            // update the segment name inside metadata
            if (slice == null) {
              ((Content)o).getMetadata().set(Fetcher.SEGMENT_NAME_KEY, segmentName);
            } else {
              ((Content)o).getMetadata().set(Fetcher.SEGMENT_NAME_KEY, segmentName + "-" + slice);
            }
            c_out = ensureMapFile(slice, Content.DIR_NAME, Content.class);
            c_out.append(key, o);
          } else if (o instanceof ParseData) {
            slice = ((ParseData)o).getParseMeta().get(sliceMarker);
            ((ParseData)o).getParseMeta().remove(sliceMarker);
            ((ParseData)o).getParseMeta().remove(nameMarker);
            // update the segment name inside contentMeta - required by Indexer
            if (slice == null) {
              ((ParseData)o).getContentMeta().set(Fetcher.SEGMENT_NAME_KEY, segmentName);
            } else {
              ((ParseData)o).getContentMeta().set(Fetcher.SEGMENT_NAME_KEY, segmentName + "-" + slice);
            }
            pd_out = ensureMapFile(slice, ParseData.DIR_NAME, ParseData.class);
            pd_out.append(key, o);
          } else if (o instanceof ParseText) {
            String text = ((ParseText)o).getText();
            if (text != null) {
              // get slice name, and remove it from the text
              if (text.startsWith(sliceMarker)) {
                int idx = text.indexOf(sliceMarker, sliceMarker.length());
                if (idx != -1) {
                  slice = text.substring(sliceMarker.length(), idx);
                  text = text.substring(idx + sliceMarker.length());
                }
              }
              // get segment name, and remove it from the text
              if (text.startsWith(nameMarker)) {
                int idx = text.indexOf(nameMarker, nameMarker.length());
                if (idx != -1) {
                  text = text.substring(idx + nameMarker.length());
                }
              }
              o = new ParseText(text);
            }
            pt_out = ensureMapFile(slice, ParseText.DIR_NAME, ParseText.class);
            pt_out.append(key, o);
          }
        }
        
        // lazily create SequenceFile-s.
        private SequenceFile.Writer ensureSequenceFile(String slice, String dirName) throws IOException {
          if (slice == null) slice = DEFAULT_SLICE;
          SequenceFile.Writer res = (SequenceFile.Writer)sliceWriters.get(slice + dirName);
          if (res != null) return res;
          Path wname;
          if (slice == DEFAULT_SLICE) {
            wname = new Path(new Path(new Path(job.getOutputPath(), segmentName), dirName), name);
          } else {
            wname = new Path(new Path(new Path(job.getOutputPath(), segmentName + "-" + slice), dirName), name);
          }
          res = new SequenceFile.Writer(fs, wname, UTF8.class, CrawlDatum.class);
          sliceWriters.put(slice + dirName, res);
          return res;
        }

        // lazily create MapFile-s.
        private MapFile.Writer ensureMapFile(String slice, String dirName, Class clazz) throws IOException {
          if (slice == null) slice = DEFAULT_SLICE;
          MapFile.Writer res = (MapFile.Writer)sliceWriters.get(slice + dirName);
          if (res != null) return res;
          Path wname;
          if (slice == DEFAULT_SLICE) {
            wname = new Path(new Path(new Path(job.getOutputPath(), segmentName), dirName), name);
          } else {
            wname = new Path(new Path(new Path(job.getOutputPath(), segmentName + "-" + slice), dirName), name);
          }
          res = new MapFile.Writer(fs, wname.toString(), UTF8.class, clazz);
          sliceWriters.put(slice + dirName, res);
          return res;
        }

        public void close(Reporter reporter) throws IOException {
          Iterator it = sliceWriters.values().iterator();
          while (it.hasNext()) {
            Object o = it.next();
            if (o instanceof SequenceFile.Writer) {
              ((SequenceFile.Writer)o).close();
            } else {
              ((MapFile.Writer)o).close();
            }
          }
        }
      };
    }
  }

  public SegmentMerger() {
    super(null);
  }
  
  public SegmentMerger(Configuration conf) {
    super(conf);
  }
  
  public void setConf(Configuration conf) {
    super.setConf(conf);
    if (conf == null) return;
    if (conf.getBoolean("segment.merger.filter", false))
      filters = new URLFilters(conf);
    sliceSize = conf.getLong("segment.merger.slice", -1);
    if ((sliceSize > 0) && (LOG.isInfoEnabled())) {
      LOG.info("Slice size: " + sliceSize + " URLs.");
    }
  }

  public void close() throws IOException {
  }

  public void configure(JobConf conf) {
    setConf(conf);
    if (sliceSize > 0) {
      sliceSize = sliceSize / conf.getNumReduceTasks();
    }
  }
  
  public void map(WritableComparable key, Writable value, OutputCollector output, Reporter reporter) throws IOException {
    if (filters != null) {
      try {
        if (filters.filter(((UTF8)key).toString()) == null) {
          return;
        }
      } catch (Exception e) {
        if (LOG.isWarnEnabled()) {
          LOG.warn("Cannot filter key " + key + ": " + e.getMessage());
        }
      }
    }
    output.collect(key, value);
  }

  /**
   * NOTE: in selecting the latest version we rely exclusively on the segment
   * name (not all segment data contain time information). Therefore it is extremely
   * important that segments be named in an increasing lexicographic order as
   * their creation time increases.
   */
  public void reduce(WritableComparable key, Iterator values, OutputCollector output, Reporter reporter) throws IOException {
    CrawlDatum lastG = null;
    CrawlDatum lastF = null;
    CrawlDatum lastSig = null;
    Content lastC = null;
    ParseData lastPD = null;
    ParseText lastPT = null;
    String lastGname = null;
    String lastFname = null;
    String lastSigname = null;
    String lastCname = null;
    String lastPDname = null;
    String lastPTname = null;
    TreeMap linked = new TreeMap();
    while (values.hasNext()) {
      ObjectWritable wrapper = (ObjectWritable)values.next();
      Object o = wrapper.get();
      if (o instanceof CrawlDatum) {
        CrawlDatum val = (CrawlDatum)o;
        // check which output dir it belongs to
        UTF8 part = (UTF8)val.getMetaData().get(SEGMENT_PART_KEY);
        if (part == null)
          throw new IOException("Null segment part, key=" + key);
        UTF8 uName = (UTF8)val.getMetaData().get(SEGMENT_NAME_KEY);
        if (uName == null)
          throw new IOException("Null segment name, key=" + key);
        String name = uName.toString();
        String partString = part.toString();
        if (partString.equals(CrawlDatum.GENERATE_DIR_NAME)) {
          if (lastG == null) {
            lastG = val;
            lastGname = name;
          } else {
            // take newer
            if (lastGname.compareTo(name) < 0) {
              lastG = val;
              lastGname = name;
            }
          }
        } else if (partString.equals(CrawlDatum.FETCH_DIR_NAME)) {
          if (lastF == null) {
            lastF = val;
            lastFname = name;
          } else {
            // take newer
            if (lastFname.compareTo(name) < 0) {
              lastF = val;
              lastFname = name;
            }
          }
        } else if (partString.equals(CrawlDatum.PARSE_DIR_NAME)) {
          if (val.getStatus() == CrawlDatum.STATUS_SIGNATURE) {
            if (lastSig == null) {
              lastSig = val;
              lastSigname = name;
            } else {
              // take newer
              if (lastSigname.compareTo(name) < 0) {
                lastSig = val;
                lastSigname = name;
              }
            }
            continue;
          }
          // collect all LINKED values from the latest segment
          ArrayList segLinked = (ArrayList)linked.get(name);
          if (segLinked == null) {
            segLinked = new ArrayList();
            linked.put(name, segLinked);
          }
          segLinked.add(val);
        } else {
          throw new IOException("Cannot determine segment part: " + partString);
        }
      } else if (o instanceof Content) {
        String name = ((Content)o).getMetadata().get(SEGMENT_NAME_KEY.toString());
        if (lastC == null) {
          lastC = (Content)o;
          lastCname = name;
        } else {
          if (lastCname.compareTo(name) < 0) {
            lastC = (Content)o;
            lastCname = name;
          }
        }
      } else if (o instanceof ParseData) {
        String name = ((ParseData)o).getParseMeta().get(SEGMENT_NAME_KEY.toString());
        if (lastPD == null) {
          lastPD = (ParseData)o;
          lastPDname = name;
        } else {
          if (lastPDname.compareTo(name) < 0) {
            lastPD = (ParseData)o;
            lastPDname = name;
          }
        }
      } else if (o instanceof ParseText) {
        String text = ((ParseText)o).getText();
        String name = null;
        int idx = text.indexOf(nameMarker, nameMarker.length());
        if (idx != -1) {
          name = text.substring(nameMarker.length(), idx);
        } else {
          throw new IOException("Missing segment name marker in ParseText, key " + key + ": " + text);
        }
        if (lastPT == null) {
          lastPT = (ParseText)o;
          lastPTname = name;
        } else {
          if (lastPTname.compareTo(name) < 0) {
            lastPT = (ParseText)o;
            lastPTname = name;
          }
        }
      }
    }
    curCount++;
    UTF8 sliceName = null;
    ObjectWritable wrapper = new ObjectWritable();
    if (sliceSize > 0) {
      sliceName = new UTF8(String.valueOf(curCount / sliceSize));
    }
    // now output the latest values
    if (lastG != null) {
      if (sliceName != null) {
        lastG.getMetaData().put(SEGMENT_SLICE_KEY, sliceName);
      }
      wrapper.set(lastG);
      output.collect(key, wrapper);
    }
    if (lastF != null) {
      if (sliceName != null) {
        lastF.getMetaData().put(SEGMENT_SLICE_KEY, sliceName);
      }
      wrapper.set(lastF);
      output.collect(key, wrapper);
    }
    if (lastSig != null) {
      if (sliceName != null) {
        lastSig.getMetaData().put(SEGMENT_SLICE_KEY, sliceName);
      }
      wrapper.set(lastSig);
      output.collect(key, wrapper);
    }
    if (lastC != null) {
      if (sliceName != null) {
        lastC.getMetadata().set(sliceMarker, sliceName.toString());
      }
      wrapper.set(lastC);
      output.collect(key, wrapper);
    }
    if (lastPD != null) {
      if (sliceName != null) {
        lastPD.getParseMeta().set(sliceMarker, sliceName.toString());
      }
      wrapper.set(lastPD);
      output.collect(key, wrapper);
    }
    if (lastPT != null) {
      if (sliceName != null) {
        lastPT = new ParseText(sliceMarker + sliceName + sliceMarker
                + lastPT.getText());
      }
      wrapper.set(lastPT);
      output.collect(key, wrapper);
    }
    if (linked.size() > 0) {
      String name = (String)linked.lastKey();
      ArrayList segLinked = (ArrayList)linked.get(name);
      for (int i = 0; i < segLinked.size(); i++) {
        CrawlDatum link = (CrawlDatum)segLinked.get(i);
        if (sliceName != null) {
          link.getMetaData().put(SEGMENT_SLICE_KEY, sliceName);
        }
        wrapper.set(link);
        output.collect(key, wrapper);
      }
    }
  }

  public void merge(Path out, Path[] segs, boolean filter, long slice) throws Exception {
    String segmentName = Generator.generateSegmentName();
    if (LOG.isInfoEnabled()) {
      LOG.info("Merging " + segs.length + " segments to " + out + "/" + segmentName);
    }
    JobConf job = new JobConf(getConf());
    job.setJobName("mergesegs " + out + "/" + segmentName);
    job.setBoolean("segment.merger.filter", filter);
    job.setLong("segment.merger.slice", slice);
    job.set("segment.merger.segmentName", segmentName);
    FileSystem fs = FileSystem.get(getConf());
    // prepare the minimal common set of input dirs
    boolean g = true;
    boolean f = true;
    boolean p = true;
    boolean c = true;
    boolean pd = true;
    boolean pt = true;
    for (int i = 0; i < segs.length; i++) {
      if (!fs.exists(segs[i])) {
        if (LOG.isWarnEnabled()) {
          LOG.warn("Input dir " + segs[i] + " doesn't exist, skipping.");
        }
        segs[i] = null;
        continue;
      }
      if (LOG.isInfoEnabled()) {
        LOG.info("SegmentMerger:   adding " + segs[i]);
      }
      Path cDir = new Path(segs[i], Content.DIR_NAME);
      Path gDir = new Path(segs[i], CrawlDatum.GENERATE_DIR_NAME);
      Path fDir = new Path(segs[i], CrawlDatum.FETCH_DIR_NAME);
      Path pDir = new Path(segs[i], CrawlDatum.PARSE_DIR_NAME);
      Path pdDir = new Path(segs[i], ParseData.DIR_NAME);
      Path ptDir = new Path(segs[i], ParseText.DIR_NAME);
      c = c && fs.exists(cDir);
      g = g && fs.exists(gDir);
      f = f && fs.exists(fDir);
      p = p && fs.exists(pDir);
      pd = pd && fs.exists(pdDir);
      pt = pt && fs.exists(ptDir);
    }
    StringBuffer sb = new StringBuffer();
    if (c) sb.append(" " + Content.DIR_NAME);
    if (g) sb.append(" " + CrawlDatum.GENERATE_DIR_NAME);
    if (f) sb.append(" " + CrawlDatum.FETCH_DIR_NAME);
    if (p) sb.append(" " + CrawlDatum.PARSE_DIR_NAME);
    if (pd) sb.append(" " + ParseData.DIR_NAME);
    if (pt) sb.append(" " + ParseText.DIR_NAME);
    if (LOG.isInfoEnabled()) {
      LOG.info("SegmentMerger: using segment data from:" + sb.toString());
    }
    for (int i = 0; i < segs.length; i++) {
      if (segs[i] == null) continue;
      if (g) {
        Path gDir = new Path(segs[i], CrawlDatum.GENERATE_DIR_NAME);
        job.addInputPath(gDir);
      }
      if (c) {
        Path cDir = new Path(segs[i], Content.DIR_NAME);
        job.addInputPath(cDir);
      }
      if (f) {
        Path fDir = new Path(segs[i], CrawlDatum.FETCH_DIR_NAME);
        job.addInputPath(fDir);
      }
      if (p) {
        Path pDir = new Path(segs[i], CrawlDatum.PARSE_DIR_NAME);
        job.addInputPath(pDir);
      }
      if (pd) {
        Path pdDir = new Path(segs[i], ParseData.DIR_NAME);
        job.addInputPath(pdDir);
      }
      if (pt) {
        Path ptDir = new Path(segs[i], ParseText.DIR_NAME);
        job.addInputPath(ptDir);
      }
    }
    job.setInputFormat(ObjectInputFormat.class);
    job.setInputKeyClass(UTF8.class);
    job.setInputValueClass(ObjectWritable.class);
    job.setMapperClass(SegmentMerger.class);
    job.setReducerClass(SegmentMerger.class);
    job.setOutputPath(out);
    job.setOutputKeyClass(UTF8.class);
    job.setOutputValueClass(ObjectWritable.class);
    job.setOutputFormat(SegmentOutputFormat.class);
    
    setConf(job);
    
    JobClient.runJob(job);
  }

  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("SegmentMerger output_dir (-dir segments | seg1 seg2 ...) [-filter] [-slice NNNN]");
      System.err.println("\toutput_dir\tname of the parent dir for output segment slice(s)");
      System.err.println("\t-dir segments\tparent dir containing several segments");
      System.err.println("\tseg1 seg2 ...\tlist of segment dirs");
      System.err.println("\t-filter\t\tfilter out URL-s prohibited by current URLFilters");
      System.err.println("\t-slice NNNN\tcreate many output segments, each containing NNNN URLs");
      return;
    }
    Configuration conf = NutchConfiguration.create();
    final FileSystem fs = FileSystem.get(conf);
    Path out = new Path(args[0]);
    ArrayList segs = new ArrayList();
    long sliceSize = 0;
    boolean filter = false;
    for (int i = 1; i < args.length; i++) {
      if (args[i].equals("-dir")) {
        Path[] files = fs.listPaths(new Path(args[++i]), new PathFilter() {
          public boolean accept(Path f) {
            try {
              if (fs.isDirectory(f)) return true;
            } catch (IOException e) {}
            ;
            return false;
          }
        });
        for (int j = 0; j < files.length; j++)
          segs.add(files[j]);
      } else if (args[i].equals("-filter")) {
        filter = true;
      } else if (args[i].equals("-slice")) {
        sliceSize = Long.parseLong(args[++i]);
      } else {
        segs.add(new Path(args[i]));
      }
    }
    if (segs.size() == 0) {
      System.err.println("ERROR: No input segments.");
      return;
    }
    SegmentMerger merger = new SegmentMerger(conf);
    merger.merge(out, (Path[]) segs.toArray(new Path[segs.size()]), filter, sliceSize);
  }

}
