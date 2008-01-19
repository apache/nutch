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

package org.apache.nutch.segment;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.OutputFormatBase;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileRecordReader;
import org.apache.hadoop.util.Progressable;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Generator;
import org.apache.nutch.metadata.MetaWrapper;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseText;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.HadoopFSUtil;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;

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

  private static final String SEGMENT_PART_KEY = "part";
  private static final String SEGMENT_SLICE_KEY = "slice";

  private URLFilters filters = null;
  private URLNormalizers normalizers = null;
  private long sliceSize = -1;
  private long curCount = 0;
  
  /**
   * Wraps inputs in an {@link MetaWrapper}, to permit merging different
   * types in reduce and use additional metadata.
   */
  public static class ObjectInputFormat extends SequenceFileInputFormat {
    
    @Override
    public RecordReader getRecordReader(InputSplit split,
        JobConf job, Reporter reporter) {

      reporter.setStatus(split.toString());

      // find part name
      SegmentPart segmentPart;
      final String spString;
      try {
        segmentPart = SegmentPart.get((FileSplit) split);
        spString = segmentPart.toString();
      } catch (IOException e) {
        throw new RuntimeException("Cannot identify segment:", e);
      }

      try {
        return new SequenceFileRecordReader(job, (FileSplit)split) {
          
          @Override
          public synchronized boolean next(WritableComparable key, Writable value) throws IOException {
            LOG.debug("Running OIF.next()");
            
            MetaWrapper wrapper = (MetaWrapper) value;
            try {
              wrapper.set((Writable)getValueClass().newInstance());
            } catch (Exception e) {
              throw new IOException(e.toString());
            }

            boolean res = super.next(key, (Writable) wrapper.get());
            wrapper.setMeta(SEGMENT_PART_KEY, spString);
            return res;
          }
          
          @Override
          public Writable createValue() {
            return new MetaWrapper();
          }
          
        };
      } catch (IOException e) {
        throw new RuntimeException("Cannot create RecordReader: ", e);
      }
    }
  }

  public static class SegmentOutputFormat extends OutputFormatBase {
    private static final String DEFAULT_SLICE = "default";
    
    @Override
    public RecordWriter getRecordWriter(final FileSystem fs, final JobConf job, final String name, final Progressable progress) throws IOException {
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
          MetaWrapper wrapper = (MetaWrapper)value;
          SegmentPart sp = SegmentPart.parse(wrapper.getMeta(SEGMENT_PART_KEY));
          Writable o = (Writable)wrapper.get();
          String slice = wrapper.getMeta(SEGMENT_SLICE_KEY);
          if (o instanceof CrawlDatum) {
            if (sp.partName.equals(CrawlDatum.GENERATE_DIR_NAME)) {
              g_out = ensureSequenceFile(slice, CrawlDatum.GENERATE_DIR_NAME);
              g_out.append(key, o);
            } else if (sp.partName.equals(CrawlDatum.FETCH_DIR_NAME)) {
              f_out = ensureMapFile(slice, CrawlDatum.FETCH_DIR_NAME, CrawlDatum.class);
              f_out.append(key, o);
            } else if (sp.partName.equals(CrawlDatum.PARSE_DIR_NAME)) {
              p_out = ensureSequenceFile(slice, CrawlDatum.PARSE_DIR_NAME);
              p_out.append(key, o);
            } else {
              throw new IOException("Cannot determine segment part: " + sp.partName);
            }
          } else if (o instanceof Content) {
            c_out = ensureMapFile(slice, Content.DIR_NAME, Content.class);
            c_out.append(key, o);
          } else if (o instanceof ParseData) {
            // update the segment name inside contentMeta - required by Indexer
            if (slice == null) {
              ((ParseData)o).getContentMeta().set(Nutch.SEGMENT_NAME_KEY, segmentName);
            } else {
              ((ParseData)o).getContentMeta().set(Nutch.SEGMENT_NAME_KEY, segmentName + "-" + slice);
            }
            pd_out = ensureMapFile(slice, ParseData.DIR_NAME, ParseData.class);
            pd_out.append(key, o);
          } else if (o instanceof ParseText) {
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
          res = SequenceFile.createWriter(fs, job, wname, Text.class, 
                                          CrawlDatum.class, 
                                          SequenceFile.getCompressionType(job), progress);
          sliceWriters.put(slice + dirName, res);
          return res;
        }

        // lazily create MapFile-s.
        private MapFile.Writer ensureMapFile(String slice, String dirName, Class<? extends Writable> clazz) throws IOException {
          if (slice == null) slice = DEFAULT_SLICE;
          MapFile.Writer res = (MapFile.Writer)sliceWriters.get(slice + dirName);
          if (res != null) return res;
          Path wname;
          if (slice == DEFAULT_SLICE) {
            wname = new Path(new Path(new Path(job.getOutputPath(), segmentName), dirName), name);
          } else {
            wname = new Path(new Path(new Path(job.getOutputPath(), segmentName + "-" + slice), dirName), name);
          }
          CompressionType compType = SequenceFile.getCompressionType(job);
          if (clazz.isAssignableFrom(ParseText.class)) {
            compType = CompressionType.RECORD;
          }
          res = new MapFile.Writer(job, fs, wname.toString(), Text.class, clazz, compType, progress);
          sliceWriters.put(slice + dirName, res);
          return res;
        }

        public void close(Reporter reporter) throws IOException {
          Iterator<Object> it = sliceWriters.values().iterator();
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
    if (conf.getBoolean("segment.merger.normalizer", false))
      normalizers = new URLNormalizers(conf, URLNormalizers.SCOPE_DEFAULT);
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
  
  private Text newKey = new Text();
  
  public void map(WritableComparable key, Writable value, OutputCollector output, Reporter reporter) throws IOException {
    String url = key.toString();
    if (normalizers != null) {
      try {
        url = normalizers.normalize(url, URLNormalizers.SCOPE_DEFAULT); // normalize the url
      } catch (Exception e) {
        LOG.warn("Skipping " + url + ":" + e.getMessage());
        url = null;
      }
    }
    if (url != null && filters != null) {
      try {
        url = filters.filter(url);
      } catch (Exception e) {
        LOG.warn("Skipping key " + url + ": " + e.getMessage());
        url = null;
      }
    }
    if(url != null) {
      newKey.set(url);
      output.collect(newKey, value);
    }
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
    TreeMap<String, ArrayList<CrawlDatum>> linked =
      new TreeMap<String, ArrayList<CrawlDatum>>();
    while (values.hasNext()) {
      MetaWrapper wrapper = (MetaWrapper)values.next();
      Object o = wrapper.get();
      String spString = wrapper.getMeta(SEGMENT_PART_KEY);
      if (spString == null) {
        throw new IOException("Null segment part, key=" + key);        
      }
      SegmentPart sp = SegmentPart.parse(spString);
      if (o instanceof CrawlDatum) {
        CrawlDatum val = (CrawlDatum)o;
        // check which output dir it belongs to
        if (sp.partName.equals(CrawlDatum.GENERATE_DIR_NAME)) {
          if (lastG == null) {
            lastG = val;
            lastGname = sp.segmentName;
          } else {
            // take newer
            if (lastGname.compareTo(sp.segmentName) < 0) {
              lastG = val;
              lastGname = sp.segmentName;
            }
          }
        } else if (sp.partName.equals(CrawlDatum.FETCH_DIR_NAME)) {
          if (lastF == null) {
            lastF = val;
            lastFname = sp.segmentName;
          } else {
            // take newer
            if (lastFname.compareTo(sp.segmentName) < 0) {
              lastF = val;
              lastFname = sp.segmentName;
            }
          }
        } else if (sp.partName.equals(CrawlDatum.PARSE_DIR_NAME)) {
          if (val.getStatus() == CrawlDatum.STATUS_SIGNATURE) {
            if (lastSig == null) {
              lastSig = val;
              lastSigname = sp.segmentName;
            } else {
              // take newer
              if (lastSigname.compareTo(sp.segmentName) < 0) {
                lastSig = val;
                lastSigname = sp.segmentName;
              }
            }
            continue;
          }
          // collect all LINKED values from the latest segment
          ArrayList<CrawlDatum> segLinked = linked.get(sp.segmentName);
          if (segLinked == null) {
            segLinked = new ArrayList<CrawlDatum>();
            linked.put(sp.segmentName, segLinked);
          }
          segLinked.add(val);
        } else {
          throw new IOException("Cannot determine segment part: " + sp.partName);
        }
      } else if (o instanceof Content) {
        if (lastC == null) {
          lastC = (Content)o;
          lastCname = sp.segmentName;
        } else {
          if (lastCname.compareTo(sp.segmentName) < 0) {
            lastC = (Content)o;
            lastCname = sp.segmentName;
          }
        }
      } else if (o instanceof ParseData) {
        if (lastPD == null) {
          lastPD = (ParseData)o;
          lastPDname = sp.segmentName;
        } else {
          if (lastPDname.compareTo(sp.segmentName) < 0) {
            lastPD = (ParseData)o;
            lastPDname = sp.segmentName;
          }
        }
      } else if (o instanceof ParseText) {
        if (lastPT == null) {
          lastPT = (ParseText)o;
          lastPTname = sp.segmentName;
        } else {
          if (lastPTname.compareTo(sp.segmentName) < 0) {
            lastPT = (ParseText)o;
            lastPTname = sp.segmentName;
          }
        }
      }
    }
    curCount++;
    String sliceName = null;
    MetaWrapper wrapper = new MetaWrapper();
    if (sliceSize > 0) {
      sliceName = String.valueOf(curCount / sliceSize);
      wrapper.setMeta(SEGMENT_SLICE_KEY, sliceName);
    }
    SegmentPart sp = new SegmentPart();
    // now output the latest values
    if (lastG != null) {
      wrapper.set(lastG);
      sp.partName = CrawlDatum.GENERATE_DIR_NAME;
      sp.segmentName = lastGname;
      wrapper.setMeta(SEGMENT_PART_KEY, sp.toString());
      output.collect(key, wrapper);
    }
    if (lastF != null) {
      wrapper.set(lastF);
      sp.partName = CrawlDatum.FETCH_DIR_NAME;
      sp.segmentName = lastFname;
      wrapper.setMeta(SEGMENT_PART_KEY, sp.toString());
      output.collect(key, wrapper);
    }
    if (lastSig != null) {
      wrapper.set(lastSig);
      sp.partName = CrawlDatum.PARSE_DIR_NAME;
      sp.segmentName = lastSigname;
      wrapper.setMeta(SEGMENT_PART_KEY, sp.toString());
      output.collect(key, wrapper);
    }
    if (lastC != null) {
      wrapper.set(lastC);
      sp.partName = Content.DIR_NAME;
      sp.segmentName = lastCname;
      wrapper.setMeta(SEGMENT_PART_KEY, sp.toString());
      output.collect(key, wrapper);
    }
    if (lastPD != null) {
      wrapper.set(lastPD);
      sp.partName = ParseData.DIR_NAME;
      sp.segmentName = lastPDname;
      wrapper.setMeta(SEGMENT_PART_KEY, sp.toString());
      output.collect(key, wrapper);
    }
    if (lastPT != null) {
      wrapper.set(lastPT);
      sp.partName = ParseText.DIR_NAME;
      sp.segmentName = lastPTname;
      wrapper.setMeta(SEGMENT_PART_KEY, sp.toString());
      output.collect(key, wrapper);
    }
    if (linked.size() > 0) {
      String name = linked.lastKey();
      sp.partName = CrawlDatum.PARSE_DIR_NAME;
      sp.segmentName = name;
      wrapper.setMeta(SEGMENT_PART_KEY, sp.toString());
      ArrayList<CrawlDatum> segLinked = linked.get(name);
      for (int i = 0; i < segLinked.size(); i++) {
        CrawlDatum link = segLinked.get(i);
        wrapper.set(link);
        output.collect(key, wrapper);
      }
    }
  }

  public void merge(Path out, Path[] segs, boolean filter, boolean normalize, long slice) throws Exception {
    String segmentName = Generator.generateSegmentName();
    if (LOG.isInfoEnabled()) {
      LOG.info("Merging " + segs.length + " segments to " + out + "/" + segmentName);
    }
    JobConf job = new NutchJob(getConf());
    job.setJobName("mergesegs " + out + "/" + segmentName);
    job.setBoolean("segment.merger.filter", filter);
    job.setBoolean("segment.merger.normalizer", normalize);
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
    job.setMapperClass(SegmentMerger.class);
    job.setReducerClass(SegmentMerger.class);
    job.setOutputPath(out);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(MetaWrapper.class);
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
    ArrayList<Path> segs = new ArrayList<Path>();
    long sliceSize = 0;
    boolean filter = false;
    boolean normalize = false;
    for (int i = 1; i < args.length; i++) {
      if (args[i].equals("-dir")) {
        Path[] files = fs.listPaths(new Path(args[++i]), HadoopFSUtil.getPassDirectoriesFilter(fs));
        for (int j = 0; j < files.length; j++)
          segs.add(files[j]);
      } else if (args[i].equals("-filter")) {
        filter = true;
      } else if (args[i].equals("-normalize")) {
        normalize = true;
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
    merger.merge(out, segs.toArray(new Path[segs.size()]), filter, normalize, sliceSize);
  }

}
