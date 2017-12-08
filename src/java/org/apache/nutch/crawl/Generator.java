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

package org.apache.nutch.crawl;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.net.*;
import java.util.*;
import java.text.*;

// rLogging imports
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.jexl2.Expression;
import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.MapContext;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.mapred.lib.MultipleSequenceFileOutputFormat;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.nutch.hostdb.HostDatum;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.net.URLFilterException;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.util.JexlUtil;
import org.apache.nutch.util.LockUtil;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.NutchTool;
import org.apache.nutch.util.TimingUtil;
import org.apache.nutch.util.URLUtil;

/**
 * Generates a subset of a crawl db to fetch. This version allows to generate
 * fetchlists for several segments in one go. Unlike in the initial version
 * (OldGenerator), the IP resolution is done ONLY on the entries which have been
 * selected for fetching. The URLs are partitioned by IP, domain or host within
 * a segment. We can chose separately how to count the URLS i.e. by domain or
 * host to limit the entries.
 **/
public class Generator extends NutchTool implements Tool {

  protected static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  public static final String GENERATE_UPDATE_CRAWLDB = "generate.update.crawldb";
  public static final String GENERATOR_MIN_SCORE = "generate.min.score";
  public static final String GENERATOR_MIN_INTERVAL = "generate.min.interval";
  public static final String GENERATOR_RESTRICT_STATUS = "generate.restrict.status";
  public static final String GENERATOR_FILTER = "generate.filter";
  public static final String GENERATOR_NORMALISE = "generate.normalise";
  public static final String GENERATOR_MAX_COUNT = "generate.max.count";
  public static final String GENERATOR_COUNT_MODE = "generate.count.mode";
  public static final String GENERATOR_COUNT_VALUE_DOMAIN = "domain";
  public static final String GENERATOR_COUNT_VALUE_HOST = "host";
  public static final String GENERATOR_TOP_N = "generate.topN";
  public static final String GENERATOR_CUR_TIME = "generate.curTime";
  public static final String GENERATOR_DELAY = "crawl.gen.delay";
  public static final String GENERATOR_MAX_NUM_SEGMENTS = "generate.max.num.segments";
  public static final String GENERATOR_EXPR = "generate.expr";
  public static final String GENERATOR_HOSTDB = "generate.hostdb";
  public static final String GENERATOR_MAX_COUNT_EXPR = "generate.max.count.expr";
  public static final String GENERATOR_FETCH_DELAY_EXPR = "generate.fetch.delay.expr";

  public static class SelectorEntry implements Writable {
    public Text url;
    public CrawlDatum datum;
    public IntWritable segnum;
    public HostDatum hostdatum;

    public SelectorEntry() {
      url = new Text();
      datum = new CrawlDatum();
      segnum = new IntWritable(0);
      hostdatum = new HostDatum();
    }

    public void readFields(DataInput in) throws IOException {
      url.readFields(in);
      datum.readFields(in);
      segnum.readFields(in);
      hostdatum.readFields(in);
    }

    public void write(DataOutput out) throws IOException {
      url.write(out);
      datum.write(out);
      segnum.write(out);
      hostdatum.write(out);
    }

    public String toString() {
      return "url=" + url.toString() + ", datum=" + datum.toString()
          + ", segnum=" + segnum.toString();
    }
  }

  public static class HostDbReaderMapper implements Mapper<Text, HostDatum, FloatTextPair, SelectorEntry>
  {
	@Override
	public void configure(JobConf conf) {
		// TODO Auto-generated method stub	
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
	}

	@Override
	public void map(Text hostname, HostDatum value,
			OutputCollector<FloatTextPair, SelectorEntry> output, Reporter reporter)
			throws IOException {
	    SelectorEntry hostDataSelector = new SelectorEntry();
	    hostDataSelector.hostdatum = value;

	    // setup small/big score on the output to distinguish between hostdatum mapper and data mapper.
	    output.collect(new FloatTextPair(new FloatWritable(-Float.MAX_VALUE),hostname), hostDataSelector);
	    
	}  
  }
  
  public static class FloatTextPair implements WritableComparable<FloatTextPair>{  
	  	public FloatWritable first;
		public Text second;
		
		public  FloatTextPair(){
		    this.first=new FloatWritable();
		    this.second=new Text();
		}
		
		public FloatTextPair(FloatWritable first, Text second) {
		    //super();
		    this.first = first;
		    this.second = second;
		}
		public FloatTextPair(float first,String second){
		    this.first=new FloatWritable(first);
		    this.second=new Text(second);
		}
		
		public FloatWritable getFirst() {
		    return first;
		}
		
		public void setFirst(FloatWritable first) {
		    this.first = first;
		}
		
		public Text getSecond() {
		    return second;
		}
		
		public void setSecond(Text second) {
		    this.second = second;
		}
		public void set(FloatWritable first,Text second){
		    this.first=first;
		    this.second=second;
		}
		
		@Override
		public int hashCode() {
		    // TODO Auto-generated method stub
		    return first.hashCode()*163+second.hashCode();
		}
		
		@Override
		public boolean equals(Object obj) {
		    // TODO Auto-generated method stub
		    if(obj instanceof FloatTextPair){
		        FloatTextPair tp=(FloatTextPair)obj;
		        return first.equals(tp.getFirst())&&second.equals(tp.getSecond());
		    }
		    return false;
		}
		
		@Override
		public String toString() {
		    return first+"\t"+second;
		}
		
		@Override
		public void readFields(DataInput in) throws IOException {
		    first.readFields(in);
		    second.readFields(in);
		}
		
		@Override
		public void write(DataOutput out) throws IOException {
		    first.write(out);
		    second.write(out);
		}		
		
		@Override
		public int compareTo(FloatTextPair tp) {
		    int cmp=first.compareTo(tp.getFirst());
		    if(cmp!=0)
		        return cmp;
		    return second.compareTo(tp.getSecond());
		    
		}
		
	}
  //The comparator is made to "merge" hostdb data and crawldd data. See  NUTCH-2455
  //TODO : Implement RawComparator
  public static class ScoreHostKeyComparator extends WritableComparator {
	    protected ScoreHostKeyComparator() {
	        super(FloatTextPair.class, true);
	    }

	    @Override
	    public int compare(WritableComparable w1, WritableComparable w2) {
	    	FloatTextPair key1 = (FloatTextPair) w1;
	    	FloatTextPair key2 = (FloatTextPair) w2;     
	    	
	    	Boolean iskey1HostDatum = key1.second.getLength() > 0;
	    	Boolean iskey2HostDatum = key2.second.getLength() > 0;
	    		    	
	    	if(iskey1HostDatum  && iskey2HostDatum){
	    		return key1.second.compareTo(key2.second);
	    	} else {
	    		if(iskey1HostDatum == iskey2HostDatum){
	    			return -1 * key1.first.compareTo(key2.first);
	    		}
	    		else if(iskey1HostDatum){
		    		return -1;
		    	} else {
		    		return 1;
		    	}	
	    	}
	    }
  }
  
  /** Selects entries due for fetch. */
  public static class Selector implements
      Mapper<Text, CrawlDatum, FloatTextPair, SelectorEntry>,
      Partitioner<FloatTextPair, Writable>,
      Reducer<FloatTextPair, SelectorEntry, FloatWritable, SelectorEntry> {
    private LongWritable genTime = new LongWritable(System.currentTimeMillis());
    private long curTime;
    private long limit;
    private long count;
    private HashMap<String, MutablePair<HostDatum, int[]>> hostDomainCounts = new HashMap<>();
    private int segCounts[];
    private int maxCount;
    private boolean byDomain = false;
    private URLPartitioner partitioner = new URLPartitioner();
    private URLFilters filters;
    private URLNormalizers normalizers;
    private ScoringFilters scfilters;
    private SelectorEntry entry = new SelectorEntry();
    private FloatWritable sortValue = new FloatWritable();
    private boolean filter;
    private boolean normalise;
    private long genDelay;
    private FetchSchedule schedule;
    private float scoreThreshold = 0f;
    private int intervalThreshold = -1;
    private String restrictStatus = null;
    private int maxNumSegments = 1;
    private Expression expr = null;
    private int currentsegmentnum = 1;
    private SequenceFile.Reader[] hostdbReaders = null;
    private Expression maxCountExpr = null;
    private Expression fetchDelayExpr = null;
    private JobConf conf = null;
    
    public void configure(JobConf job) {
      this.conf = job;
      curTime = job.getLong(GENERATOR_CUR_TIME, System.currentTimeMillis());
      limit = job.getLong(GENERATOR_TOP_N, Long.MAX_VALUE)
          / job.getNumReduceTasks();
      maxCount = job.getInt(GENERATOR_MAX_COUNT, -1);
      if (maxCount == -1) {
        byDomain = false;
      }
      if (GENERATOR_COUNT_VALUE_DOMAIN.equals(job.get(GENERATOR_COUNT_MODE)))
        byDomain = true;
      filters = new URLFilters(job);
      normalise = job.getBoolean(GENERATOR_NORMALISE, true);
      if (normalise)
        normalizers = new URLNormalizers(job,
            URLNormalizers.SCOPE_GENERATE_HOST_COUNT);
      scfilters = new ScoringFilters(job);
      partitioner.configure(job);
      filter = job.getBoolean(GENERATOR_FILTER, true);
      genDelay = job.getLong(GENERATOR_DELAY, 7L) * 3600L * 24L * 1000L;
      long time = job.getLong(Nutch.GENERATE_TIME_KEY, 0L);
      if (time > 0)
        genTime.set(time);
      schedule = FetchScheduleFactory.getFetchSchedule(job);
      scoreThreshold = job.getFloat(GENERATOR_MIN_SCORE, Float.NaN);
      intervalThreshold = job.getInt(GENERATOR_MIN_INTERVAL, -1);
      restrictStatus = job.get(GENERATOR_RESTRICT_STATUS, null);
      expr = JexlUtil.parseExpression(job.get(GENERATOR_EXPR, null));
      maxNumSegments = job.getInt(GENERATOR_MAX_NUM_SEGMENTS, 1);
      segCounts = new int[maxNumSegments];
      
      if (job.get(GENERATOR_HOSTDB) != null) {
        maxCountExpr = JexlUtil.parseExpression(job.get(GENERATOR_MAX_COUNT_EXPR, null));
        fetchDelayExpr = JexlUtil.parseExpression(job.get(GENERATOR_FETCH_DELAY_EXPR, null));
      }
    }
    
    @Override
    public void close() {
    }
    /** Select and invert subset due for fetch. */
    public void map(Text key, CrawlDatum value,
        OutputCollector<FloatTextPair, SelectorEntry> output, Reporter reporter)
        throws IOException {
      Text url = key;
      
      if (filter) {
        // If filtering is on don't generate URLs that don't pass
        // URLFilters
        try {
          if (filters.filter(url.toString()) == null)
            return;
        } catch (URLFilterException e) {
          if (LOG.isWarnEnabled()) {
            LOG.warn("Couldn't filter url: " + url + " (" + e.getMessage()
                + ")");
          }
        }
      }
      CrawlDatum crawlDatum = value;

      // check fetch schedule
      if (!schedule.shouldFetch(url, crawlDatum, curTime)) {
        LOG.debug("-shouldFetch rejected '" + url + "', fetchTime="
            + crawlDatum.getFetchTime() + ", curTime=" + curTime);
        return;
      }

      LongWritable oldGenTime = (LongWritable) crawlDatum.getMetaData().get(
          Nutch.WRITABLE_GENERATE_TIME_KEY);
      if (oldGenTime != null) { // awaiting fetch & update
        if (oldGenTime.get() + genDelay > curTime) // still wait for
          // update
          return;
      }
      float sort = 1.0f;
      try {
        sort = scfilters.generatorSortValue(key, crawlDatum, sort);
      } catch (ScoringFilterException sfe) {
        if (LOG.isWarnEnabled()) {
          LOG.warn("Couldn't filter generatorSortValue for " + key + ": " + sfe);
        }
      }
      
      // check expr
      if (expr != null) {
        if (!crawlDatum.evaluate(expr)) {
          return;
        }
      }

      if (restrictStatus != null
          && !restrictStatus.equalsIgnoreCase(CrawlDatum
              .getStatusName(crawlDatum.getStatus())))
        return;

      // consider only entries with a score superior to the threshold
      if (scoreThreshold != Float.NaN && sort < scoreThreshold)
        return;

      // consider only entries with a retry (or fetch) interval lower than
      // threshold
      if (intervalThreshold != -1
          && crawlDatum.getFetchInterval() > intervalThreshold)
        return;

      // sort by decreasing score, using DecreasingFloatComparator
      sortValue.set(sort);
      // record generation time
      crawlDatum.getMetaData().put(Nutch.WRITABLE_GENERATE_TIME_KEY, genTime);
      entry.datum = crawlDatum;
      entry.url = key;
    
      output.collect(new FloatTextPair(sortValue,new Text()), entry); // invert for sort by score
    }

    /** Partition by host / domain or IP. */
   public int getPartition(FloatTextPair key, Writable value,
        int numReduceTasks) {
      return partitioner.getPartition(((SelectorEntry) value).url, key.getSecond(),
          numReduceTasks);
    } 

    private JexlContext createContext(HostDatum datum) {
      JexlContext context = new MapContext();
      context.set("dnsFailures", datum.getDnsFailures());
      context.set("connectionFailures", datum.getConnectionFailures());
      context.set("unfetched", datum.getUnfetched());
      context.set("fetched", datum.getFetched());
      context.set("notModified", datum.getNotModified());
      context.set("redirTemp", datum.getRedirTemp());
      context.set("redirPerm", datum.getRedirPerm());
      context.set("gone", datum.getGone());
      context.set("conf", conf);
      
      // Set metadata variables
      for (Map.Entry<Writable, Writable> entry : datum.getMetaData().entrySet()) {
        Object value = entry.getValue();
        
        if (value instanceof FloatWritable) {
          FloatWritable fvalue = (FloatWritable)value;
          Text tkey = (Text)entry.getKey();
          context.set(tkey.toString(), fvalue.get());
        }
        
        if (value instanceof IntWritable) {
          IntWritable ivalue = (IntWritable)value;
          Text tkey = (Text)entry.getKey();
          context.set(tkey.toString(), ivalue.get());
        }
        
        if (value instanceof Text) {
          Text tvalue = (Text)value;
          Text tkey = (Text)entry.getKey();     
          context.set(tkey.toString().replace("-", "_"), tvalue.toString());
        }
      }
      
      return context;
    }

    /** Collect until limit is reached. */
    public void reduce(FloatTextPair key, Iterator<SelectorEntry> values,
        OutputCollector<FloatWritable, SelectorEntry> output, Reporter reporter)
        throws IOException {
    
      String hostname = null;
      LongWritable variableFetchDelayWritable = null; // in millis
      Text variableFetchDelayKey = new Text("_variableFetchDelay_");
      int maxCount = this.maxCount;
      int[] hostDomainCount = null;
      HostDatum hostDatum = null;
      
      while (values.hasNext()) {
        SelectorEntry entry = values.next();
        Text url = entry.url;
        String urlString = url.toString();
        URL u = null;
        String hostorDomainName = null;
        
        //Extract hostdatum 
        if(key.second.getLength() > 0)
        {
        	hostDatum = entry.hostdatum;
        	MutablePair<HostDatum, int[]> hostDataPair = new MutablePair<HostDatum, int[]>(hostDatum, new int []{1,0});
        	hostDomainCounts.put(key.second.toString(), hostDataPair);
        } else //Process normal input with pre-filled in hostdatum in hostCounts
        {
         try {
              u = new URL(urlString);
              
              if (byDomain) {
                  hostorDomainName = URLUtil.getUrlRootByMode(u,URLPartitioner.PARTITION_MODE_DOMAIN).toLowerCase();
                } else {
                  hostorDomainName = URLUtil.getUrlRootByMode(u,URLPartitioner.PARTITION_MODE_HOST).toLowerCase();
              }
              
              MutablePair<HostDatum, int[]> hostDomainCountPair = hostDomainCounts.get(hostorDomainName);
      
               if(hostDomainCountPair == null){ 
                  hostDomainCount = new int[] { 1, 0 };
                  hostDomainCountPair = new MutablePair<HostDatum, int[]>(null, hostDomainCount);
                  hostDomainCounts.put(hostorDomainName, hostDomainCountPair);
               }
               else{
              	  hostDomainCount = hostDomainCountPair.getRight();
               }
             
              //Check hostdb expressions only for host, ignore domains
               if(!byDomain)
            	hostDatum = hostDomainCountPair.getLeft();
                
                if (hostDatum != null) {
                  if (maxCountExpr != null) {
                    long variableMaxCount = Math.round((double)maxCountExpr.evaluate(createContext(hostDatum)));
                    LOG.info("Generator: variable maxCount: {} for {}", variableMaxCount, hostorDomainName);
                    maxCount = (int)variableMaxCount;
                  }
                  
                  if (fetchDelayExpr != null) {
                    long variableFetchDelay = Math.round((double)fetchDelayExpr.evaluate(createContext(hostDatum)));
                    LOG.info("Generator: variable fetchDelay: {} ms for {}", variableFetchDelay, hostorDomainName);
                    variableFetchDelayWritable = new LongWritable(variableFetchDelay);              
                  }
                }
              }
          catch (Exception e) {
        	  LOG.info("Exception while doing host/domain extraction", e.toString());
          }
              
        // Got a non-zero variable fetch delay? Add it to the datum's metadata
        if (variableFetchDelayWritable != null) {
          entry.datum.getMetaData().put(variableFetchDelayKey, variableFetchDelayWritable);
        }

        if (count == limit) {
          // do we have any segments left?
          if (currentsegmentnum < maxNumSegments) {
            count = 0;
            currentsegmentnum++;
          } else
            break;
        }

    

        try {
          if (normalise && normalizers != null) {
            urlString = normalizers.normalize(urlString,
                URLNormalizers.SCOPE_GENERATE_HOST_COUNT);
          }

        } catch (Exception e) {
          LOG.warn("Malformed URL: '" + urlString + "', skipping ("
              + StringUtils.stringifyException(e) + ")");
          reporter.getCounter("Generator", "MALFORMED_URL").increment(1);
          continue;
        }

        // only filter if we are counting hosts or domains
        if (maxCount > 0) {
          // increment hostCount
          hostDomainCount[1]++;

          // check if topN reached, select next segment if it is
          while (segCounts[hostDomainCount[0] - 1] >= limit
              && hostDomainCount[0] < maxNumSegments) {
            hostDomainCount[0]++;
            hostDomainCount[1] = 0;
          }

          // reached the limit of allowed URLs per host / domain
          // see if we can put it in the next segment?
          if (hostDomainCount[1] >= maxCount) {
            if (hostDomainCount[0] < maxNumSegments) {
              hostDomainCount[0]++;
              hostDomainCount[1] = 0;
            } else {
              if (hostDomainCount[1] == maxCount + 1 && LOG.isInfoEnabled()) {
                LOG.info("Host or domain "
                    + hostorDomainName
                    + " has more than "
                    + maxCount
                    + " URLs for all "
                    + maxNumSegments
                    + " segments. Additional URLs won't be included in the fetchlist.");
              }
              // skip this entry
              continue;
            }
          }
          entry.segnum = new IntWritable(hostDomainCount[0]);
          segCounts[hostDomainCount[0] - 1]++;
        } else {
          entry.segnum = new IntWritable(currentsegmentnum);
          segCounts[currentsegmentnum - 1]++;
        }

         output.collect(key.first, entry);

        // Count is incremented only when we keep the URL
        // maxCount may cause us to skip it.
        count++;
      }
      }
    }
  }
  

  // Allows the reducers to generate one subfile per
  public static class GeneratorOutputFormat extends
      MultipleSequenceFileOutputFormat<FloatWritable, SelectorEntry> {
    // generate a filename based on the segnum stored for this entry
    protected String generateFileNameForKeyValue(FloatWritable key,
        SelectorEntry value, String name) {
      return "fetchlist-" + value.segnum.toString() + "/" + name;
    }

  }

  public static class SelectorInverseMapper extends MapReduceBase implements
      Mapper<FloatWritable, SelectorEntry, Text, SelectorEntry> {

    public void map(FloatWritable key, SelectorEntry value,
        OutputCollector<Text, SelectorEntry> output, Reporter reporter)
        throws IOException {
      SelectorEntry entry = value;
      output.collect(entry.url, entry);
    }
  }

  public static class PartitionReducer extends MapReduceBase implements
      Reducer<Text, SelectorEntry, Text, CrawlDatum> {

    public void reduce(Text key, Iterator<SelectorEntry> values,
        OutputCollector<Text, CrawlDatum> output, Reporter reporter)
        throws IOException {
      // if using HashComparator, we get only one input key in case of
      // hash collision
      // so use only URLs from values
      while (values.hasNext()) {
        SelectorEntry entry = values.next();
        output.collect(entry.url, entry.datum);
      }
    }

  }

  /** Sort fetch lists by hash of URL. */
  public static class HashComparator extends WritableComparator {
    public HashComparator() {
      super(Text.class);
    }

    @SuppressWarnings("rawtypes")
    public int compare(WritableComparable a, WritableComparable b) {
      Text url1 = (Text) a;
      Text url2 = (Text) b;
      int hash1 = hash(url1.getBytes(), 0, url1.getLength());
      int hash2 = hash(url2.getBytes(), 0, url2.getLength());
      return (hash1 < hash2 ? -1 : (hash1 == hash2 ? 0 : 1));
    }

    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      int hash1 = hash(b1, s1, l1);
      int hash2 = hash(b2, s2, l2);
      return (hash1 < hash2 ? -1 : (hash1 == hash2 ? 0 : 1));
    }

    private static int hash(byte[] bytes, int start, int length) {
      int hash = 1;
      // make later bytes more significant in hash code, so that sorting
      // by
      // hashcode correlates less with by-host ordering.
      for (int i = length - 1; i >= 0; i--)
        hash = (31 * hash) + (int) bytes[start + i];
      return hash;
    }
  }

  /**
   * Update the CrawlDB so that the next generate won't include the same URLs.
   */
  public static class CrawlDbUpdater extends MapReduceBase implements
      Mapper<Text, CrawlDatum, Text, CrawlDatum>,
      Reducer<Text, CrawlDatum, Text, CrawlDatum> {
    long generateTime;

    public void configure(JobConf job) {
      generateTime = job.getLong(Nutch.GENERATE_TIME_KEY, 0L);
    }

    public void map(Text key, CrawlDatum value,
        OutputCollector<Text, CrawlDatum> output, Reporter reporter)
        throws IOException {
      output.collect(key, value);
    }

    private CrawlDatum orig = new CrawlDatum();
    private LongWritable genTime = new LongWritable(0L);

    public void reduce(Text key, Iterator<CrawlDatum> values,
        OutputCollector<Text, CrawlDatum> output, Reporter reporter)
        throws IOException {
      genTime.set(0L);
      while (values.hasNext()) {
        CrawlDatum val = values.next();
        if (val.getMetaData().containsKey(Nutch.WRITABLE_GENERATE_TIME_KEY)) {
          LongWritable gt = (LongWritable) val.getMetaData().get(
              Nutch.WRITABLE_GENERATE_TIME_KEY);
          genTime.set(gt.get());
          if (genTime.get() != generateTime) {
            orig.set(val);
            genTime.set(0L);
            continue;
          }
        } else {
          orig.set(val);
        }
      }
      if (genTime.get() != 0L) {
        orig.getMetaData().put(Nutch.WRITABLE_GENERATE_TIME_KEY, genTime);
      }
      output.collect(key, orig);
    }
  }

  public Generator() {
  }

  public Generator(Configuration conf) {
    setConf(conf);
  }

  public Path[] generate(Path dbDir, Path segments, int numLists, long topN,
      long curTime) throws IOException {

    JobConf job = new NutchJob(getConf());
    boolean filter = job.getBoolean(GENERATOR_FILTER, true);
    boolean normalise = job.getBoolean(GENERATOR_NORMALISE, true);
    return generate(dbDir, segments, numLists, topN, curTime, filter,
        normalise, false, 1, null);
  }

  /**
   * old signature used for compatibility - does not specify whether or not to
   * normalise and set the number of segments to 1
   **/
  public Path[] generate(Path dbDir, Path segments, int numLists, long topN,
      long curTime, boolean filter, boolean force) throws IOException {
    return generate(dbDir, segments, numLists, topN, curTime, filter, true,
        force, 1, null);
  }
  
  public Path[] generate(Path dbDir, Path segments, int numLists, long topN,
      long curTime, boolean filter, boolean norm, boolean force,
      int maxNumSegments, String expr) throws IOException {
    return generate(dbDir, segments, numLists, topN, curTime, filter, true,
        force, 1, expr, null);
  }

  /**
   * Generate fetchlists in one or more segments. Whether to filter URLs or not
   * is read from the crawl.generate.filter property in the configuration files.
   * If the property is not found, the URLs are filtered. Same for the
   * normalisation.
   * 
   * @param dbDir
   *          Crawl database directory
   * @param segments
   *          Segments directory
   * @param numLists
   *          Number of reduce tasks
   * @param topN
   *          Number of top URLs to be selected
   * @param curTime
   *          Current time in milliseconds
   * 
   * @return Path to generated segment or null if no entries were selected
   * 
   * @throws IOException
   *           When an I/O error occurs
   */
  public Path[] generate(Path dbDir, Path segments, int numLists, long topN,
      long curTime, boolean filter, boolean norm, boolean force,
      int maxNumSegments, String expr, String hostdb) throws IOException {

    Path tempDir = new Path(getConf().get("mapred.temp.dir", ".")
        + "/generate-temp-" + java.util.UUID.randomUUID().toString());
    FileSystem fs = tempDir.getFileSystem(getConf());

    Path lock = CrawlDb.lock(getConf(), dbDir, force);
    
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    long start = System.currentTimeMillis();
    LOG.info("Generator: starting at " + sdf.format(start));
    LOG.info("Generator: Selecting best-scoring urls due for fetch.");
    LOG.info("Generator: filtering: " + filter);
    LOG.info("Generator: normalizing: " + norm);
    if (topN != Long.MAX_VALUE) {
      LOG.info("Generator: topN: {}", topN);
    }
    if (expr != null) {
      LOG.info("Generator: expr: {}", expr);
    }
    if (expr != null) {
      LOG.info("Generator: hostdb: {}", hostdb);
    }
    
    // map to inverted subset due for fetch, sort by score
    JobConf job = new NutchJob(getConf());
    job.setJobName("generate: select from " + dbDir);

    if (numLists == -1) { // for politeness make
      numLists = job.getNumMapTasks(); // a partition per fetch task
    }
    if ("local".equals(job.get("mapreduce.framework.name")) && numLists != 1) {
      // override
      LOG.info("Generator: running in local mode, generating exactly one partition.");
      numLists = 1;
    }
    job.setLong(GENERATOR_CUR_TIME, curTime);
    // record real generation time
    long generateTime = System.currentTimeMillis();
    job.setLong(Nutch.GENERATE_TIME_KEY, generateTime);
    job.setLong(GENERATOR_TOP_N, topN);
    job.setBoolean(GENERATOR_FILTER, filter);
    job.setBoolean(GENERATOR_NORMALISE, norm);
    job.setInt(GENERATOR_MAX_NUM_SEGMENTS, maxNumSegments);
    if (expr != null) {
      job.set(GENERATOR_EXPR, expr);
    }
    if (hostdb != null) {
      job.set(GENERATOR_HOSTDB, hostdb);
  	  MultipleInputs.addInputPath(job,new Path(hostdb, "current"), SequenceFileInputFormat.class, HostDbReaderMapper.class);
    }
    job.setMapOutputKeyClass(FloatTextPair.class);
    MultipleInputs.addInputPath(job, new Path(dbDir, CrawlDb.CURRENT_NAME), SequenceFileInputFormat.class, Selector.class);

    job.setOutputKeyComparatorClass(ScoreHostKeyComparator.class);
    job.setPartitionerClass(Selector.class);
    job.setReducerClass(Selector.class);
    
    FileOutputFormat.setOutputPath(job, tempDir);
    job.setOutputFormat(SequenceFileOutputFormat.class);
    job.setOutputKeyClass(FloatWritable.class);
    job.setOutputValueClass(SelectorEntry.class);
    job.setOutputFormat(GeneratorOutputFormat.class);

    try {
      JobClient.runJob(job);
    } catch (IOException e) {
      LockUtil.removeLockFile(getConf(), lock);
      fs.delete(tempDir, true);
      throw e;
    }

    // read the subdirectories generated in the temp
    // output and turn them into segments
    List<Path> generatedSegments = new ArrayList<>();

    FileStatus[] status = fs.listStatus(tempDir);
    try {
      for (FileStatus stat : status) {
        Path subfetchlist = stat.getPath();
        if (!subfetchlist.getName().startsWith("fetchlist-"))
          continue;
        // start a new partition job for this segment
        Path newSeg = partitionSegment(segments, subfetchlist, numLists);
        generatedSegments.add(newSeg);
      }
    } catch (Exception e) {
      LOG.warn("Generator: exception while partitioning segments, exiting ...");
      fs.delete(tempDir, true);
      return null;
    }

    if (generatedSegments.size() == 0) {
      LOG.warn("Generator: 0 records selected for fetching, exiting ...");
      LockUtil.removeLockFile(getConf(), lock);
      fs.delete(tempDir, true);
      return null;
    }

    if (getConf().getBoolean(GENERATE_UPDATE_CRAWLDB, false)) {
      // update the db from tempDir
      Path tempDir2 = new Path(dbDir,
          "generate-temp-" + java.util.UUID.randomUUID().toString());

      job = new NutchJob(getConf());
      job.setJobName("generate: updatedb " + dbDir);
      job.setLong(Nutch.GENERATE_TIME_KEY, generateTime);
      for (Path segmpaths : generatedSegments) {
        Path subGenDir = new Path(segmpaths, CrawlDatum.GENERATE_DIR_NAME);
        FileInputFormat.addInputPath(job, subGenDir);
      }
      FileInputFormat.addInputPath(job, new Path(dbDir, CrawlDb.CURRENT_NAME));
      job.setInputFormat(SequenceFileInputFormat.class);

      job.setMapperClass(CrawlDbUpdater.class);
      job.setReducerClass(CrawlDbUpdater.class);
      job.setOutputFormat(MapFileOutputFormat.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(CrawlDatum.class);
      FileOutputFormat.setOutputPath(job, tempDir2);
      try {
        JobClient.runJob(job);
        CrawlDb.install(job, dbDir);
      } catch (IOException e) {
        LockUtil.removeLockFile(getConf(), lock);
        fs.delete(tempDir, true);
        fs.delete(tempDir2, true);
        throw e;
      }
      fs.delete(tempDir2, true);
    }

    LockUtil.removeLockFile(getConf(), lock);
    fs.delete(tempDir, true);

    long end = System.currentTimeMillis();
    LOG.info("Generator: finished at " + sdf.format(end) + ", elapsed: "
        + TimingUtil.elapsedTime(start, end));

    Path[] patharray = new Path[generatedSegments.size()];
    return generatedSegments.toArray(patharray);
  }

  private Path partitionSegment(Path segmentsDir, Path inputDir, int numLists)
      throws IOException {
    // invert again, partition by host/domain/IP, sort by url hash
    if (LOG.isInfoEnabled()) {
      LOG.info("Generator: Partitioning selected urls for politeness.");
    }
    Path segment = new Path(segmentsDir, generateSegmentName());
    Path output = new Path(segment, CrawlDatum.GENERATE_DIR_NAME);

    LOG.info("Generator: segment: " + segment);

    NutchJob job = new NutchJob(getConf());
    job.setJobName("generate: partition " + segment);

    job.setInt("partition.url.seed", new Random().nextInt());

    FileInputFormat.addInputPath(job, inputDir);
    job.setInputFormat(SequenceFileInputFormat.class);
    job.setMapperClass(SelectorInverseMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(SelectorEntry.class);
    job.setPartitionerClass(URLPartitioner.class);
    job.setReducerClass(PartitionReducer.class);
    job.setNumReduceTasks(numLists);

    FileOutputFormat.setOutputPath(job, output);
    job.setOutputFormat(SequenceFileOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(CrawlDatum.class);
    job.setOutputKeyComparatorClass(HashComparator.class);
    JobClient.runJob(job);
    return segment;
  }

  private static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");

  public static synchronized String generateSegmentName() {
    try {
      Thread.sleep(1000);
    } catch (Throwable t) {
    }
    ;
    return sdf.format(new Date(System.currentTimeMillis()));
  }

  /**
   * Generate a fetchlist from the crawldb.
   */
  public static void main(String args[]) throws Exception {
    int res = ToolRunner
        .run(NutchConfiguration.create(), new Generator(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      System.out
          .println("Usage: Generator <crawldb> <segments_dir> [-force] [-topN N] [-numFetchers numFetchers] [-expr <expr>] [-adddays <numDays>] [-noFilter] [-noNorm] [-maxNumSegments <num>]");
      return -1;
    }

    Path dbDir = new Path(args[0]);
    Path segmentsDir = new Path(args[1]);
    String hostdb = null;
    long curTime = System.currentTimeMillis();
    long topN = Long.MAX_VALUE;
    int numFetchers = -1;
    boolean filter = true;
    boolean norm = true;
    boolean force = false;
    String expr = null;
    int maxNumSegments = 1;

    for (int i = 2; i < args.length; i++) {
      if ("-topN".equals(args[i])) {
        topN = Long.parseLong(args[i + 1]);
        i++;
      } else if ("-numFetchers".equals(args[i])) {
        numFetchers = Integer.parseInt(args[i + 1]);
        i++;
      } else if ("-hostdb".equals(args[i])) {
        hostdb = args[i + 1];
        i++;
      } else if ("-adddays".equals(args[i])) {
        long numDays = Integer.parseInt(args[i + 1]);
        curTime += numDays * 1000L * 60 * 60 * 24;
      } else if ("-noFilter".equals(args[i])) {
        filter = false;
      } else if ("-noNorm".equals(args[i])) {
        norm = false;
      } else if ("-force".equals(args[i])) {
        force = true;
      } else if ("-maxNumSegments".equals(args[i])) {
        maxNumSegments = Integer.parseInt(args[i + 1]);
      } else if ("-expr".equals(args[i])) {
        expr = args[i + 1];
      }

    }

    try {
      Path[] segs = generate(dbDir, segmentsDir, numFetchers, topN, curTime,
          filter, norm, force, maxNumSegments, expr, hostdb);
      if (segs == null)
        return 1;
    } catch (Exception e) {
      LOG.error("Generator: " + StringUtils.stringifyException(e));
      return -1;
    }
    return 0;
  }

  @Override
  public Map<String, Object> run(Map<String, Object> args, String crawlId) throws Exception {

    Map<String, Object> results = new HashMap<>();

    long curTime = System.currentTimeMillis();
    long topN = Long.MAX_VALUE;
    int numFetchers = -1;
    boolean filter = true;
    boolean norm = true;
    boolean force = false;
    int maxNumSegments = 1;
    String expr = null;

    Path crawlDb;
    if(args.containsKey(Nutch.ARG_CRAWLDB)) {
      Object crawldbPath = args.get(Nutch.ARG_CRAWLDB);
      if(crawldbPath instanceof Path) {
        crawlDb = (Path) crawldbPath;
      }
      else {
        crawlDb = new Path(crawldbPath.toString());
      }
    }
    else {
      crawlDb = new Path(crawlId+"/crawldb");
    }

    Path segmentsDir;
    if(args.containsKey(Nutch.ARG_SEGMENTDIR)) {
      Object segDir = args.get(Nutch.ARG_SEGMENTDIR);
      if(segDir instanceof Path) {
        segmentsDir = (Path) segDir;
      }
      else {
        segmentsDir = new Path(segDir.toString());
      }
    }
    else {
      segmentsDir = new Path(crawlId+"/segments");
    }
    
    if (args.containsKey("expr")) {
      expr = (String)args.get("expr");
    }
    if (args.containsKey("topN")) {
      topN = Long.parseLong((String)args.get("topN"));
    }
    if (args.containsKey("numFetchers")) {
      numFetchers = Integer.parseInt((String)args.get("numFetchers"));
    }
    if (args.containsKey("adddays")) {
      long numDays = Integer.parseInt((String)args.get("adddays"));
      curTime += numDays * 1000L * 60 * 60 * 24;
    }
    if (args.containsKey("noFilter")) {
      filter = false;
    } 
    if (args.containsKey("noNorm")) {
      norm = false;
    } 
    if (args.containsKey("force")) {
      force = true;
    } 
    if (args.containsKey("maxNumSegments")) {
      maxNumSegments = Integer.parseInt((String)args.get("maxNumSegments"));
    }

    try {
      Path[] segs = generate(crawlDb, segmentsDir, numFetchers, topN, curTime,
          filter, norm, force, maxNumSegments, expr);
      if (segs == null){
        results.put(Nutch.VAL_RESULT, Integer.toString(1));
        return results;
      }

    } catch (Exception e) {
      LOG.error("Generator: " + StringUtils.stringifyException(e));
      results.put(Nutch.VAL_RESULT, Integer.toString(-1));
      return results;
    }
    results.put(Nutch.VAL_RESULT, Integer.toString(0));
    return results;
  }
}
  
