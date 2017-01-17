/*******************************************************************************
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
 ******************************************************************************/
package org.apache.nutch.crawl;

import org.apache.avro.util.Utf8;
import org.apache.gora.mapreduce.GoraOutputFormat;
import org.apache.gora.persistency.Persistent;
import org.apache.gora.store.DataStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.StorageUtils;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * This class takes a flat file of URLs and adds them to the of pages to be
 * crawled. Useful for bootstrapping the system. The URL files contain one URL
 * per line, optionally followed by custom metadata separated by tabs with the
 * metadata key separated from the corresponding value by '='. <br>
 * Note that some metadata keys are reserved : <br>
 * - <i>nutch.score</i> : allows to set a custom score for a specific URL <br>
 * - <i>nutch.fetchInterval</i> : allows to set a custom fetch interval for a
 * specific URL <br>
 * e.g. http://www.nutch.org/ \t nutch.score=10 \t nutch.fetchInterval=2592000
 * \t userType=open_source
 **/
public class InjectorJob extends NutchTool implements Tool {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());
  
  private static final Set<WebPage.Field> FIELDS = new HashSet<WebPage.Field>();

  static {
    FIELDS.add(WebPage.Field.MARKERS);
    FIELDS.add(WebPage.Field.STATUS);
  }

  /** metadata key reserved for setting a custom score for a specific URL */
  public static String nutchScoreMDName = "nutch.score";
  /**
   * metadata key reserved for setting a custom fetchInterval for a specific URL
   */
  public static String nutchFetchIntervalMDName = "nutch.fetchInterval";

  public static class UrlMapper extends Mapper<LongWritable, Text, String, WebPage> {
    private URLNormalizers urlNormalizers;
    private int interval;
    private float scoreInjected;
    private URLFilters filters;
    private ScoringFilters scfilters;
    private long curTime;

    @Override
    protected void setup(Context context) throws IOException,
    InterruptedException {
      urlNormalizers = new URLNormalizers(context.getConfiguration(),
          URLNormalizers.SCOPE_INJECT);
      interval = context.getConfiguration().getInt("db.fetch.interval.default",
          2592000);
      filters = new URLFilters(context.getConfiguration());
      scfilters = new ScoringFilters(context.getConfiguration());
      scoreInjected = context.getConfiguration().getFloat("db.score.injected",
          1.0f);
      curTime = context.getConfiguration().getLong("injector.current.time",
          System.currentTimeMillis());
    }

    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      String url = value.toString().trim(); // value is line of text

      if (url != null && (url.length() == 0 || url.startsWith("#"))) {
        /* Ignore line that start with # */
        return;
      }

      // if tabs : metadata that could be stored
      // must be name=value and separated by \t
      float customScore = -1f;
      int customInterval = interval;
      Map<String, String> metadata = new TreeMap<String, String>();
      InjectType injectType = InjectType.INJECT;
      if (url.indexOf("\t") != -1) {
        String[] splits = url.split("\t");
        url = splits[0];
        for (int s = 1; s < splits.length; s++) {
          // find separation between name and value
          int indexEquals = splits[s].indexOf("=");
          if (splits[s].indexOf("sitemaps:") > -1) {
            String[] sitemaps = splits[s].trim().split(" ");
            String sitemapUrl;
            for (int i = 1; i < sitemaps.length; i++) {
              sitemapUrl = url + sitemaps[i];
              write(sitemapUrl, context, customInterval, customScore,
                  new HashMap<String, String>(), InjectType.SITEMAP_INJECT);
            }
            continue;
          } else if (splits[s].indexOf("-sitemap") == 0) {
            injectType = InjectType.SITEMAP_INJECT;
            continue;
          } else if (indexEquals == -1) {
            // skip anything without a =
            continue;
          }
          String metaname = splits[s].substring(0, indexEquals);
          String metavalue = splits[s].substring(indexEquals + 1);
          if (metaname.equals(nutchScoreMDName)) {
            try {
              customScore = Float.parseFloat(metavalue);
            } catch (NumberFormatException nfe) {
            }
          } else if (metaname.equals(nutchFetchIntervalMDName)) {
            try {
              customInterval = Integer.parseInt(metavalue);
            } catch (NumberFormatException nfe) {
            }
          } else
            metadata.put(metaname, metavalue);
        }
      }
      write(url, context, customInterval, customScore, metadata, injectType);
    }

    private void write(String url, Context context, Integer customInterval,
        Float customScore, Map<String, String> metadata, InjectType injectType)
            throws IOException, InterruptedException {
      try {
        url = urlNormalizers.normalize(url, URLNormalizers.SCOPE_INJECT);
        url = filters.filter(url); // filter the url
      } catch (Exception e) {
        LOG.warn("Skipping " + url + ":" + e);
        url = null;
      }
      if (url == null) {
        context.getCounter("injector", "urls_filtered").increment(1);
        return;
      } else { // if it passes
        String reversedUrl = TableUtil.reverseUrl(url); // collect it
        WebPage row = WebPage.newBuilder().build();
        row.setFetchTime(curTime);
        row.setFetchInterval(customInterval);

        // now add the metadata
        Iterator<String> keysIter = metadata.keySet().iterator();
        while (keysIter.hasNext()) {
          String keymd = keysIter.next();
          String valuemd = metadata.get(keymd);
          row.getMetadata().put(new Utf8(keymd),
              ByteBuffer.wrap(valuemd.getBytes(StandardCharsets.UTF_8)));
        }

        if (customScore != -1)
          row.setScore(customScore);
        else
          row.setScore(scoreInjected);

        try {
          scfilters.injectedScore(url, row);
        } catch (ScoringFilterException e) {
          if (LOG.isWarnEnabled()) {
            LOG.warn("Cannot filter injected score for url {}, using default ({})", url, e.getMessage());
          }
        }
        context.getCounter("injector", "urls_injected").increment(1);
        row.getMarkers()
        .put(DbUpdaterJob.DISTANCE, new Utf8(String.valueOf(0)));
        Mark.INJECT_MARK.putMark(row, injectType.getTypeString());
        context.write(reversedUrl, row);
      }
    }
  }

  public InjectorJob() {
  }

  public InjectorJob(Configuration conf) {
    setConf(conf);
  }

  public Map<String, Object> run(Map<String, Object> args) throws Exception {
    getConf().setLong("injector.current.time", System.currentTimeMillis());
    Path input;
    Object path = args.get(Nutch.ARG_SEEDDIR);
    if (path instanceof Path) {
      input = (Path) path;
    } else {
      input = new Path(path.toString());
    }
    numJobs = 1;
    currentJobNum = 0;
    currentJob = NutchJob.getInstance(getConf(), "inject " + input);
    FileInputFormat.addInputPath(currentJob, input);
    currentJob.setMapperClass(UrlMapper.class);
    currentJob.setMapOutputKeyClass(String.class);
    currentJob.setMapOutputValueClass(WebPage.class);
    currentJob.setOutputFormatClass(GoraOutputFormat.class);

    DataStore<String, WebPage> store = StorageUtils.createWebStore(
        currentJob.getConfiguration(), String.class, WebPage.class);
    GoraOutputFormat.setOutput(currentJob, store, true);

    // NUTCH-1471 Make explicit which datastore class we use
    Class<? extends DataStore<Object, Persistent>> dataStoreClass = StorageUtils
        .getDataStoreClass(currentJob.getConfiguration());
    LOG.info("InjectorJob: Using " + dataStoreClass
        + " as the Gora storage class.");

    currentJob.setReducerClass(Reducer.class);
    currentJob.setNumReduceTasks(0);

    currentJob.waitForCompletion(true);
    ToolUtil.recordJobStatus(null, currentJob, results);

    // NUTCH-1370 Make explicit #URLs injected @runtime
    long urlsInjected = currentJob.getCounters()
        .findCounter("injector", "urls_injected").getValue();
    long urlsFiltered = currentJob.getCounters()
        .findCounter("injector", "urls_filtered").getValue();
    LOG.info("InjectorJob: total number of urls rejected by filters: "
        + urlsFiltered);
    LOG.info("InjectorJob: total number of urls injected after normalization and filtering: "
        + urlsInjected);

    return results;
  }

  public void inject(Path urlDir) throws Exception {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ROOT);
    long start = System.currentTimeMillis();
    LOG.info("InjectorJob: starting at " + sdf.format(start));
    LOG.info("InjectorJob: Injecting urlDir: " + urlDir);
    run(ToolUtil.toArgMap(Nutch.ARG_SEEDDIR, urlDir));
    long end = System.currentTimeMillis();
    LOG.info("Injector: finished at " + sdf.format(end) + ", elapsed: "
        + TimingUtil.elapsedTime(start, end));
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 1) {
      System.err.println("Usage: InjectorJob <url_dir> [-crawlId <id>]");
      return -1;
    }
    for (int i = 1; i < args.length; i++) {
      if ("-crawlId".equals(args[i])) {
        getConf().set(Nutch.CRAWL_ID_KEY, args[i + 1]);
        i++;
      } else {
        System.err.println("Unrecognized arg " + args[i]);
        return -1;
      }
    }

    try {
      inject(new Path(args[0]));
      return -0;
    } catch (Exception e) {
      LOG.error("InjectorJob: " + StringUtils.stringifyException(e));
      return -1;
    }
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(), new InjectorJob(),
        args);
    System.exit(res);
  }
}
