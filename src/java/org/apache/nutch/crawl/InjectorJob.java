package org.apache.nutch.crawl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.avro.util.Utf8;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.StorageUtils;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.TableUtil;
import org.gora.mapreduce.GoraMapper;
import org.gora.mapreduce.GoraOutputFormat;

/** This class takes a flat file of URLs and adds them to the of pages to be
 * crawled.  Useful for bootstrapping the system.
 * The URL files contain one URL per line, optionally followed by custom metadata
 * separated by tabs with the metadata key separated from the corresponding value by '='. <br>
 * Note that some metadata keys are reserved : <br>
 * - <i>nutch.score</i> : allows to set a custom score for a specific URL <br>
 * - <i>nutch.fetchInterval</i> : allows to set a custom fetch interval for a specific URL <br>
 * e.g. http://www.nutch.org/ \t nutch.score=10 \t nutch.fetchInterval=2592000 \t userType=open_source
 **/
public class InjectorJob extends GoraMapper<String, WebPage, String, WebPage>
    implements Tool {

  public static final Log LOG = LogFactory.getLog(InjectorJob.class);

  private Configuration conf;

  private FetchSchedule schedule;

  private static final Set<WebPage.Field> FIELDS = new HashSet<WebPage.Field>();

  private static final Utf8 YES_STRING = new Utf8("y");

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

  public static class UrlMapper extends
      Mapper<LongWritable, Text, String, WebPage> {
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

    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      String url = value.toString();

      // if tabs : metadata that could be stored
      // must be name=value and separated by \t
      float customScore = -1f;
      int customInterval = interval;
      Map<String, String> metadata = new TreeMap<String, String>();
      if (url.indexOf("\t") != -1) {
        String[] splits = url.split("\t");
        url = splits[0];
        for (int s = 1; s < splits.length; s++) {
          // find separation between name and value
          int indexEquals = splits[s].indexOf("=");
          if (indexEquals == -1) {
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
      try {
        url = urlNormalizers.normalize(url, URLNormalizers.SCOPE_INJECT);
        url = filters.filter(url); // filter the url
      } catch (Exception e) {
        LOG.warn("Skipping " + url + ":" + e);
        url = null;
      }
      if (url == null)
        return;

      String reversedUrl = TableUtil.reverseUrl(url);
      WebPage row = new WebPage();
      row.setFetchTime(curTime);
      row.setFetchInterval(customInterval);

      // now add the metadata
      Iterator<String> keysIter = metadata.keySet().iterator();
      while (keysIter.hasNext()) {
          String keymd = keysIter.next();
          String valuemd = metadata.get(keymd);
          row.putToMetadata(new Utf8(keymd), ByteBuffer.wrap(valuemd.getBytes()));
      }


      if (customScore != -1)
        row.setScore(customScore);
      else {
        row.setScore(scoreInjected);
        try {
          scfilters.injectedScore(url, row);
        } catch (ScoringFilterException e) {
          if (LOG.isWarnEnabled()) {
            LOG.warn("Cannot filter injected score for url " + url
                + ", using default (" + e.getMessage() + ")");
          }
          row.setScore(scoreInjected);
        }
      }

      Mark.INJECT_MARK.putMark(row, YES_STRING);
      context.write(reversedUrl, row);
    }
  }

  public InjectorJob() {
    
  }
  
  public InjectorJob(Configuration conf) {
    setConf(conf);
  }
  
  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public void setup(Context context) throws IOException {
    Configuration conf = context.getConfiguration();
    schedule = FetchScheduleFactory.getFetchSchedule(conf);
    // scoreInjected = conf.getFloat("db.score.injected", 1.0f);
  }

  @Override
  protected void map(String key, WebPage row, Context context)
      throws IOException, InterruptedException {
    if (Mark.INJECT_MARK.checkMark(row) == null) {
      return;
    }
    Mark.INJECT_MARK.removeMark(row);
    if (!row.isReadable(WebPage.Field.STATUS.getIndex())) {
      row.setStatus(CrawlStatus.STATUS_UNFETCHED);
      schedule.initializeSchedule(key, row);
      // row.setScore(scoreInjected);
    }

    context.write(key, row);
  }

  public void inject(Path urlDir) throws Exception {
    LOG.info("InjectorJob: starting");
    LOG.info("InjectorJob: urlDir: " + urlDir);

    getConf().setLong("injector.current.time", System.currentTimeMillis());
    Job job = new NutchJob(getConf(), "inject-p1 " + urlDir);
    FileInputFormat.addInputPath(job, urlDir);
    job.setMapperClass(UrlMapper.class);
    job.setMapOutputKeyClass(String.class);
    job.setMapOutputValueClass(WebPage.class);
    job.setOutputFormatClass(GoraOutputFormat.class);
    GoraOutputFormat.setOutput(job, String.class,
        WebPage.class, StorageUtils.getDataStoreClass(getConf()), true);
    job.setReducerClass(Reducer.class);
    job.setNumReduceTasks(0);
    job.waitForCompletion(true);

    job = new NutchJob(getConf(), "inject-p2 " + urlDir);
    StorageUtils.initMapperJob(job, FIELDS, String.class, WebPage.class,
        InjectorJob.class);
    job.setNumReduceTasks(0);
    job.waitForCompletion(true);
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 1) {
      System.err.println("Usage: InjectorJob <url_dir>");
      return -1;
    }
    try {
      inject(new Path(args[0]));
      LOG.info("InjectorJob: finished");
      return -0;
    } catch (Exception e) {
      LOG.fatal("InjectorJob: " + StringUtils.stringifyException(e));
      return -1;
    }
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(), new InjectorJob(), args);
    System.exit(res);
  }
}
