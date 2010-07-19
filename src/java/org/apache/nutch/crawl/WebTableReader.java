package org.apache.nutch.crawl;

import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.avro.util.Utf8;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.storage.StorageUtils;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.TableUtil;
import org.gora.mapreduce.GoraMapper;
import org.gora.query.Query;
import org.gora.query.Result;
import org.gora.store.DataStore;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Displays information about the entries of the webtable
 **/

public class WebTableReader extends Configured implements Tool {

  public static final Log LOG = LogFactory.getLog(WebTableReader.class);

  public static class WebTableStatMapper extends
      GoraMapper<String, WebPage, Text, LongWritable> {
    LongWritable COUNT_1 = new LongWritable(1);
    private boolean sort = false;

    public WebTableStatMapper() {
    }

    public void setup(Context context) {
      sort = context.getConfiguration().getBoolean("db.reader.stats.sort",
          false);
    }

    public void close() {
    }

    @Override
    protected void map(
        String key,
        WebPage value,
        org.apache.hadoop.mapreduce.Mapper<String, WebPage, Text, LongWritable>.Context context)
        throws IOException, InterruptedException {
      context.write(new Text("T"), COUNT_1);
      context.write(new Text("status " + value.getStatus()), COUNT_1);
      context.write(new Text("retry " + value.getRetriesSinceFetch()), COUNT_1);
      context.write(new Text("s"), new LongWritable(
          (long) (value.getScore() * 1000.0)));
      if (sort) {
        URL u = new URL(TableUtil.unreverseUrl(key.toString()));
        String host = u.getHost();
        context.write(new Text("status " + value.getStatus() + " " + host),
            COUNT_1);
      }

    }
  }

  public static class WebTableStatCombiner extends
      Reducer<Text, LongWritable, Text, LongWritable> {
    LongWritable val = new LongWritable();

    public void setup(Context context) {
    }

    public void cleanup(Context context) {
    }

    @Override
    public void reduce(
        Text key,
        Iterable<LongWritable> values,
        org.apache.hadoop.mapreduce.Reducer<Text, LongWritable, Text, LongWritable>.Context context)
        throws IOException, InterruptedException {
      val.set(0L);
      Iterator<LongWritable> iter = values.iterator();
      String k = ((Text) key).toString();
      if (!k.equals("s")) {
        while (iter.hasNext()) {
          LongWritable cnt = iter.next();
          val.set(val.get() + cnt.get());
        }
        context.write(key, val);
      } else {
        long total = 0;
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        while (iter.hasNext()) {
          LongWritable cnt = iter.next();
          if (cnt.get() < min)
            min = cnt.get();
          if (cnt.get() > max)
            max = cnt.get();
          total += cnt.get();
        }
        context.write(new Text("scn"), new LongWritable(min));
        context.write(new Text("scx"), new LongWritable(max));
        context.write(new Text("sct"), new LongWritable(total));
      }
    }

  }

  public static class WebTableStatReducer extends
      Reducer<Text, LongWritable, Text, LongWritable> {

    public void cleanup(Context context) {
    }

    @Override
    protected void reduce(
        Text key,
        Iterable<LongWritable> values,
        org.apache.hadoop.mapreduce.Reducer<Text, LongWritable, Text, LongWritable>.Context context)
        throws IOException, InterruptedException {
      Iterator<LongWritable> iter = values.iterator();
      String k = ((Text) key).toString();
      if (k.equals("T")) {
        // sum all values for this key
        long sum = 0;
        while (iter.hasNext()) {
          sum += iter.next().get();
        }
        // output sum
        context.write(key, new LongWritable(sum));
      } else if (k.startsWith("status") || k.startsWith("retry")) {
        LongWritable cnt = new LongWritable();
        while (iter.hasNext()) {
          LongWritable val = iter.next();
          cnt.set(cnt.get() + val.get());
        }
        context.write(key, cnt);
      } else if (k.equals("scx")) {
        LongWritable cnt = new LongWritable(Long.MIN_VALUE);
        while (iter.hasNext()) {
          LongWritable val = iter.next();
          if (cnt.get() < val.get())
            cnt.set(val.get());
        }
        context.write(key, cnt);
      } else if (k.equals("scn")) {
        LongWritable cnt = new LongWritable(Long.MAX_VALUE);
        while (iter.hasNext()) {
          LongWritable val = iter.next();
          if (cnt.get() > val.get())
            cnt.set(val.get());
        }
        context.write(key, cnt);
      } else if (k.equals("sct")) {
        LongWritable cnt = new LongWritable();
        while (iter.hasNext()) {
          LongWritable val = iter.next();
          cnt.set(cnt.get() + val.get());
        }
        context.write(key, cnt);
      }
    }

  }

  public void processStatJob(boolean sort) throws IOException,
      ClassNotFoundException, InterruptedException {

    if (LOG.isInfoEnabled()) {
      LOG.info("WebTable statistics start");
    }

    Path tmpFolder = new Path(getConf().get("mapred.temp.dir", ".")
        + "stat_tmp" + System.currentTimeMillis());

    Job job = new NutchJob(getConf(), "db_stats");

    job.getConfiguration().setBoolean("db.reader.stats.sort", sort);

    DataStore<String, WebPage> store = StorageUtils.createDataStore(job
        .getConfiguration(), String.class, WebPage.class);
    Query<String, WebPage> query = store.newQuery();
    query.setFields(WebPage._ALL_FIELDS);

    GoraMapper.initMapperJob(job, query, store, Text.class, LongWritable.class,
        WebTableStatMapper.class, null, true);

    job.setCombinerClass(WebTableStatCombiner.class);
    job.setReducerClass(WebTableStatReducer.class);

    FileOutputFormat.setOutputPath(job, tmpFolder);

    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);

    boolean success = job.waitForCompletion(true);

    FileSystem fileSystem = FileSystem.get(getConf());

    if (!success) {
      fileSystem.delete(tmpFolder, true);
      return;
    }

    Text key = new Text();
    LongWritable value = new LongWritable();

    SequenceFile.Reader[] readers = org.apache.hadoop.mapred.SequenceFileOutputFormat
        .getReaders(getConf(), tmpFolder);

    TreeMap<String, LongWritable> stats = new TreeMap<String, LongWritable>();
    for (int i = 0; i < readers.length; i++) {
      SequenceFile.Reader reader = readers[i];
      while (reader.next(key, value)) {
        String k = key.toString();
        LongWritable val = stats.get(k);
        if (val == null) {
          val = new LongWritable();
          if (k.equals("scx"))
            val.set(Long.MIN_VALUE);
          if (k.equals("scn"))
            val.set(Long.MAX_VALUE);
          stats.put(k, val);
        }
        if (k.equals("scx")) {
          if (val.get() < value.get())
            val.set(value.get());
        } else if (k.equals("scn")) {
          if (val.get() > value.get())
            val.set(value.get());
        } else {
          val.set(val.get() + value.get());
        }
      }
      reader.close();
    }

    if (LOG.isInfoEnabled()) {
      LOG.info("Statistics for WebTable: ");
      LongWritable totalCnt = stats.get("T");
      stats.remove("T");
      LOG.info("TOTAL urls:\t" + totalCnt.get());
      for (Map.Entry<String, LongWritable> entry : stats.entrySet()) {
        String k = entry.getKey();
        LongWritable val = entry.getValue();
        if (k.equals("scn")) {
          LOG.info("min score:\t" + (float) (val.get() / 1000.0f));
        } else if (k.equals("scx")) {
          LOG.info("max score:\t" + (float) (val.get() / 1000.0f));
        } else if (k.equals("sct")) {
          LOG.info("avg score:\t"
              + (float) ((((double) val.get()) / totalCnt.get()) / 1000.0));
        } else if (k.startsWith("status")) {
          String[] st = k.split(" ");
          int code = Integer.parseInt(st[1]);
          if (st.length > 2)
            LOG.info("   " + st[2] + " :\t" + val);
          else
            LOG.info(st[0] + " " + code + " ("
                + CrawlStatus.getName((byte) code) + "):\t" + val);
        } else
          LOG.info(k + ":\t" + val);
      }
    }
    // removing the tmp folder
    fileSystem.delete(tmpFolder, true);
    if (LOG.isInfoEnabled()) {
      LOG.info("WebTable statistics: done");
    }

  }

  /** Prints out the entry to the standard out **/
  private void read(String key) throws ClassNotFoundException, IOException {
    DataStore<String, WebPage> datastore = StorageUtils.createDataStore(getConf(),
        String.class, WebPage.class);

    Query<String, WebPage> query = datastore.newQuery();
    String reversedUrl = TableUtil.reverseUrl(key);
    query.setKey(reversedUrl);

    Result<String, WebPage> result = datastore.execute(query);
    boolean found = false;
    // should happen only once
    while (result.next()) {
      WebPage page = result.get();
      String skey = result.getKey();
      // we should not get to this point but nevermind
      if (page == null || skey == null)
        break;
      found = true;
      String url = TableUtil.unreverseUrl(skey);
      System.out.println(getPageRepresentation(url, page));
    }
    if (!found)
      System.out.println(key + " not found");
    result.close();
    datastore.close();
  }

  /** Filters the entries from the table based on a regex **/
  public static class WebTableRegexMapper extends
      GoraMapper<String, WebPage, Text, Text> {

    static final String regexParamName = "webtable.url.regex";

    public WebTableRegexMapper() {
    }

    private Pattern regex = null;

    @Override
    protected void map(
        String key,
        WebPage value,
        org.apache.hadoop.mapreduce.Mapper<String, WebPage, Text, Text>.Context context)
        throws IOException, InterruptedException {
      // checks whether the Key passes the regex
      String url = TableUtil.unreverseUrl(key.toString());
      if (regex.matcher(url).matches()) {
        context.write(new Text(url),
            new Text(getPageRepresentation(key, value)));
      }
    }

    @Override
    protected void setup(
        org.apache.hadoop.mapreduce.Mapper<String, WebPage, Text, Text>.Context context)
        throws IOException, InterruptedException {
      regex = Pattern.compile(context.getConfiguration().get(regexParamName,
          ".+"));
    }

  }

  public void processDumpJob(String output, Configuration config, String regex)
      throws IOException, ClassNotFoundException, InterruptedException {

    if (LOG.isInfoEnabled()) {
      LOG.info("WebTable dump: starting");
    }

    Path outFolder = new Path(output);
    Job job = new NutchJob(getConf(), "db_dump");

    job.getConfiguration().set(WebTableRegexMapper.regexParamName, regex);

    DataStore<String, WebPage> store = StorageUtils.createDataStore(job
        .getConfiguration(), String.class, WebPage.class);
    Query<String, WebPage> query = store.newQuery();
    query.setFields(WebPage._ALL_FIELDS);

    GoraMapper.initMapperJob(job, query, store, Text.class, Text.class,
        WebTableRegexMapper.class, null, true);

    FileOutputFormat.setOutputPath(job, outFolder);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    boolean success = job.waitForCompletion(true);

    if (LOG.isInfoEnabled()) {
      LOG.info("WebTable dump: done");
    }
  }

  private static String getPageRepresentation(String key, WebPage page) {
    StringBuffer sb = new StringBuffer();
    sb.append(key).append("\n");
    sb.append("baseUrl:\t" + page.getBaseUrl()).append("\n");
    sb.append("status:\t").append(page.getStatus()).append(" (").append(
        CrawlStatus.getName((byte) page.getStatus())).append(")\n");
    sb.append("parse_status:\t" + page.getParseStatus()).append("\n");
    sb.append("title:\t" + page.getTitle()).append("\n");
    sb.append("score:\t" + page.getScore()).append("\n");

    Map<Utf8, ByteBuffer> metadata = page.getMetadata();
    if (metadata != null) {
      Iterator<Entry<Utf8, ByteBuffer>> iterator = metadata.entrySet()
          .iterator();
      while (iterator.hasNext()) {
        Entry<Utf8, ByteBuffer> entry = iterator.next();
        sb.append("metadata " + entry.getKey().toString()).append(" : \t")
            .append(Bytes.toString(entry.getValue().array())).append("\n");
      }
    }
    return sb.toString();
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(), new WebTableReader(),
        args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    if (args.length < 1) {
      System.err
          .println("Usage: WebTableReader (-stats | -url [url] | -dump <out_dir> [-regex regex])");
      System.err
          .println("\t-stats [-sort] \tprint overall statistics to System.out");
      System.err.println("\t\t[-sort]\tlist status sorted by host");
      System.err
          .println("\t-url <url>\tprint information on <url> to System.out");
      System.err
          .println("\t-dump <out_dir> [-regex regex]\tdump the webtable to a text file in <out_dir>");
      System.err
          .println("\t\t[-regex]\tfilter on the URL of the webtable entry");
      return -1;
    }
    String param = null;
    try {
      for (int i = 0; i < args.length; i++) {
        if (args[i].equals("-url")) {
          param = args[++i];
          read(param);
          return 0;
        } else if (args[i].equals("-stats")) {
          boolean toSort = false;
          if (i < args.length - 1 && "-sort".equals(args[i + 1])) {
            toSort = true;
            i++;
          }
          processStatJob(toSort);
        } else if (args[i].equals("-dump")) {
          param = args[++i];
          String regex = ".+";
          if (i < args.length - 1 && "-regex".equals(args[i + 1]))
            regex = args[i = i + 2];
          processDumpJob(param, getConf(), regex);
        }
      }
    } catch (Exception e) {
      LOG.fatal("WebTableReader: " + StringUtils.stringifyException(e));
      return -1;
    }
    return 0;
  }

}
