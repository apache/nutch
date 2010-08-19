package org.apache.nutch.crawl;

import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.regex.Pattern;

import org.apache.avro.util.Utf8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import org.apache.nutch.parse.ParseStatusUtils;
import org.apache.nutch.protocol.ProtocolStatusUtils;
import org.apache.nutch.storage.ParseStatus;
import org.apache.nutch.storage.ProtocolStatus;
import org.apache.nutch.storage.StorageUtils;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.Bytes;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.StringUtil;
import org.apache.nutch.util.TableUtil;
import org.gora.mapreduce.GoraMapper;
import org.gora.query.Query;
import org.gora.query.Result;
import org.gora.store.DataStore;

/**
 * Displays information about the entries of the webtable
 **/

public class WebTableReader extends Configured implements Tool {

  public static final Logger LOG = LoggerFactory.getLogger(WebTableReader.class);

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
      if (totalCnt==null)totalCnt=new LongWritable(0);
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
  private void read(String key, boolean dumpContent, boolean dumpHeaders,
      boolean dumpLinks, boolean dumpText) throws ClassNotFoundException, IOException {
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
      System.out.println(getPageRepresentation(url, page, dumpContent,
          dumpHeaders, dumpLinks, dumpText));
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
    static final String contentParamName = "webtable.dump.content";
    static final String linksParamName = "webtable.dump.links";
    static final String textParamName = "webtable.dump.text";
    static final String headersParamName = "webtable.dump.headers";

    public WebTableRegexMapper() {
    }

    private Pattern regex = null;
    private boolean dumpContent, dumpHeaders, dumpLinks, dumpText;

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
            new Text(getPageRepresentation(key, value, dumpContent, dumpHeaders,
                dumpLinks, dumpText)));
      }
    }

    @Override
    protected void setup(
        org.apache.hadoop.mapreduce.Mapper<String, WebPage, Text, Text>.Context context)
        throws IOException, InterruptedException {
      regex = Pattern.compile(context.getConfiguration().get(regexParamName,
          ".+"));
      dumpContent = context.getConfiguration().getBoolean(contentParamName, false);
      dumpHeaders = context.getConfiguration().getBoolean(headersParamName, false);
      dumpLinks = context.getConfiguration().getBoolean(linksParamName, false);
      dumpText = context.getConfiguration().getBoolean(textParamName, false);
    }

  }

  public void processDumpJob(String output, Configuration config, String regex,
      boolean content, boolean headers, boolean links, boolean text)
      throws IOException, ClassNotFoundException, InterruptedException {

    if (LOG.isInfoEnabled()) {
      LOG.info("WebTable dump: starting");
    }

    Path outFolder = new Path(output);
    Job job = new NutchJob(getConf(), "db_dump");
    Configuration cfg = job.getConfiguration();
    cfg.set(WebTableRegexMapper.regexParamName, regex);
    cfg.setBoolean(WebTableRegexMapper.contentParamName, content);
    cfg.setBoolean(WebTableRegexMapper.headersParamName, headers);
    cfg.setBoolean(WebTableRegexMapper.linksParamName, links);
    cfg.setBoolean(WebTableRegexMapper.textParamName, text);
    
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

  private static String getPageRepresentation(String key, WebPage page,
      boolean dumpContent, boolean dumpHeaders, boolean dumpLinks, boolean dumpText) {
    StringBuffer sb = new StringBuffer();
    sb.append("key:\t" + key).append("\n");
    sb.append("baseUrl:\t" + page.getBaseUrl()).append("\n");
    sb.append("status:\t").append(page.getStatus()).append(" (").append(
        CrawlStatus.getName((byte) page.getStatus())).append(")\n");
    sb.append("fetchInterval:\t" + page.getFetchInterval()).append("\n");
    sb.append("fetchTime:\t" + page.getFetchTime()).append("\n");
    sb.append("prevFetchTime:\t" + page.getPrevFetchTime()).append("\n");
    sb.append("retries:\t" + page.getRetriesSinceFetch()).append("\n");
    sb.append("modifiedTime:\t" + page.getModifiedTime()).append("\n");
    sb.append("protocolStatus:\t" + 
        ProtocolStatusUtils.toString(page.getProtocolStatus())).append("\n");
    sb.append("parseStatus:\t" + 
        ParseStatusUtils.toString(page.getParseStatus())).append("\n");
    sb.append("title:\t" + page.getTitle()).append("\n");
    sb.append("score:\t" + page.getScore()).append("\n");
    ByteBuffer sig = page.getSignature();
    if (sig != null) {
      sb.append("signature:\t" + StringUtil.toHexString(sig.array())).append("\n");
    }
    Map<Utf8, Utf8> markers = page.getMarkers();
    sb.append("markers:\t" + markers).append("\n");

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
    if (dumpLinks) {
      Map<Utf8,Utf8> inlinks = page.getInlinks();
      Map<Utf8,Utf8> outlinks = page.getOutlinks();
      if (outlinks != null) {
        for (Entry<Utf8,Utf8> e : outlinks.entrySet()) {
          sb.append("outlink:\t" + e.getKey() + "\t" + e.getValue() + "\n");
        }
      }
      if (inlinks != null) {
        for (Entry<Utf8,Utf8> e : inlinks.entrySet()) {
          sb.append("inlink:\t" + e.getKey() + "\t" + e.getValue() + "\n");
        }
      }
    }
    if (dumpHeaders) {
      Map<Utf8,Utf8> headers = page.getHeaders();
      if (headers != null) {
        for (Entry<Utf8,Utf8> e : headers.entrySet()) {
          sb.append("header:\t" + e.getKey() + "\t" + e.getValue() + "\n");
        }        
      }
    }
    ByteBuffer content = page.getContent();
    if (content != null && dumpContent) {
      sb.append("contentType:\t" + page.getContentType()).append("\n");
      sb.append("content:start:\n");
      sb.append(Bytes.toString(content.array()));
      sb.append("\ncontent:end:\n");
    }
    Utf8 text = page.getText();
    if (text != null && dumpText) {
      sb.append("text:start:\n");
      sb.append(text.toString());
      sb.append("\ntext:end:\n");      
    }
    
    return sb.toString();
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(), new WebTableReader(),
        args);
    System.exit(res);
  }
  
  private static enum Op {READ, STAT, DUMP};

  public int run(String[] args) throws Exception {
    if (args.length < 1) {
      System.err
          .println("Usage: WebTableReader (-stats | -url [url] | -dump <out_dir> [-regex regex]) [-content] [-headers] [-links] [-text]");
      System.err
          .println("\t-stats [-sort] \tprint overall statistics to System.out");
      System.err.println("\t\t[-sort]\tlist status sorted by host");
      System.err
          .println("\t-url <url>\tprint information on <url> to System.out");
      System.err
          .println("\t-dump <out_dir> [-regex regex]\tdump the webtable to a text file in <out_dir>");
      System.err.println("\t\t-content\tdump also raw content");
      System.err.println("\t\t-headers\tdump protocol headers");
      System.err.println("\t\t-links\tdump links");
      System.err.println("\t\t-text\tdump extracted text");
      System.err
          .println("\t\t[-regex]\tfilter on the URL of the webtable entry");
      return -1;
    }
    String param = null;
    boolean content = false;
    boolean links = false;
    boolean text = false;
    boolean headers = false;
    boolean toSort = false;
    String regex = ".+";
    Op op = null;
    try {
      for (int i = 0; i < args.length; i++) {
        if (args[i].equals("-url")) {
          param = args[++i];
          op = Op.READ;
          //read(param);
          //return 0;
        } else if (args[i].equals("-stats")) {
          op = op.STAT;
        } else if (args[i].equals("-sort")) {
          toSort = true;
        } else if (args[i].equals("-dump")) {
          op = Op.DUMP;
          param = args[++i];
        } else if (args[i].equals("-content")) {
          content = true;
        } else if (args[i].equals("-headers")) {
          headers = true;
        } else if (args[i].equals("-links")) {
          links = true;
        } else if (args[i].equals("-text")) {
          text = true;
        } else if (args[i].equals("-regex")) {
          regex = args[++i];
        }
      }
      if (op == null) {
        throw new Exception("Select one of -url | -stat | -dump");
      }
      switch (op) {
      case READ:
        read(param, content, headers, links, text);
        break;
      case STAT:
        processStatJob(toSort);
        break;
      case DUMP:
        processDumpJob(param, getConf(), regex, content, headers, links, text);
        break;
      }
      return 0;
    } catch (Exception e) {
      LOG.error("WebTableReader: " + StringUtils.stringifyException(e));
      return -1;
    }
  }

}
