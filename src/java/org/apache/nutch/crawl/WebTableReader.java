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
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.parse.ParseStatusUtils;
import org.apache.nutch.protocol.ProtocolStatusUtils;
import org.apache.nutch.storage.StorageUtils;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.Bytes;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.NutchTool;
import org.apache.nutch.util.StringUtil;
import org.apache.nutch.util.TableUtil;
import org.apache.nutch.util.ToolUtil;
import org.apache.gora.mapreduce.GoraMapper;
import org.apache.gora.query.Query;
import org.apache.gora.query.Result;
import org.apache.gora.store.DataStore;

/**
 * Displays information about the entries of the webtable
 **/

public class WebTableReader extends NutchTool implements Tool {

  public static final Logger LOG = LoggerFactory.getLogger(WebTableReader.class);

  public static class WebTableStatMapper extends
      GoraMapper<String, WebPage, Text, LongWritable> {
    LongWritable COUNT_1 = new LongWritable(1);
    private boolean sort = false;

    public WebTableStatMapper() {
    }

    @Override
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

    @Override
    public void setup(Context context) {
    }

    @Override
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
      String k = key.toString();
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

    @Override
    public void cleanup(Context context) {
    }

    @Override
    protected void reduce(
        Text key,
        Iterable<LongWritable> values,
        org.apache.hadoop.mapreduce.Reducer<Text, LongWritable, Text, LongWritable>.Context context)
        throws IOException, InterruptedException {
      Iterator<LongWritable> iter = values.iterator();
      String k = key.toString();
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

  public void processStatJob(boolean sort) throws Exception {

    if (LOG.isInfoEnabled()) {
      LOG.info("WebTable statistics start");
    }
    run(ToolUtil.toArgMap(Nutch.ARG_SORT, sort));
    for (Entry<String,Object> e : results.entrySet()) {
      LOG.info(e.getKey() + ":\t" + e.getValue());
    }
  }

  /** Prints out the entry to the standard out **/
  private void read(String key, boolean dumpContent, boolean dumpHeaders,
      boolean dumpLinks, boolean dumpText) throws ClassNotFoundException, IOException, Exception {
    DataStore<String, WebPage> datastore = StorageUtils.createWebStore(getConf(),
        String.class, WebPage.class);

    Query<String, WebPage> query = datastore.newQuery();
    String reversedUrl = TableUtil.reverseUrl(key);
    query.setKey(reversedUrl);

    Result<String, WebPage> result = datastore.execute(query);
    boolean found = false;
    // should happen only once
    while (result.next()) {
      try {
        WebPage page = result.get();
        String skey = result.getKey();
        // we should not get to this point but nevermind
        if (page == null || skey == null)
          break;
        found = true;
        String url = TableUtil.unreverseUrl(skey);
        System.out.println(getPageRepresentation(url, page, dumpContent,
            dumpHeaders, dumpLinks, dumpText));
      }catch (Exception e) {
        e.printStackTrace();
      }
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

    DataStore<String, WebPage> store = StorageUtils.createWebStore(job
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
    sb.append("fetchTime:\t" + page.getFetchTime()).append("\n");
    sb.append("prevFetchTime:\t" + page.getPrevFetchTime()).append("\n");
    sb.append("fetchInterval:\t" + page.getFetchInterval()).append("\n"); 
    sb.append("retriesSinceFetch:\t" + page.getRetriesSinceFetch()).append("\n");
    sb.append("modifiedTime:\t" + page.getModifiedTime()).append("\n");
    sb.append("prevModifiedTime:\t" + page.getPrevModifiedTime()).append("\n");
    sb.append("protocolStatus:\t" +
        ProtocolStatusUtils.toString(page.getProtocolStatus())).append("\n");
    ByteBuffer prevSig = page.getPrevSignature();
        if (prevSig != null) {
      sb.append("prevSignature:\t" + StringUtil.toHexString(prevSig)).append("\n");
    }
    ByteBuffer sig = page.getSignature();
    if (sig != null) {
      sb.append("signature:\t" + StringUtil.toHexString(sig)).append("\n");
    }
    sb.append("parseStatus:\t" +
        ParseStatusUtils.toString(page.getParseStatus())).append("\n");
    sb.append("title:\t" + page.getTitle()).append("\n");
    sb.append("score:\t" + page.getScore()).append("\n");

    Map<Utf8, Utf8> markers = page.getMarkers();
    sb.append("markers:\t" + markers).append("\n");
    sb.append("reprUrl:\t" + page.getReprUrl()).append("\n");
    Utf8 batchId = page.getBatchId();
    if (batchId != null) {
      sb.append("batchId:\t" + batchId.toString()).append("\n");
    }
    Map<Utf8, ByteBuffer> metadata = page.getMetadata();
    if (metadata != null) {
      Iterator<Entry<Utf8, ByteBuffer>> iterator = metadata.entrySet()
          .iterator();
      while (iterator.hasNext()) {
        Entry<Utf8, ByteBuffer> entry = iterator.next();
        sb.append("metadata " + entry.getKey().toString()).append(" : \t")
            .append(Bytes.toString(entry.getValue())).append("\n");
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
      sb.append(Bytes.toString(content));
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
          .println("Usage: WebTableReader (-stats | -url [url] | -dump <out_dir> [-regex regex]) \n \t \t      [-crawlId <id>] [-content] [-headers] [-links] [-text]");
      System.err.println("    -crawlId <id>  - the id to prefix the schemas to operate on, \n \t \t     (default: storage.crawl.id)");
      System.err.println("    -stats [-sort] - print overall statistics to System.out");
      System.err.println("    [-sort]        - list status sorted by host");
      System.err.println("    -url <url>     - print information on <url> to System.out");
      System.err.println("    -dump <out_dir> [-regex regex] - dump the webtable to a text file in \n \t \t     <out_dir>");
      System.err.println("    -content       - dump also raw content");
      System.err.println("    -headers       - dump protocol headers");
      System.err.println("    -links         - dump links");
      System.err.println("    -text          - dump extracted text");
      System.err.println("    [-regex]       - filter on the URL of the webtable entry");
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
          op = Op.STAT;
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
        } else if (args[i].equals("-crawlId")) {
          getConf().set(Nutch.CRAWL_ID_KEY, args[++i]);
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

  // for now handles only -stat
  @Override
  public Map<String,Object> run(Map<String,Object> args) throws Exception {
    Path tmpFolder = new Path(getConf().get("mapred.temp.dir", ".")
        + "stat_tmp" + System.currentTimeMillis());

    numJobs = 1;
    currentJob = new NutchJob(getConf(), "db_stats");

    currentJob.getConfiguration().setBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs", false);
    
    Boolean sort = (Boolean)args.get(Nutch.ARG_SORT);
    if (sort == null) sort = Boolean.FALSE;
    currentJob.getConfiguration().setBoolean("db.reader.stats.sort", sort);

    DataStore<String, WebPage> store = StorageUtils.createWebStore(currentJob
        .getConfiguration(), String.class, WebPage.class);
    Query<String, WebPage> query = store.newQuery();
    query.setFields(WebPage._ALL_FIELDS);

    GoraMapper.initMapperJob(currentJob, query, store, Text.class, LongWritable.class,
        WebTableStatMapper.class, null, true);

    currentJob.setCombinerClass(WebTableStatCombiner.class);
    currentJob.setReducerClass(WebTableStatReducer.class);

    FileOutputFormat.setOutputPath(currentJob, tmpFolder);

    currentJob.setOutputFormatClass(SequenceFileOutputFormat.class);

    currentJob.setOutputKeyClass(Text.class);
    currentJob.setOutputValueClass(LongWritable.class);
    FileSystem fileSystem = FileSystem.get(getConf());

    try {
      currentJob.waitForCompletion(true);
    } finally {
      ToolUtil.recordJobStatus(null, currentJob, results);
      if (!currentJob.isSuccessful()) {
        fileSystem.delete(tmpFolder, true);
        return results;
      }
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

    LongWritable totalCnt = stats.get("T");
    if (totalCnt==null)totalCnt=new LongWritable(0);
    stats.remove("T");
    results.put("TOTAL urls", totalCnt.get());
    for (Map.Entry<String, LongWritable> entry : stats.entrySet()) {
      String k = entry.getKey();
      LongWritable val = entry.getValue();
      if (k.equals("scn")) {
        results.put("min score", (val.get() / 1000.0f));
      } else if (k.equals("scx")) {
        results.put("max score", (val.get() / 1000.0f));
      } else if (k.equals("sct")) {
        results.put("avg score",
            (float) ((((double) val.get()) / totalCnt.get()) / 1000.0));
      } else if (k.startsWith("status")) {
        String[] st = k.split(" ");
        int code = Integer.parseInt(st[1]);
        if (st.length > 2)
          results.put(st[2], val.get());
        else
          results.put(st[0] + " " + code + " ("
              + CrawlStatus.getName((byte) code) + ")", val.get());
      } else
        results.put(k, val.get());
    }
    // removing the tmp folder
    fileSystem.delete(tmpFolder, true);
    if (LOG.isInfoEnabled()) {
      LOG.info("Statistics for WebTable: ");
      for (Entry<String,Object> e : results.entrySet()) {
        LOG.info(e.getKey() + ":\t" + e.getValue());
      }
      LOG.info("WebTable statistics: done");
    }
    return results;
  }
}
