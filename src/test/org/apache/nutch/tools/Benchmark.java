package org.apache.nutch.tools;

import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.Crawl;
import org.apache.nutch.crawl.CrawlDb;
import org.apache.nutch.crawl.CrawlDbReader;
import org.apache.nutch.crawl.Generator;
import org.apache.nutch.crawl.Injector;
import org.apache.nutch.crawl.LinkDb;
import org.apache.nutch.fetcher.Fetcher;
import org.apache.nutch.indexer.solr.SolrDeleteDuplicates;
import org.apache.nutch.indexer.solr.SolrIndexer;
import org.apache.nutch.parse.ParseSegment;
import org.apache.nutch.util.HadoopFSUtil;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;

public class Benchmark extends Configured implements Tool {
  private static final Log LOG = LogFactory.getLog(Benchmark.class);

  public static void main(String[] args) throws Exception {
    Configuration conf = NutchConfiguration.create();
    int res = ToolRunner.run(conf, new Benchmark(), args);
    System.exit(res);
  }
  
  private static String getDate() {
    return new SimpleDateFormat("yyyyMMddHHmmss").format
      (new Date(System.currentTimeMillis()));
  }
 
  private void createSeeds(FileSystem fs, Path seedsDir, int count) throws Exception {
    OutputStream os = fs.create(new Path(seedsDir, "seeds"));
    for (int i = 0; i < count; i++) {
      String url = "http://www.test-" + i + ".com/\r\n";
      os.write(url.getBytes());
    }
    os.flush();
    os.close();
  }
  
  public int run(String[] args) throws Exception {
    String plugins = "protocol-http|parse-tika|scoring-opic|urlfilter-regex|urlnormalizer-pass";
    int seeds = 1;
    int depth = 10;
    int threads = 10;
    boolean delete = true;
    long topN = Long.MAX_VALUE;
    
    if (args.length == 0) {
      System.err.println("Usage: Benchmark [-seeds NN] [-depth NN] [-threads NN] [-keep] [-maxPerHost NN] [-plugins <regex>]");
      System.err.println("\t-seeds NN\tcreate NN unique hosts in a seed list (default: 1)");
      System.err.println("\t-depth NN\tperform NN crawl cycles (default: 10)");
      System.err.println("\t-threads NN\tuse NN threads per Fetcher task (default: 10)");
      System.err.println("\t-keep\tkeep segment data (default: delete after updatedb)");
      System.err.println("\t-plugins <regex>\toverride 'plugin.includes'.");
      System.err.println("\tNOTE: if not specified, this is reset to: " + plugins);
      System.err.println("\tNOTE: if 'default' is specified then a value set in nutch-default/nutch-site is used.");
      System.err.println("\t-maxPerHost NN\tmax. # of URLs per host in a fetchlist");
      return -1;
    }
    int maxPerHost = Integer.MAX_VALUE;
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-seeds")) {
        seeds = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-threads")) {
        threads = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-depth")) {
        depth = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-keep")) {
        delete = false;
      } else if (args[i].equals("-plugins")) {
        plugins = args[++i];
      } else if (args[i].equalsIgnoreCase("-maxPerHost")) {
        maxPerHost = Integer.parseInt(args[++i]);
      } else {
        LOG.fatal("Invalid argument: '" + args[i] + "'");
        return -1;
      }
    }
    Configuration conf = getConf();
    conf.set("http.proxy.host", "localhost");
    conf.setInt("http.proxy.port", 8181);
    conf.set("http.agent.name", "test");
    conf.set("http.robots.agents", "test,*");
    if (!plugins.equals("default")) {
      conf.set("plugin.includes", plugins);
    }
    conf.setInt(Generator.GENERATOR_MAX_COUNT, maxPerHost);
    conf.set(Generator.GENERATOR_COUNT_MODE, Generator.GENERATOR_COUNT_VALUE_HOST);
    JobConf job = new NutchJob(getConf());    
    FileSystem fs = FileSystem.get(job);
    Path dir = new Path(getConf().get("hadoop.tmp.dir"),
            "bench-" + System.currentTimeMillis());
    fs.mkdirs(dir);
    Path rootUrlDir = new Path(dir, "seed");
    fs.mkdirs(rootUrlDir);
    createSeeds(fs, rootUrlDir, seeds);

    if (LOG.isInfoEnabled()) {
      LOG.info("crawl started in: " + dir);
      LOG.info("rootUrlDir = " + rootUrlDir);
      LOG.info("threads = " + threads);
      LOG.info("depth = " + depth);      
    }
    
    Path crawlDb = new Path(dir + "/crawldb");
    Path linkDb = new Path(dir + "/linkdb");
    Path segments = new Path(dir + "/segments");
    long start = System.currentTimeMillis();
    Injector injector = new Injector(getConf());
    Generator generator = new Generator(getConf());
    Fetcher fetcher = new Fetcher(getConf());
    ParseSegment parseSegment = new ParseSegment(getConf());
    CrawlDb crawlDbTool = new CrawlDb(getConf());
    LinkDb linkDbTool = new LinkDb(getConf());
      
    // initialize crawlDb
    injector.inject(crawlDb, rootUrlDir);
    int i;
    for (i = 0; i < depth; i++) {             // generate new segment
      Path[] segs = generator.generate(crawlDb, segments, -1, topN, System
          .currentTimeMillis());
      if (segs == null) {
        LOG.info("Stopping at depth=" + i + " - no more URLs to fetch.");
        break;
      }
      fetcher.fetch(segs[0], threads, org.apache.nutch.fetcher.Fetcher.isParsing(getConf()));  // fetch it
      if (!Fetcher.isParsing(job)) {
        parseSegment.parse(segs[0]);    // parse it, if needed
      }
      crawlDbTool.update(crawlDb, segs, true, true); // update crawldb
      linkDbTool.invert(linkDb, segs, true, true, false); // invert links
      // delete data
      if (delete) {
        for (Path p : segs) {
          fs.delete(p, true);
        }
      }
    }
    if (i == 0) {
      LOG.warn("No URLs to fetch - check your seed list and URL filters.");
    }
    if (LOG.isInfoEnabled()) { LOG.info("crawl finished: " + dir); }
    long end = System.currentTimeMillis();
    LOG.info("TOTAL TIME: " + (end - start)/1000 + " sec");
    CrawlDbReader dbreader = new CrawlDbReader();
    dbreader.processStatJob(crawlDb.toString(), conf, false);
    return 0;
  }

}
