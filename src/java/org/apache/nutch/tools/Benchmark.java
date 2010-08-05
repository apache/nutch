package org.apache.nutch.tools;

import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.DbUpdaterJob;
import org.apache.nutch.crawl.GeneratorJob;
import org.apache.nutch.crawl.InjectorJob;
import org.apache.nutch.crawl.WebTableReader;
import org.apache.nutch.fetcher.FetcherJob;
import org.apache.nutch.parse.ParserJob;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;

public class Benchmark extends Configured implements Tool {
  private static final Log LOG = LogFactory.getLog(Benchmark.class);

  public static void main(String[] args) throws Exception {
    Configuration conf = NutchConfiguration.create();
    int res = ToolRunner.run(conf, new Benchmark(), args);
    System.exit(res);
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
    //boolean delete = true;
    long topN = Long.MAX_VALUE;
    
    if (args.length == 0) {
      System.err.println("Usage: Benchmark [-seeds NN] [-depth NN] [-threads NN] [-maxPerHost NN] [-plugins <regex>]");
      System.err.println("\t-seeds NN\tcreate NN unique hosts in a seed list (default: 1)");
      System.err.println("\t-depth NN\tperform NN crawl cycles (default: 10)");
      System.err.println("\t-threads NN\tuse NN threads per Fetcher task (default: 10)");
      // XXX what is the equivalent here? not an additional job...
      // System.err.println("\t-keep\tkeep segment data (default: delete after updatedb)");
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
    conf.setInt(GeneratorJob.GENERATOR_MAX_COUNT, maxPerHost);
    conf.set(GeneratorJob.GENERATOR_COUNT_MODE, GeneratorJob.GENERATOR_COUNT_VALUE_HOST);
    Job job = new NutchJob(conf);    
    FileSystem fs = FileSystem.get(job.getConfiguration());
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
    
    long start = System.currentTimeMillis();
    InjectorJob injector = new InjectorJob(conf);
    GeneratorJob generator = new GeneratorJob(conf);
    FetcherJob fetcher = new FetcherJob(conf);
    ParserJob parseSegment = new ParserJob(conf);
    DbUpdaterJob crawlDbTool = new DbUpdaterJob(conf);
    // not needed in the new API
    //LinkDb linkDbTool = new LinkDb(getConf());
      
    // initialize crawlDb
    injector.inject(rootUrlDir);
    int i;
    for (i = 0; i < depth; i++) {             // generate new segment
      String crawlId = generator.generate(topN, System.currentTimeMillis(),
              false, false);
      if (crawlId == null) {
        LOG.info("Stopping at depth=" + i + " - no more URLs to fetch.");
        break;
      }
      boolean isParsing = getConf().getBoolean("fetcher.parse", true);
      fetcher.fetch(threads, crawlId, false, isParsing);  // fetch it
      if (!isParsing) {
        parseSegment.parse(crawlId, false, false);    // parse it, if needed
      }
      crawlDbTool.run(new String[0]); // update crawldb
    }
    if (i == 0) {
      LOG.warn("No URLs to fetch - check your seed list and URL filters.");
    }
    if (LOG.isInfoEnabled()) { LOG.info("crawl finished: " + dir); }
    long end = System.currentTimeMillis();
    LOG.info("TOTAL TIME: " + (end - start)/1000 + " sec");
    WebTableReader dbreader = new WebTableReader();
    dbreader.setConf(conf);
    dbreader.processStatJob(false);
    return 0;
  }

}
