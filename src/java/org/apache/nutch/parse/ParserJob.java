package org.apache.nutch.parse;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;

import org.apache.avro.util.Utf8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.GeneratorJob;
import org.apache.nutch.crawl.SignatureFactory;
import org.apache.nutch.crawl.URLWebPage;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.ParseStatus;
import org.apache.nutch.storage.StorageUtils;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.IdentityPageReducer;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.TableUtil;
import org.gora.mapreduce.GoraMapper;

public class ParserJob extends GoraMapper<String, WebPage, String, WebPage>
    implements Tool {

  public static final Logger LOG = LoggerFactory.getLogger(ParserJob.class);
  
  private static final String RESUME_KEY = "parse.job.resume";
  private static final String FORCE_KEY = "parse.job.force";

  private static final Collection<WebPage.Field> FIELDS = new HashSet<WebPage.Field>();

  private Configuration conf;

  static {
    FIELDS.add(WebPage.Field.STATUS);
    FIELDS.add(WebPage.Field.CONTENT);
    FIELDS.add(WebPage.Field.CONTENT_TYPE);
    FIELDS.add(WebPage.Field.SIGNATURE);
    FIELDS.add(WebPage.Field.MARKERS);
    FIELDS.add(WebPage.Field.PARSE_STATUS);
    FIELDS.add(WebPage.Field.OUTLINKS);
    FIELDS.add(WebPage.Field.METADATA);
  }

  private ParseUtil parseUtil;

  private boolean shouldResume;
  
  private boolean force;

  private Utf8 crawlId;
  
  public ParserJob() {
    
  }
  
  public ParserJob(Configuration conf) {
    setConf(conf);
  }

  @Override
  public void setup(Context context) throws IOException {
    Configuration conf = context.getConfiguration();
    parseUtil = new ParseUtil(conf);
    shouldResume = conf.getBoolean(RESUME_KEY, false);
    force = conf.getBoolean(FORCE_KEY, false);    
    crawlId = new Utf8(conf.get(GeneratorJob.CRAWL_ID, Nutch.ALL_CRAWL_ID_STR));
  }

  @Override
  public void map(String key, WebPage page, Context context)
      throws IOException, InterruptedException {
    Utf8 mark = Mark.FETCH_MARK.checkMark(page);
    if (!NutchJob.shouldProcess(mark, crawlId)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Skipping " + TableUtil.unreverseUrl(key) + "; different crawl id");
      }
      return;
    }
    if (shouldResume && Mark.PARSE_MARK.checkMark(page) != null) {
      if (force) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Forced parsing " + TableUtil.unreverseUrl(key) + "; already parsed");
        }        
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Skipping " + TableUtil.unreverseUrl(key) + "; already parsed");
        }
        return;
      }
    }

    URLWebPage redirectedPage = parseUtil.process(key, page);
    ParseStatus pstatus = page.getParseStatus();
    if (pstatus != null) {
      context.getCounter("ParserStatus",
          ParseStatusCodes.majorCodes[pstatus.getMajorCode()]).increment(1);
    }

    if (redirectedPage != null) {
      context.write(TableUtil.reverseUrl(redirectedPage.getUrl()),
                    redirectedPage.getDatum());
    }
    context.write(key, page);
  }

  public Collection<WebPage.Field> getFields(Job job) {
    Configuration conf = job.getConfiguration();
    Collection<WebPage.Field> fields = new HashSet<WebPage.Field>(FIELDS);
    ParserFactory parserFactory = new ParserFactory(conf);
    ParseFilters parseFilters = new ParseFilters(conf);

    Collection<WebPage.Field> parsePluginFields = parserFactory.getFields();
    Collection<WebPage.Field> signaturePluginFields =
      SignatureFactory.getFields(conf);
    Collection<WebPage.Field> htmlParsePluginFields = parseFilters.getFields();

    if (parsePluginFields != null) {
      fields.addAll(parsePluginFields);
    }
    if (signaturePluginFields != null) {
      fields.addAll(signaturePluginFields);
    }
    if (htmlParsePluginFields != null) {
      fields.addAll(htmlParsePluginFields);
    }

    return fields;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  public int parse(String crawlId, boolean shouldResume, boolean force) throws Exception {
    LOG.info("ParserJob: starting");

    if (crawlId != null) {
      getConf().set(GeneratorJob.CRAWL_ID, crawlId);
    }
    getConf().setBoolean(RESUME_KEY, shouldResume);
    getConf().setBoolean(FORCE_KEY, force);

    LOG.info("ParserJob: resuming:\t" + getConf().getBoolean(RESUME_KEY, false));
    LOG.info("ParserJob: forced reparse:\t" + getConf().getBoolean(FORCE_KEY, false));
    if (crawlId == null || crawlId.equals(Nutch.ALL_CRAWL_ID_STR)) {
      LOG.info("ParserJob: parsing all");
    } else {
      LOG.info("ParserJob: crawlId:\t" + crawlId);
    }

    final Job job = new NutchJob(getConf(), "parse");

    Collection<WebPage.Field> fields = getFields(job);
    StorageUtils.initMapperJob(job, fields, String.class, WebPage.class,
        ParserJob.class);
    StorageUtils.initReducerJob(job, IdentityPageReducer.class);
    job.setNumReduceTasks(0);
    boolean success = job.waitForCompletion(true);
    if (!success){
      LOG.info("ParserJob: failed");
      return -1;
    }
    LOG.info("ParserJob: success");
    return 0;
  }

  public int run(String[] args) throws Exception {
    boolean shouldResume = false;
    boolean force = false;
    String crawlId = null;

    if (args.length < 1) {
      System.err.println("Usage: ParserJob (<crawlId> | -all) [-resume] [-force]");
      System.err.println("\tcrawlId\tsymbolic crawl ID created by Generator");
      System.err.println("\t-all\tconsider pages from all crawl jobs");
      System.err.println("-resume\tresume a previous incomplete job");
      System.err.println("-force\tforce re-parsing even if a page is already parsed");
      return -1;
    }
    for (String s : args) {
      if ("-resume".equals(s)) {
        shouldResume = true;
      } else if ("-force".equals(s)) {
        force = true;
      } else if ("-all".equals(s)) {
        crawlId = s;
      } else {
        if (crawlId != null) {
          System.err.println("CrawlId already set to '" + crawlId + "'!");
          return -1;
        }
        crawlId = s;
      }
    }
    if (crawlId == null) {
      System.err.println("CrawlId not set (or -all not specified)!");
      return -1;
    }
    return parse(crawlId, shouldResume, force);
  }

  public static void main(String[] args) throws Exception {
    final int res = ToolRunner.run(NutchConfiguration.create(),
        new ParserJob(), args);
    System.exit(res);
  }

}
