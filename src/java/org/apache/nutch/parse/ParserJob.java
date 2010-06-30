package org.apache.nutch.parse;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;

import org.apache.avro.util.Utf8;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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

  public static final Log LOG = LogFactory.getLog(ParserJob.class);

  private static final Collection<WebPage.Field> FIELDS = new HashSet<WebPage.Field>();

  private Configuration conf;

  static {
    FIELDS.add(WebPage.Field.STATUS);
    FIELDS.add(WebPage.Field.CONTENT);
    FIELDS.add(WebPage.Field.CONTENT_TYPE);
    FIELDS.add(WebPage.Field.SIGNATURE);
    FIELDS.add(WebPage.Field.MARKERS);
    FIELDS.add(WebPage.Field.OUTLINKS);
    FIELDS.add(WebPage.Field.METADATA);
  }

  private ParseUtil parseUtil;

  private boolean shouldContinue;

  private Utf8 crawlId;

  @Override
  public void setup(Context context) throws IOException {
    Configuration conf = context.getConfiguration();
    parseUtil = new ParseUtil(conf);
    shouldContinue = conf.getBoolean("job.continue", false);
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
    if (shouldContinue && Mark.PARSE_MARK.checkMark(page) != null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Skipping " + TableUtil.unreverseUrl(key) + "; already parsed");
      }
      return;
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
    HtmlParseFilters parseFilters = new HtmlParseFilters(conf);

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

  public int parse(String crawlId, boolean shouldContinue) throws Exception {
    LOG.info("ParserJob: starting");

    getConf().set(GeneratorJob.CRAWL_ID, crawlId);
    getConf().setBoolean("job.continue", shouldContinue);

    LOG.info("ParserJob: continuing: " + getConf().getBoolean("job.continue", false));
    if (crawlId.equals(Nutch.ALL_CRAWL_ID_STR)) {
      LOG.info("ParserJob: parsing all");
    } else {
      LOG.info("ParserJob: crawlId: " + crawlId);
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
    boolean shouldContinue = false;
    String crawlId;

    String usage = "Usage: ParserJob (<crawl id> | -all) [-continue]";

    if (args.length < 1) {
      System.err.println(usage);
      return 1;
    }

    crawlId = args[0];
    if (crawlId.equals("-continue")) {
      System.err.println(usage);
      return 1;
    }

    if (args.length >= 1 && "-continue".equals(args[0])) {
      shouldContinue = true;
    }

    return parse(crawlId, shouldContinue);
  }

  public static void main(String[] args) throws Exception {
    final int res = ToolRunner.run(NutchConfiguration.create(),
        new ParserJob(), args);
    System.exit(res);
  }

}
