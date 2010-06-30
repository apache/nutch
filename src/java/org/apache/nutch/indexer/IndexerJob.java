package org.apache.nutch.indexer;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;

import org.apache.avro.util.Utf8;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.nutch.crawl.GeneratorJob;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.parse.ParseStatusCodes;
import org.apache.nutch.parse.ParseStatusUtils;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.ParseStatus;
import org.apache.nutch.storage.StorageUtils;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.TableUtil;
import org.gora.mapreduce.GoraMapper;
import org.gora.mapreduce.StringComparator;

public abstract class IndexerJob
extends GoraMapper<String, WebPage, String, WebPage>
implements Tool {

  public static final Log LOG = LogFactory.getLog(IndexerJob.class);

  private static final Collection<WebPage.Field> FIELDS = new HashSet<WebPage.Field>();

  private static final Utf8 REINDEX = new Utf8("-reindex");

  private Configuration conf;

  protected Utf8 crawlId;

  static {
    FIELDS.add(WebPage.Field.SIGNATURE);
    FIELDS.add(WebPage.Field.PARSE_STATUS);
    FIELDS.add(WebPage.Field.SCORE);
    FIELDS.add(WebPage.Field.MARKERS);
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
    crawlId = new Utf8(conf.get(GeneratorJob.CRAWL_ID, Nutch.ALL_CRAWL_ID_STR));
  }

  @Override
  public void map(String key, WebPage page, Context context)
  throws IOException, InterruptedException {
    ParseStatus pstatus = page.getParseStatus();
    if (pstatus == null || !ParseStatusUtils.isSuccess(pstatus)
        || pstatus.getMinorCode() == ParseStatusCodes.SUCCESS_REDIRECT) {
      return; // filter urls not parsed
    }

    Utf8 mark = Mark.UPDATEDB_MARK.checkMark(page);
    if (!crawlId.equals(REINDEX)) {
      if (!NutchJob.shouldProcess(mark, crawlId)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Skipping " + TableUtil.unreverseUrl(key) + "; different crawl id");
        }
        return;
      }
    }

    context.write(key, page);
  }

  private static Collection<WebPage.Field> getFields(Job job) {
    Configuration conf = job.getConfiguration();
    Collection<WebPage.Field> columns = new HashSet<WebPage.Field>(FIELDS);
    IndexingFilters filters = new IndexingFilters(conf);
    columns.addAll(filters.getFields());
    ScoringFilters scoringFilters = new ScoringFilters(conf);
    columns.addAll(scoringFilters.getFields());
    return columns;
  }

  protected Job createIndexJob(Configuration conf, String jobName, String crawlId)
  throws IOException, ClassNotFoundException {
    conf.set(GeneratorJob.CRAWL_ID, crawlId);
    Job job = new NutchJob(conf, jobName);
    // TODO: Figure out why this needs to be here
    job.getConfiguration().setClass("mapred.output.key.comparator.class",
        StringComparator.class, RawComparator.class);

    Collection<WebPage.Field> fields = getFields(job);
    StorageUtils.initMapperJob(job, fields, String.class, WebPage.class, this.getClass());
    job.setReducerClass(IndexerReducer.class);
    job.setOutputFormatClass(IndexerOutputFormat.class);
    return job;
  }
}
