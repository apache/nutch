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
package org.apache.nutch.parse;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;

import org.apache.avro.util.Utf8;
import org.apache.gora.filter.MapFieldValueFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.GeneratorJob;
import org.apache.nutch.crawl.SignatureFactory;
import org.apache.nutch.metadata.HttpHeaders;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.ParseStatus;
import org.apache.nutch.storage.StorageUtils;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.IdentityPageReducer;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.NutchTool;
import org.apache.nutch.util.StringUtil;
import org.apache.nutch.util.TableUtil;
import org.apache.nutch.util.TimingUtil;
import org.apache.nutch.util.ToolUtil;
import org.apache.gora.filter.FilterOp;
import org.apache.gora.mapreduce.GoraMapper;

public class ParserJob extends NutchTool implements Tool {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  private static final String RESUME_KEY = "parse.job.resume";
  private static final String FORCE_KEY = "parse.job.force";

  public static final String SKIP_TRUNCATED = "parser.skip.truncated";

  private static final Utf8 REPARSE = new Utf8("-reparse");

  private static String SITEMAP_PARSE = "parse.sitemap";

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
    FIELDS.add(WebPage.Field.HEADERS);
    FIELDS.add(WebPage.Field.SITEMAPS);
    FIELDS.add(WebPage.Field.STM_PRIORITY);
  }

  public static class ParserMapper extends GoraMapper<String, WebPage, String, WebPage> {
    private ParseUtil parseUtil;

    private boolean shouldResume;

    private boolean force;

    private boolean sitemap;

    private Utf8 batchId;

    private boolean skipTruncated;

    @Override
    public void setup(Context context) throws IOException {
      Configuration conf = context.getConfiguration();
      parseUtil = new ParseUtil(conf);
      shouldResume = conf.getBoolean(RESUME_KEY, false);
      force = conf.getBoolean(FORCE_KEY, false);
      sitemap = conf.getBoolean(SITEMAP_PARSE, false);
      batchId = new Utf8(
          conf.get(GeneratorJob.BATCH_ID, Nutch.ALL_BATCH_ID_STR));
      skipTruncated = conf.getBoolean(SKIP_TRUNCATED, true);
      if (sitemap) {
        skipTruncated = false;
      } else {
        skipTruncated = conf.getBoolean(SKIP_TRUNCATED, true);
      }
    }

    @Override
    public void map(String key, WebPage page, Context context)
        throws IOException, InterruptedException {
      String unreverseKey = TableUtil.unreverseUrl(key);
      if (batchId.equals(REPARSE)) {
        LOG.debug("Reparsing " + unreverseKey);
      } else {
        if (Mark.FETCH_MARK.checkMark(page) == null) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Skipping {}, not fetched yet", unreverseKey);
          }
          return;
        }
        if (shouldResume && Mark.PARSE_MARK.checkMark(page) != null) {
          if (force) {
            LOG.info("Forced parsing " + unreverseKey + "; already parsed");
          } else {
            LOG.info("Skipping " + unreverseKey + "; already parsed");
            return;
          }
        } else {
          LOG.info("Parsing " + unreverseKey);
        }
      }

      if (skipTruncated && isTruncated(unreverseKey, page)) {
        return;
      }

      if (sitemap && URLFilters.isSitemap(page)) {
        LOG.info("Parsing for sitemap"); //TODO this log should be top line
        parseUtil.processSitemapParse(unreverseKey, page, context);
      } else {
        parseUtil.process(unreverseKey, page);
      }
      ParseStatus pstatus = page.getParseStatus();
      if (pstatus != null) {
        context.getCounter("ParserStatus",
            ParseStatusCodes.majorCodes[pstatus.getMajorCode()]).increment(1);
      }

      context.write(key, page);
    }
  }

  public ParserJob() {

  }

  public ParserJob(Configuration conf) {
    setConf(conf);
  }

  /**
   * Checks if the page's content is truncated.
   * 
   * @param url
   * @param page
   * @return If the page is truncated <code>true</code>. When it is not, or when
   *         it could be determined, <code>false</code>.
   */
  public static boolean isTruncated(String url, WebPage page) {
    ByteBuffer content = page.getContent();
    if (content == null) {
      return false;
    }
    CharSequence lengthUtf8 = page.getHeaders().get(
        new Utf8(HttpHeaders.CONTENT_LENGTH));
    if (lengthUtf8 == null) {
      return false;
    }
    String lengthStr = lengthUtf8.toString().trim();
    if (StringUtil.isEmpty(lengthStr)) {
      return false;
    }
    int inHeaderSize;
    try {
      inHeaderSize = Integer.parseInt(lengthStr);
    } catch (NumberFormatException e) {
      LOG.warn("Wrong contentlength format for " + url, e);
      return false;
    }
    int actualSize = content.limit();
    if (inHeaderSize > actualSize) {
      LOG.warn(url + " skipped. Content of size " + inHeaderSize
          + " was truncated to " + actualSize);
      return true;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug(url + " actualSize=" + actualSize + " inHeaderSize="
          + inHeaderSize);
    }
    return false;
  }

  public Collection<WebPage.Field> getFields(Job job) {
    Configuration conf = job.getConfiguration();
    Collection<WebPage.Field> fields = new HashSet<WebPage.Field>(FIELDS);
    ParserFactory parserFactory = new ParserFactory(conf);
    ParseFilters parseFilters = new ParseFilters(conf);

    Collection<WebPage.Field> parsePluginFields = parserFactory.getFields();
    Collection<WebPage.Field> signaturePluginFields = SignatureFactory
        .getFields(conf);
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

  @Override
  public Map<String, Object> run(Map<String, Object> args) throws Exception {
    String batchId = (String) args.get(Nutch.ARG_BATCH);
    Boolean shouldResume = (Boolean) args.get(Nutch.ARG_RESUME);
    Boolean force = (Boolean) args.get(Nutch.ARG_FORCE);
    Boolean sitemap = (Boolean) args.get(Nutch.ARG_SITEMAP);

    if (batchId != null) {
      getConf().set(GeneratorJob.BATCH_ID, batchId);
    }
    if (shouldResume != null) {
      getConf().setBoolean(RESUME_KEY, shouldResume);
    }
    if (force != null) {
      getConf().setBoolean(FORCE_KEY, force);
    }
    if (sitemap != null) {
      getConf().setBoolean(SITEMAP_PARSE, sitemap);
    }
    LOG.info("ParserJob: resuming:\t{}", getConf().getBoolean(RESUME_KEY, false));
    LOG.info("ParserJob: forced reparse:\t {}", getConf().getBoolean(FORCE_KEY, false));
    if (batchId == null || batchId.equals(Nutch.ALL_BATCH_ID_STR)) {
      LOG.info("ParserJob: parsing all");
    } else {
      LOG.info("ParserJob: batchId:\t{}", batchId);
    }
    currentJob = NutchJob.getInstance(getConf(), "parse");

    Collection<WebPage.Field> fields = getFields(currentJob);
    MapFieldValueFilter<String, WebPage> batchIdFilter = getBatchIdFilter(batchId);
    StorageUtils.initMapperJob(currentJob, fields, String.class, WebPage.class,
        ParserMapper.class, batchIdFilter);
    StorageUtils.initReducerJob(currentJob, IdentityPageReducer.class);
    currentJob.setNumReduceTasks(0);

    currentJob.waitForCompletion(true);
    ToolUtil.recordJobStatus(null, currentJob, results);
    return results;
  }

  private MapFieldValueFilter<String, WebPage> getBatchIdFilter(String batchId) {
    if (batchId.equals(REPARSE.toString())
        || batchId.equals(Nutch.ALL_CRAWL_ID.toString())) {
      return null;
    }
    MapFieldValueFilter<String, WebPage> filter = new MapFieldValueFilter<String, WebPage>();
    filter.setFieldName(WebPage.Field.MARKERS.toString());
    filter.setFilterOp(FilterOp.EQUALS);
    filter.setFilterIfMissing(true);
    filter.setMapKey(Mark.FETCH_MARK.getName());
    filter.getOperands().add(new Utf8(batchId));
    return filter;
  }

  public int parse(String batchId, boolean shouldResume, boolean force)
      throws Exception {
    return parse(batchId, shouldResume, force, false);
  }

  public int parse(String batchId, boolean shouldResume, boolean force,
      boolean sitemap)
          throws Exception {

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ROOT);
    long start = System.currentTimeMillis();
    LOG.info("ParserJob: starting at {}", sdf.format(start));

    run(ToolUtil.toArgMap(Nutch.ARG_BATCH, batchId, Nutch.ARG_RESUME,
        shouldResume, Nutch.ARG_FORCE, force, Nutch.ARG_SITEMAP, sitemap));
    LOG.info("ParserJob: success");

    long finish = System.currentTimeMillis();
    LOG.info("ParserJob: finished at {}, time elapsed: {}", 
        sdf.format(finish), TimingUtil.elapsedTime(start, finish));
    return 0;
  }

  public int run(String[] args) throws Exception {
    boolean shouldResume = false;
    boolean force = false;
    boolean sitemap = false;
    String batchId = null;

    if (args.length < 1) {
      System.err
      .println("Usage: ParserJob (<batchId> | -all) [-crawlId <id>] [-resume] [-force] [-sitemap]");
      System.err
      .println("    <batchId>     - symbolic batch ID created by Generator");
      System.err
      .println("    -crawlId <id> - the id to prefix the schemas to operate on, \n \t \t    (default: storage.crawl.id)");
      System.err
      .println("    -all          - consider pages from all crawl jobs");
      System.err
      .println("    -sitemap      - parse only sitemap pages, default false");
      System.err
      .println("    -resume       - resume a previous incomplete job");
      System.err
      .println("    -force        - force re-parsing even if a page is already parsed");
      return -1;
    }
    for (int i = 0; i < args.length; i++) {
      if ("-resume".equals(args[i])) {
        shouldResume = true;
      } else if ("-force".equals(args[i])) {
        force = true;
      } else if ("-crawlId".equals(args[i])) {
        getConf().set(Nutch.CRAWL_ID_KEY, args[++i]);
      } else if ("-all".equals(args[i])) {
        batchId = args[i];
      } else if ("-sitemap".equals(args[i])) {
        sitemap = true;
      } else {
        if (batchId != null) {
          System.err.println("BatchId already set to '" + batchId + "'!");
          return -1;
        }
        batchId = args[i];
      }
    }
    if (batchId == null) {
      System.err.println("BatchId not set (or -all/-reparse not specified)!");
      return -1;
    }
    return parse(batchId, shouldResume, force, sitemap);
  }

  public static void main(String[] args) throws Exception {
    final int res = ToolRunner.run(NutchConfiguration.create(),
        new ParserJob(), args);
    System.exit(res);
  }

}
