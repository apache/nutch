/*
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
 */
package org.apache.nutch.indexer;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.CrawlDb;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.crawl.LinkDb;
import org.apache.nutch.crawl.NutchWritable;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseImpl;
import org.apache.nutch.parse.ParseText;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;

public class IndexerMapReduce extends Configured {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  public static final String INDEXER_PARAMS = "indexer.additional.params";
  public static final String INDEXER_DELETE = "indexer.delete";
  public static final String INDEXER_DELETE_ROBOTS_NOINDEX = "indexer.delete.robots.noindex";
  public static final String INDEXER_DELETE_SKIPPED = "indexer.delete.skipped.by.indexingfilter";
  public static final String INDEXER_SKIP_NOTMODIFIED = "indexer.skip.notmodified";
  public static final String URL_FILTERING = "indexer.url.filters";
  public static final String URL_NORMALIZING = "indexer.url.normalizers";
  public static final String INDEXER_BINARY_AS_BASE64 = "indexer.binary.base64";

  /*// using normalizers and/or filters
  private static boolean normalize = false;
  private static boolean filter = false;

  // url normalizers, filters and job configuration
  private static URLNormalizers urlNormalizers;
  private static URLFilters urlFilters;*/

  /** Predefined action to delete documents from the index */
  private static final NutchIndexAction DELETE_ACTION = new NutchIndexAction(
      null, NutchIndexAction.DELETE);

  /**
   * Normalizes and trims extra whitespace from the given url.
   * 
   * @param url
   *          The url to normalize.
   * 
   * @return The normalized url.
   */
  private static String normalizeUrl(String url, boolean normalize, 
       URLNormalizers urlNormalizers) {
    if (!normalize) {
      return url;
    }

    String normalized = null;
    if (urlNormalizers != null) {
      try {

        // normalize and trim the url
        normalized = urlNormalizers
            .normalize(url, URLNormalizers.SCOPE_INDEXER);
        normalized = normalized.trim();
      } catch (Exception e) {
        LOG.warn("Skipping " + url + ":" + e);
        normalized = null;
      }
    }

    return normalized;
  }

  /**
   * Filters the given url.
   * 
   * @param url
   *          The url to filter.
   * 
   * @return The filtered url or null.
   */
  private static String filterUrl(String url, boolean filter, 
       URLFilters urlFilters) {
    if (!filter) {
      return url;
    }

    try {
      url = urlFilters.filter(url);
    } catch (Exception e) {
      url = null;
    }

    return url;
  }

  public static class IndexerMapper extends 
     Mapper<Text, Writable, Text, NutchWritable> {

    // using normalizers and/or filters
    private boolean normalize = false;
    private boolean filter = false;

    // url normalizers, filters and job configuration
    private URLNormalizers urlNormalizers;
    private URLFilters urlFilters;

    @Override
    public void setup(Mapper<Text, Writable, Text, NutchWritable>.Context context){
      Configuration conf = context.getConfiguration();
      
      normalize = conf.getBoolean(URL_NORMALIZING, false);
      filter = conf.getBoolean(URL_FILTERING, false);
      
      if (normalize) {
        urlNormalizers = new URLNormalizers(conf,
            URLNormalizers.SCOPE_INDEXER);
      }   

      if (filter) {
        urlFilters = new URLFilters(conf);
      }    
    }

    @Override
    public void map(Text key, Writable value,
        Context context) throws IOException, InterruptedException {

      String urlString = filterUrl(normalizeUrl(key.toString(), normalize, 
                                     urlNormalizers), filter, urlFilters);
      if (urlString == null) {
        return;
      } else {
        key.set(urlString);
      }

      context.write(key, new NutchWritable(value));
    }
  }

  public static class IndexerReducer extends
     Reducer<Text, NutchWritable, Text, NutchIndexAction> {

    private boolean skip = false;
    private boolean delete = false;
    private boolean deleteRobotsNoIndex = false;
    private boolean deleteSkippedByIndexingFilter = false;
    private boolean base64 = false;
    private IndexingFilters filters;
    private ScoringFilters scfilters;

    // using normalizers and/or filters
    private boolean normalize = false;
    private boolean filter = false;

    // url normalizers, filters and job configuration
    private URLNormalizers urlNormalizers;
    private URLFilters urlFilters;

    @Override
    public void setup(Reducer<Text, NutchWritable, Text, NutchIndexAction>.Context context) {
      Configuration conf = context.getConfiguration();
      filters = new IndexingFilters(conf);
      scfilters = new ScoringFilters(conf);
      delete = conf.getBoolean(INDEXER_DELETE, false);
      deleteRobotsNoIndex = conf.getBoolean(INDEXER_DELETE_ROBOTS_NOINDEX,
          false);
      deleteSkippedByIndexingFilter = conf.getBoolean(INDEXER_DELETE_SKIPPED,
          false);
      skip = conf.getBoolean(INDEXER_SKIP_NOTMODIFIED, false);
      base64 = conf.getBoolean(INDEXER_BINARY_AS_BASE64, false);

      normalize = conf.getBoolean(URL_NORMALIZING, false);
      filter = conf.getBoolean(URL_FILTERING, false);

      if (normalize) {
        urlNormalizers = new URLNormalizers(conf,
            URLNormalizers.SCOPE_INDEXER);
      }

      if (filter) {
        urlFilters = new URLFilters(conf);
      }
    }

    public void reduce(Text key, Iterable<NutchWritable> values,
        Context context) throws IOException, InterruptedException {
      Inlinks inlinks = null;
      CrawlDatum dbDatum = null;
      CrawlDatum fetchDatum = null;
      Content content = null;
      ParseData parseData = null;
      ParseText parseText = null;

      for (NutchWritable val : values) {
        final Writable value = val.get(); // unwrap
        if (value instanceof Inlinks) {
          inlinks = (Inlinks) value;
        } else if (value instanceof CrawlDatum) {
          final CrawlDatum datum = (CrawlDatum) value;
          if (CrawlDatum.hasDbStatus(datum)) {
            dbDatum = datum;
          } else if (CrawlDatum.hasFetchStatus(datum)) {
            // don't index unmodified (empty) pages
            if (datum.getStatus() != CrawlDatum.STATUS_FETCH_NOTMODIFIED) {
              fetchDatum = datum;
            }
          } else if (CrawlDatum.STATUS_LINKED == datum.getStatus()
              || CrawlDatum.STATUS_SIGNATURE == datum.getStatus()
              || CrawlDatum.STATUS_PARSE_META == datum.getStatus()) {
            continue;
          } else {
            throw new RuntimeException("Unexpected status: " + datum.getStatus());
          }
        } else if (value instanceof ParseData) {
          parseData = (ParseData) value;

          // Handle robots meta? https://issues.apache.org/jira/browse/NUTCH-1434
          if (deleteRobotsNoIndex) {
            // Get the robots meta data
            String robotsMeta = parseData.getMeta("robots");

            // Has it a noindex for this url?
            if (robotsMeta != null
                && robotsMeta.toLowerCase().indexOf("noindex") != -1) {
              // Delete it!
              context.write(key, DELETE_ACTION);
              context.getCounter("IndexerStatus", "deleted (robots=noindex)").increment(1);
              return;
            }
          }
        } else if (value instanceof ParseText) {
          parseText = (ParseText) value;
        } else if (value instanceof Content) {
          content = (Content)value;
        } else if (LOG.isWarnEnabled()) {
          LOG.warn("Unrecognized type: " + value.getClass());
        }
      }

      // Whether to delete GONE or REDIRECTS
      if (delete && fetchDatum != null) {
        if (fetchDatum.getStatus() == CrawlDatum.STATUS_FETCH_GONE
            || dbDatum != null && dbDatum.getStatus() == CrawlDatum.STATUS_DB_GONE) {
          context.getCounter("IndexerStatus", "deleted (gone)").increment(1);
          context.write(key, DELETE_ACTION);
          return;
        }

        if (fetchDatum.getStatus() == CrawlDatum.STATUS_FETCH_REDIR_PERM
            || fetchDatum.getStatus() == CrawlDatum.STATUS_FETCH_REDIR_TEMP
            || dbDatum != null && dbDatum.getStatus() == CrawlDatum.STATUS_DB_REDIR_PERM
            || dbDatum != null && dbDatum.getStatus() == CrawlDatum.STATUS_DB_REDIR_TEMP) {
          context.getCounter("IndexerStatus", "deleted (redirects)").increment(1);
          context.write(key, DELETE_ACTION);
          return;
        }
      }

      if (fetchDatum == null || parseText == null || parseData == null) {
        return; // only have inlinks
      }

      // Whether to delete pages marked as duplicates
      if (delete && dbDatum != null && dbDatum.getStatus() == CrawlDatum.STATUS_DB_DUPLICATE) {
        context.getCounter("IndexerStatus", "deleted (duplicates)").increment(1);
        context.write(key, DELETE_ACTION);
        return;
      }

      // Whether to skip DB_NOTMODIFIED pages
      if (skip && dbDatum != null && dbDatum.getStatus() == CrawlDatum.STATUS_DB_NOTMODIFIED) {
        context.getCounter("IndexerStatus", "skipped (not modified)").increment(1);
        return;
      }

      if (!parseData.getStatus().isSuccess()
          || fetchDatum.getStatus() != CrawlDatum.STATUS_FETCH_SUCCESS) {
        return;
      }

      NutchDocument doc = new NutchDocument();
      doc.add("id", key.toString());

      final Metadata metadata = parseData.getContentMeta();

      // add segment, used to map from merged index back to segment files
      doc.add("segment", metadata.get(Nutch.SEGMENT_NAME_KEY));

      // add digest, used by dedup
      doc.add("digest", metadata.get(Nutch.SIGNATURE_KEY));
      
      final Parse parse = new ParseImpl(parseText, parseData);
      float boost = 1.0f;
      // run scoring filters
      try {
        boost = scfilters.indexerScore(key, doc, dbDatum, fetchDatum, parse,
            inlinks, boost);
      } catch (final ScoringFilterException e) {
        context.getCounter("IndexerStatus", "errors (ScoringFilter)").increment(1);
        if (LOG.isWarnEnabled()) {
          LOG.warn("Error calculating score {}: {}", key, e);
        }
        return;
      }
      // apply boost to all indexed fields.
      doc.setWeight(boost);
      // store boost for use by explain and dedup
      doc.add("boost", Float.toString(boost));

      try {
        if (dbDatum != null) {
          // Indexing filters may also be interested in the signature
          fetchDatum.setSignature(dbDatum.getSignature());
          
          // extract information from dbDatum and pass it to
          // fetchDatum so that indexing filters can use it
          final Text url = (Text) dbDatum.getMetaData().get(
              Nutch.WRITABLE_REPR_URL_KEY);
          if (url != null) {
            // Representation URL also needs normalization and filtering.
            // If repr URL is excluded by filters we still accept this document
            // but represented by its primary URL ("key") which has passed URL
            // filters.
            String urlString = filterUrl(normalizeUrl(key.toString(), normalize,
                                       urlNormalizers), filter, urlFilters);
            if (urlString != null) {
              url.set(urlString);
              fetchDatum.getMetaData().put(Nutch.WRITABLE_REPR_URL_KEY, url);
            }
          }
        }
        // run indexing filters
        doc = filters.filter(doc, parse, key, fetchDatum, inlinks);
      } catch (final IndexingException e) {
        if (LOG.isWarnEnabled()) {
          LOG.warn("Error indexing " + key + ": " + e);
        }
        context.getCounter("IndexerStatus", "errors (IndexingFilter)").increment(1);
        return;
      }

      // skip documents discarded by indexing filters
      if (doc == null) {
        // https://issues.apache.org/jira/browse/NUTCH-1449
        if (deleteSkippedByIndexingFilter) {
          NutchIndexAction action = new NutchIndexAction(null, NutchIndexAction.DELETE);
          context.write(key, action);
          context.getCounter("IndexerStatus", "deleted (IndexingFilter)").increment(1);
        } else {
          context.getCounter("IndexerStatus", "skipped (IndexingFilter)").increment(1);
        }
        return;
      }

      if (content != null) {
        // Add the original binary content
        String binary;
        if (base64) {
          // optionally encode as base64
          binary = Base64.encodeBase64String(content.getContent());
        } else {
          binary = new String(content.getContent());
        }
        doc.add("binaryContent", binary);
      }

      context.getCounter("IndexerStatus", "indexed (add/update)").increment(1);

      NutchIndexAction action = new NutchIndexAction(doc, NutchIndexAction.ADD);
      context.write(key, action);
    }
  }

  public void close() throws IOException {
  }

  public static void initMRJob(Path crawlDb, Path linkDb,
      Collection<Path> segments, Job job, boolean addBinaryContent) throws IOException{

    LOG.info("IndexerMapReduce: crawldb: {}", crawlDb);

    if (linkDb != null)
      LOG.info("IndexerMapReduce: linkdb: {}", linkDb);

    Configuration conf = job.getConfiguration();
    for (final Path segment : segments) {
      LOG.info("IndexerMapReduces: adding segment: {}", segment);
      FileInputFormat.addInputPath(job, new Path(segment,
          CrawlDatum.FETCH_DIR_NAME));
      FileInputFormat.addInputPath(job, new Path(segment,
          CrawlDatum.PARSE_DIR_NAME));
      FileInputFormat.addInputPath(job, new Path(segment, ParseData.DIR_NAME));
      FileInputFormat.addInputPath(job, new Path(segment, ParseText.DIR_NAME));

      if (addBinaryContent) {
        FileInputFormat.addInputPath(job, new Path(segment, Content.DIR_NAME));
      }
    }

    FileInputFormat.addInputPath(job, new Path(crawlDb, CrawlDb.CURRENT_NAME));

    if (linkDb != null) {
      Path currentLinkDb = new Path(linkDb, LinkDb.CURRENT_NAME);
      try {
        if (currentLinkDb.getFileSystem(conf).exists(currentLinkDb)) {
          FileInputFormat.addInputPath(job, currentLinkDb);
        } else {
          LOG.warn("Ignoring linkDb for indexing, no linkDb found in path: {}",
              linkDb);
        }
      } catch (IOException e) {
        LOG.warn("Failed to use linkDb ({}) for indexing: {}", linkDb,
            org.apache.hadoop.util.StringUtils.stringifyException(e));
      }
    }

    job.setInputFormatClass(SequenceFileInputFormat.class);

    job.setJarByClass(IndexerMapReduce.class);
    job.setMapperClass(IndexerMapReduce.IndexerMapper.class);
    job.setReducerClass(IndexerMapReduce.IndexerReducer.class);

    job.setOutputFormatClass(IndexerOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setMapOutputValueClass(NutchWritable.class);
    job.setOutputValueClass(NutchWritable.class);
  }
}
