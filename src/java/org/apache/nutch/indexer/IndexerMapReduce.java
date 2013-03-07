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
import java.util.Collection;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
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
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;

public class IndexerMapReduce extends Configured
implements Mapper<Text, Writable, Text, NutchWritable>,
          Reducer<Text, NutchWritable, Text, NutchIndexAction> {

  public static final Logger LOG = LoggerFactory.getLogger(IndexerMapReduce.class);

  public static final String INDEXER_PARAMS = "indexer.additional.params";
  public static final String INDEXER_DELETE = "indexer.delete";
  public static final String INDEXER_DELETE_ROBOTS_NOINDEX = "indexer.delete.robots.noindex";
  public static final String INDEXER_SKIP_NOTMODIFIED = "indexer.skip.notmodified";
  public static final String URL_FILTERING = "indexer.url.filters";
  public static final String URL_NORMALIZING = "indexer.url.normalizers";

  private boolean skip = false;
  private boolean delete = false;
  private boolean deleteRobotsNoIndex = false;
  private IndexingFilters filters;
  private ScoringFilters scfilters;

  // using normalizers and/or filters
  private boolean normalize = false;
  private boolean filter = false;

  // url normalizers, filters and job configuration
  private URLNormalizers urlNormalizers;
  private URLFilters urlFilters;

  public void configure(JobConf job) {
    setConf(job);
    this.filters = new IndexingFilters(getConf());
    this.scfilters = new ScoringFilters(getConf());
    this.delete = job.getBoolean(INDEXER_DELETE, false);
    this.deleteRobotsNoIndex = job.getBoolean(INDEXER_DELETE_ROBOTS_NOINDEX, false);
    this.skip = job.getBoolean(INDEXER_SKIP_NOTMODIFIED, false);

    normalize = job.getBoolean(URL_NORMALIZING, false);
    filter = job.getBoolean(URL_FILTERING, false);

    if (normalize) {
      urlNormalizers = new URLNormalizers(getConf(), URLNormalizers.SCOPE_DEFAULT);
    }

    if (filter) {
      urlFilters = new URLFilters(getConf());
    }
  }

  /**
   * Normalizes and trims extra whitespace from the given url.
   *
   * @param url The url to normalize.
   *
   * @return The normalized url.
   */
  private String normalizeUrl(String url) {
    if (!normalize) {
      return url;
    }

    String normalized = null;
    if (urlNormalizers != null) {
      try {

        // normalize and trim the url
        normalized = urlNormalizers.normalize(url,
          URLNormalizers.SCOPE_INDEXER);
        normalized = normalized.trim();
      }
      catch (Exception e) {
        LOG.warn("Skipping " + url + ":" + e);
        normalized = null;
      }
    }

    return normalized;
  }

  /**
   * Filters the given url.
   *
   * @param url The url to filter.
   *
   * @return The filtered url or null.
   */
  private String filterUrl(String url) {
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

  public void map(Text key, Writable value,
      OutputCollector<Text, NutchWritable> output, Reporter reporter) throws IOException {

    String urlString = filterUrl(normalizeUrl(key.toString()));
    if (urlString == null) {
      return;
    } else {
      key.set(urlString);
    }

    output.collect(key, new NutchWritable(value));
  }

  public void reduce(Text key, Iterator<NutchWritable> values,
                     OutputCollector<Text, NutchIndexAction> output, Reporter reporter)
    throws IOException {
    Inlinks inlinks = null;
    CrawlDatum dbDatum = null;
    CrawlDatum fetchDatum = null;
    ParseData parseData = null;
    ParseText parseText = null;

    while (values.hasNext()) {
      final Writable value = values.next().get(); // unwrap
      if (value instanceof Inlinks) {
        inlinks = (Inlinks)value;
      } else if (value instanceof CrawlDatum) {
        final CrawlDatum datum = (CrawlDatum)value;
        if (CrawlDatum.hasDbStatus(datum)) {
          dbDatum = datum;
        }
        else if (CrawlDatum.hasFetchStatus(datum)) {

          // don't index unmodified (empty) pages
          if (datum.getStatus() != CrawlDatum.STATUS_FETCH_NOTMODIFIED) {
            fetchDatum = datum;

            /**
             * Check if we need to delete 404 NOT FOUND and 301 PERMANENT REDIRECT.
             */
            if (delete) {
              if (fetchDatum.getStatus() == CrawlDatum.STATUS_FETCH_GONE) {
                reporter.incrCounter("IndexerStatus", "Documents deleted", 1);

                NutchIndexAction action = new NutchIndexAction(null, NutchIndexAction.DELETE);
                output.collect(key, action);
                return;
              }
              if (fetchDatum.getStatus() == CrawlDatum.STATUS_FETCH_REDIR_PERM) {
                reporter.incrCounter("IndexerStatus", "Perm redirects deleted", 1);

                NutchIndexAction action = new NutchIndexAction(null, NutchIndexAction.DELETE);
                output.collect(key, action);
                return;
              }
            }
          }

        } else if (CrawlDatum.STATUS_LINKED == datum.getStatus() ||
                   CrawlDatum.STATUS_SIGNATURE == datum.getStatus() ||
                   CrawlDatum.STATUS_PARSE_META == datum.getStatus()) {
          continue;
        } else {
          throw new RuntimeException("Unexpected status: "+datum.getStatus());
        }
      } else if (value instanceof ParseData) {
        parseData = (ParseData)value;

        // Handle robots meta? https://issues.apache.org/jira/browse/NUTCH-1434
        if (deleteRobotsNoIndex) {
          // Get the robots meta data
          String robotsMeta = parseData.getMeta("robots");

          // Has it a noindex for this url?
          if (robotsMeta != null && robotsMeta.toLowerCase().indexOf("noindex") != -1) {
            // Delete it!
            NutchIndexAction action = new NutchIndexAction(null, NutchIndexAction.DELETE);
            output.collect(key, action);
            return;
          }
        }
      } else if (value instanceof ParseText) {
        parseText = (ParseText)value;
      } else if (LOG.isWarnEnabled()) {
        LOG.warn("Unrecognized type: "+value.getClass());
      }
    }

    if (fetchDatum == null || dbDatum == null
        || parseText == null || parseData == null) {
      return;                                     // only have inlinks
    }

    // Whether to skip DB_NOTMODIFIED pages
    if (skip && dbDatum.getStatus() == CrawlDatum.STATUS_DB_NOTMODIFIED) {
      reporter.incrCounter("IndexerStatus", "Skipped", 1);
      return;
    }

    if (!parseData.getStatus().isSuccess() ||
        fetchDatum.getStatus() != CrawlDatum.STATUS_FETCH_SUCCESS) {
      return;
    }

    NutchDocument doc = new NutchDocument();
    final Metadata metadata = parseData.getContentMeta();

    // add segment, used to map from merged index back to segment files
    doc.add("segment", metadata.get(Nutch.SEGMENT_NAME_KEY));

    // add digest, used by dedup
    doc.add("digest", metadata.get(Nutch.SIGNATURE_KEY));

    final Parse parse = new ParseImpl(parseText, parseData);
    try {
      // extract information from dbDatum and pass it to
      // fetchDatum so that indexing filters can use it
      final Text url = (Text) dbDatum.getMetaData().get(Nutch.WRITABLE_REPR_URL_KEY);
      if (url != null) {
        fetchDatum.getMetaData().put(Nutch.WRITABLE_REPR_URL_KEY, url);
      }
      // run indexing filters
      doc = this.filters.filter(doc, parse, key, fetchDatum, inlinks);
    } catch (final IndexingException e) {
      if (LOG.isWarnEnabled()) { LOG.warn("Error indexing "+key+": "+e); }
      reporter.incrCounter("IndexerStatus", "Errors", 1);
      return;
    }

    // skip documents discarded by indexing filters
    if (doc == null) {
      reporter.incrCounter("IndexerStatus", "Skipped by filters", 1);
      return;
    }

    float boost = 1.0f;
    // run scoring filters
    try {
      boost = this.scfilters.indexerScore(key, doc, dbDatum,
              fetchDatum, parse, inlinks, boost);
    } catch (final ScoringFilterException e) {
      if (LOG.isWarnEnabled()) {
        LOG.warn("Error calculating score " + key + ": " + e);
      }
      return;
    }
    // apply boost to all indexed fields.
    doc.setWeight(boost);
    // store boost for use by explain and dedup
    doc.add("boost", Float.toString(boost));

    reporter.incrCounter("IndexerStatus", "Documents added", 1);

    NutchIndexAction action = new NutchIndexAction(doc, NutchIndexAction.ADD);
    output.collect(key, action);
  }

  public void close() throws IOException { }

  public static void initMRJob(Path crawlDb, Path linkDb,
                           Collection<Path> segments,
                           JobConf job) {

    LOG.info("IndexerMapReduce: crawldb: " + crawlDb);
    
    if (linkDb!=null)
      LOG.info("IndexerMapReduce: linkdb: " + linkDb);

    for (final Path segment : segments) {
      LOG.info("IndexerMapReduces: adding segment: " + segment);
      FileInputFormat.addInputPath(job, new Path(segment, CrawlDatum.FETCH_DIR_NAME));
      FileInputFormat.addInputPath(job, new Path(segment, CrawlDatum.PARSE_DIR_NAME));
      FileInputFormat.addInputPath(job, new Path(segment, ParseData.DIR_NAME));
      FileInputFormat.addInputPath(job, new Path(segment, ParseText.DIR_NAME));
    }

    FileInputFormat.addInputPath(job, new Path(crawlDb, CrawlDb.CURRENT_NAME));
    
    if (linkDb!=null)
	  FileInputFormat.addInputPath(job, new Path(linkDb, LinkDb.CURRENT_NAME));
    
    job.setInputFormat(SequenceFileInputFormat.class);

    job.setMapperClass(IndexerMapReduce.class);
    job.setReducerClass(IndexerMapReduce.class);

    job.setOutputFormat(IndexerOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setMapOutputValueClass(NutchWritable.class);
    job.setOutputValueClass(NutchWritable.class);
  }
}
