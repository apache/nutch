/**
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

package org.apache.nutch.parse;

// Commons Logging imports
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.MapFile.Writer.Option;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Metadata;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapred.*;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.fetcher.Fetcher;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.util.StringUtil;
import org.apache.nutch.util.URLUtil;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.net.*;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.util.Progressable;

/* Parse content in a segment. */
public class ParseOutputFormat implements OutputFormat<Text, Parse> {
  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());
  private URLFilters filters;
  private URLExemptionFilters exemptionFilters;
  private URLNormalizers normalizers;
  private ScoringFilters scfilters;

  private static class SimpleEntry implements Entry<Text, CrawlDatum> {
    private Text key;
    private CrawlDatum value;

    public SimpleEntry(Text key, CrawlDatum value) {
      this.key = key;
      this.value = value;
    }

    public Text getKey() {
      return key;
    }

    public CrawlDatum getValue() {
      return value;
    }

    public CrawlDatum setValue(CrawlDatum value) {
      this.value = value;
      return this.value;
    }
  }

  public void checkOutputSpecs(FileSystem fs, JobConf job) throws IOException {
    Path out = FileOutputFormat.getOutputPath(job);
    if ((out == null) && (job.getNumReduceTasks() != 0)) {
      throw new InvalidJobConfException("Output directory not set in JobConf.");
    }
    if (fs == null) {
      fs = out.getFileSystem(job);
    }
    if (fs.exists(new Path(out, CrawlDatum.PARSE_DIR_NAME)))
      throw new IOException("Segment already parsed!");
  }

  public RecordWriter<Text, Parse> getRecordWriter(FileSystem fs, JobConf job,
      String name, Progressable progress) throws IOException {

    if (job.getBoolean("parse.filter.urls", true)) {
      filters = new URLFilters(job);
      exemptionFilters = new URLExemptionFilters(job);
    }

    if (job.getBoolean("parse.normalize.urls", true)) {
      normalizers = new URLNormalizers(job, URLNormalizers.SCOPE_OUTLINK);
    }

    this.scfilters = new ScoringFilters(job);
    final int interval = job.getInt("db.fetch.interval.default", 2592000);
    final boolean ignoreInternalLinks = job.getBoolean(
        "db.ignore.internal.links", false);
    final boolean ignoreExternalLinks = job.getBoolean(
        "db.ignore.external.links", false);
    final String ignoreExternalLinksMode = job.get(
        "db.ignore.external.links.mode", "byHost");
    
    int maxOutlinksPerPage = job.getInt("db.max.outlinks.per.page", 100);
    final boolean isParsing = job.getBoolean("fetcher.parse", true);
    final int maxOutlinks = (maxOutlinksPerPage < 0) ? Integer.MAX_VALUE
        : maxOutlinksPerPage;
    final CompressionType compType = SequenceFileOutputFormat
        .getOutputCompressionType(job);
    Path out = FileOutputFormat.getOutputPath(job);

    Path text = new Path(new Path(out, ParseText.DIR_NAME), name);
    Path data = new Path(new Path(out, ParseData.DIR_NAME), name);
    Path crawl = new Path(new Path(out, CrawlDatum.PARSE_DIR_NAME), name);

    final String[] parseMDtoCrawlDB = job.get("db.parsemeta.to.crawldb", "")
        .split(" *, *");

    // textOut Options
    Option tKeyClassOpt = (Option) MapFile.Writer.keyClass(Text.class);
    org.apache.hadoop.io.SequenceFile.Writer.Option tValClassOpt = SequenceFile.Writer.valueClass(ParseText.class);
    org.apache.hadoop.io.SequenceFile.Writer.Option tProgressOpt = SequenceFile.Writer.progressable(progress);
    org.apache.hadoop.io.SequenceFile.Writer.Option tCompOpt = SequenceFile.Writer.compression(CompressionType.RECORD);
    
    final MapFile.Writer textOut = new MapFile.Writer(job, text,
        tKeyClassOpt, tValClassOpt, tCompOpt, tProgressOpt);
    
    // dataOut Options
    Option dKeyClassOpt = (Option) MapFile.Writer.keyClass(Text.class);
    org.apache.hadoop.io.SequenceFile.Writer.Option dValClassOpt = SequenceFile.Writer.valueClass(ParseData.class);
    org.apache.hadoop.io.SequenceFile.Writer.Option dProgressOpt = SequenceFile.Writer.progressable(progress);
    org.apache.hadoop.io.SequenceFile.Writer.Option dCompOpt = SequenceFile.Writer.compression(compType);

    final MapFile.Writer dataOut = new MapFile.Writer(job, data,
        dKeyClassOpt, dValClassOpt, dCompOpt, dProgressOpt);
    
    final SequenceFile.Writer crawlOut = SequenceFile.createWriter(job, SequenceFile.Writer.file(crawl),
        SequenceFile.Writer.keyClass(Text.class),
        SequenceFile.Writer.valueClass(CrawlDatum.class),
        SequenceFile.Writer.bufferSize(fs.getConf().getInt("io.file.buffer.size",4096)),
        SequenceFile.Writer.replication(fs.getDefaultReplication(crawl)),
        SequenceFile.Writer.blockSize(1073741824),
        SequenceFile.Writer.compression(compType, new DefaultCodec()),
        SequenceFile.Writer.progressable(progress),
        SequenceFile.Writer.metadata(new Metadata())); 

    return new RecordWriter<Text, Parse>() {

      public void write(Text key, Parse parse) throws IOException {

        String fromUrl = key.toString();
        // host or domain name of the source URL
        String origin = null;
        textOut.append(key, new ParseText(parse.getText()));

        ParseData parseData = parse.getData();
        // recover the signature prepared by Fetcher or ParseSegment
        String sig = parseData.getContentMeta().get(Nutch.SIGNATURE_KEY);
        if (sig != null) {
          byte[] signature = StringUtil.fromHexString(sig);
          if (signature != null) {
            // append a CrawlDatum with a signature
            CrawlDatum d = new CrawlDatum(CrawlDatum.STATUS_SIGNATURE, 0);
            d.setSignature(signature);
            crawlOut.append(key, d);
          }
        }

        // see if the parse metadata contain things that we'd like
        // to pass to the metadata of the crawlDB entry
        CrawlDatum parseMDCrawlDatum = null;
        for (String mdname : parseMDtoCrawlDB) {
          String mdvalue = parse.getData().getParseMeta().get(mdname);
          if (mdvalue != null) {
            if (parseMDCrawlDatum == null)
              parseMDCrawlDatum = new CrawlDatum(CrawlDatum.STATUS_PARSE_META,
                  0);
            parseMDCrawlDatum.getMetaData().put(new Text(mdname),
                new Text(mdvalue));
          }
        }
        if (parseMDCrawlDatum != null)
          crawlOut.append(key, parseMDCrawlDatum);

        // need to determine origin (once for all outlinks)
        if (ignoreExternalLinks || ignoreInternalLinks) {
          URL originURL = new URL(fromUrl.toString());
          // based on domain?
          if ("bydomain".equalsIgnoreCase(ignoreExternalLinksMode)) {
            origin = URLUtil.getDomainName(originURL).toLowerCase();
          } 
          // use host 
          else {
            origin = originURL.getHost().toLowerCase();
          }
        }

        ParseStatus pstatus = parseData.getStatus();
        if (pstatus != null && pstatus.isSuccess()
            && pstatus.getMinorCode() == ParseStatus.SUCCESS_REDIRECT) {
          String newUrl = pstatus.getMessage();
          int refreshTime = Integer.valueOf(pstatus.getArgs()[1]);
          newUrl = filterNormalize(fromUrl, newUrl, origin,
              ignoreInternalLinks, ignoreExternalLinks, ignoreExternalLinksMode, filters, exemptionFilters, normalizers,
              URLNormalizers.SCOPE_FETCHER);

          if (newUrl != null) {
            String reprUrl = URLUtil.chooseRepr(fromUrl, newUrl,
                refreshTime < Fetcher.PERM_REFRESH_TIME);
            CrawlDatum newDatum = new CrawlDatum();
            newDatum.setStatus(CrawlDatum.STATUS_LINKED);
            if (reprUrl != null && !reprUrl.equals(newUrl)) {
              newDatum.getMetaData().put(Nutch.WRITABLE_REPR_URL_KEY,
                  new Text(reprUrl));
            }
            crawlOut.append(new Text(newUrl), newDatum);
          }
        }

        // collect outlinks for subsequent db update
        Outlink[] links = parseData.getOutlinks();
        int outlinksToStore = Math.min(maxOutlinks, links.length);

        int validCount = 0;
        CrawlDatum adjust = null;
        List<Entry<Text, CrawlDatum>> targets = new ArrayList<>(
            outlinksToStore);
        List<Outlink> outlinkList = new ArrayList<>(outlinksToStore);
        for (int i = 0; i < links.length && validCount < outlinksToStore; i++) {
          String toUrl = links[i].getToUrl();

          // Only normalize and filter if fetcher.parse = false
          if (!isParsing) {
            toUrl = ParseOutputFormat.filterNormalize(fromUrl, toUrl, origin,
                ignoreInternalLinks, ignoreExternalLinks, ignoreExternalLinksMode, filters, exemptionFilters, normalizers);
            if (toUrl == null) {
              continue;
            }
          }

          CrawlDatum target = new CrawlDatum(CrawlDatum.STATUS_LINKED, interval);
          Text targetUrl = new Text(toUrl);

          // see if the outlink has any metadata attached
          // and if so pass that to the crawldatum so that
          // the initial score or distribution can use that
          MapWritable outlinkMD = links[i].getMetadata();
          if (outlinkMD != null) {
            target.getMetaData().putAll(outlinkMD);
          }

          try {
            scfilters.initialScore(targetUrl, target);
          } catch (ScoringFilterException e) {
            LOG.warn("Cannot filter init score for url " + key
                + ", using default: " + e.getMessage());
            target.setScore(0.0f);
          }

          targets.add(new SimpleEntry(targetUrl, target));

          // OVerwrite URL in Outlink object with normalized URL (NUTCH-1174)
          links[i].setUrl(toUrl);
          outlinkList.add(links[i]);
          validCount++;
        }

        try {
          // compute score contributions and adjustment to the original score
          adjust = scfilters.distributeScoreToOutlinks(key, parseData, targets,
              null, links.length);
        } catch (ScoringFilterException e) {
          LOG.warn("Cannot distribute score from " + key + ": "
              + e.getMessage());
        }
        for (Entry<Text, CrawlDatum> target : targets) {
          crawlOut.append(target.getKey(), target.getValue());
        }
        if (adjust != null)
          crawlOut.append(key, adjust);

        Outlink[] filteredLinks = outlinkList.toArray(new Outlink[outlinkList
            .size()]);
        parseData = new ParseData(parseData.getStatus(), parseData.getTitle(),
            filteredLinks, parseData.getContentMeta(), parseData.getParseMeta());
        dataOut.append(key, parseData);
        if (!parse.isCanonical()) {
          CrawlDatum datum = new CrawlDatum();
          datum.setStatus(CrawlDatum.STATUS_FETCH_SUCCESS);
          String timeString = parse.getData().getContentMeta()
              .get(Nutch.FETCH_TIME_KEY);
          try {
            datum.setFetchTime(Long.parseLong(timeString));
          } catch (Exception e) {
            LOG.warn("Can't read fetch time for: " + key);
            datum.setFetchTime(System.currentTimeMillis());
          }
          crawlOut.append(key, datum);
        }
      }

      public void close(Reporter reporter) throws IOException {
        textOut.close();
        dataOut.close();
        crawlOut.close();
      }

    };

  }

  public static String filterNormalize(String fromUrl, String toUrl,
      String fromHost, boolean ignoreInternalLinks, boolean ignoreExternalLinks,
      String ignoreExternalLinksMode, URLFilters filters, URLExemptionFilters exemptionFilters,
      URLNormalizers normalizers) {
    return filterNormalize(fromUrl, toUrl, fromHost, ignoreInternalLinks, ignoreExternalLinks,
        ignoreExternalLinksMode, filters, exemptionFilters, normalizers,
        URLNormalizers.SCOPE_OUTLINK);
  }

  public static String filterNormalize(String fromUrl, String toUrl,
      String origin, boolean ignoreInternalLinks, boolean ignoreExternalLinks,
       String ignoreExternalLinksMode, URLFilters filters,
       URLExemptionFilters exemptionFilters, URLNormalizers normalizers,
        String urlNormalizerScope) {
    // ignore links to self (or anchors within the page)
    if (fromUrl.equals(toUrl)) {
      return null;
    }
    if (ignoreExternalLinks || ignoreInternalLinks) {
      URL targetURL = null;
      try {
        targetURL = new URL(toUrl);
      } catch (MalformedURLException e1) {
        return null; // skip it
      }
      if (ignoreExternalLinks) {
        if ("bydomain".equalsIgnoreCase(ignoreExternalLinksMode)) {
          String toDomain = URLUtil.getDomainName(targetURL).toLowerCase();
          //FIXME: toDomain will never be null, correct?
          if (toDomain == null || !toDomain.equals(origin)) {
            return null; // skip it
          }
        } else {
          String toHost = targetURL.getHost().toLowerCase();
          if (!toHost.equals(origin)) { // external host link
            if (exemptionFilters == null // check if it is exempted?
                || !exemptionFilters.isExempted(fromUrl, toUrl)) {
              return null; ///skip it, This external url is not exempted.
            }
          }
        }
      }
      if (ignoreInternalLinks) {
        if ("bydomain".equalsIgnoreCase(ignoreExternalLinksMode)) {
          String toDomain = URLUtil.getDomainName(targetURL).toLowerCase();
          //FIXME: toDomain will never be null, correct?
          if (toDomain == null || toDomain.equals(origin)) {
            return null; // skip it
          }
        } else {
          String toHost = targetURL.getHost().toLowerCase();
          //FIXME: toDomain will never be null, correct?
          if (toHost == null || toHost.equals(origin)) {
            return null; // skip it
          }
        }
      }
    }

    try {
      if (normalizers != null) {
        toUrl = normalizers.normalize(toUrl, urlNormalizerScope); // normalize
                                                                  // the url
      }
      if (filters != null) {
        toUrl = filters.filter(toUrl); // filter the url
      }
      if (toUrl == null) {
        return null;
      }
    } catch (Exception e) {
      return null;
    }

    return toUrl;
  }

}
