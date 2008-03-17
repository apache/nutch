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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.io.*;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.fetcher.Fetcher;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapred.*;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.util.StringUtil;
import org.apache.nutch.util.URLUtil;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.net.*;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.util.Progressable;

/* Parse content in a segment. */
public class ParseOutputFormat implements OutputFormat {
  private static final Log LOG = LogFactory.getLog(ParseOutputFormat.class);

  private URLFilters filters;
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
    if (fs.exists(new Path(job.getOutputPath(), CrawlDatum.PARSE_DIR_NAME)))
      throw new IOException("Segment already parsed!");
  }

  public RecordWriter getRecordWriter(FileSystem fs, JobConf job,
                                      String name, Progressable progress) throws IOException {

    this.filters = new URLFilters(job);
    this.normalizers = new URLNormalizers(job, URLNormalizers.SCOPE_OUTLINK);
    this.scfilters = new ScoringFilters(job);
    final int interval = job.getInt("db.fetch.interval.default", 2592000);
    final boolean ignoreExternalLinks = job.getBoolean("db.ignore.external.links", false);
    int maxOutlinksPerPage = job.getInt("db.max.outlinks.per.page", 100);
    final int maxOutlinks = (maxOutlinksPerPage < 0) ? Integer.MAX_VALUE
                                                     : maxOutlinksPerPage;
    final CompressionType compType = SequenceFile.getCompressionType(job);
    
    Path text =
      new Path(new Path(job.getOutputPath(), ParseText.DIR_NAME), name);
    Path data =
      new Path(new Path(job.getOutputPath(), ParseData.DIR_NAME), name);
    Path crawl =
      new Path(new Path(job.getOutputPath(), CrawlDatum.PARSE_DIR_NAME), name);
    
    final MapFile.Writer textOut =
      new MapFile.Writer(job, fs, text.toString(), Text.class, ParseText.class,
          CompressionType.RECORD, progress);
    
    final MapFile.Writer dataOut =
      new MapFile.Writer(job, fs, data.toString(), Text.class, ParseData.class,
          compType, progress);
    
    final SequenceFile.Writer crawlOut =
      SequenceFile.createWriter(fs, job, crawl, Text.class, CrawlDatum.class,
          compType, progress);
    
    return new RecordWriter() {


        public void write(WritableComparable key, Writable value)
          throws IOException {
          
          Parse parse = (Parse)value;
          String fromUrl = key.toString();
          String fromHost = null; 
          String toHost = null;          
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

          try {
            ParseStatus pstatus = parseData.getStatus();
            if (pstatus != null && pstatus.isSuccess() &&
                pstatus.getMinorCode() == ParseStatus.SUCCESS_REDIRECT) {
              String newUrl = pstatus.getMessage();
              int refreshTime = Integer.valueOf(pstatus.getArgs()[1]);
              newUrl = normalizers.normalize(newUrl,
                                             URLNormalizers.SCOPE_FETCHER);
              newUrl = filters.filter(newUrl);
              String url = key.toString();
              if (newUrl != null && !newUrl.equals(url)) {
                String reprUrl =
                  URLUtil.chooseRepr(url, newUrl,
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
          } catch (URLFilterException e) {
            // ignore
          }

          // collect outlinks for subsequent db update
          Outlink[] links = parseData.getOutlinks();
          int outlinksToStore = Math.min(maxOutlinks, links.length);
          if (ignoreExternalLinks) {
            try {
              fromHost = new URL(fromUrl).getHost().toLowerCase();
            } catch (MalformedURLException e) {
              fromHost = null;
            }
          } else {
            fromHost = null;
          }

          int validCount = 0;
          CrawlDatum adjust = null;
          List<Entry<Text, CrawlDatum>> targets = new ArrayList<Entry<Text, CrawlDatum>>(outlinksToStore);
          List<Outlink> outlinkList = new ArrayList<Outlink>(outlinksToStore);
          for (int i = 0; i < links.length && validCount < outlinksToStore; i++) {
            String toUrl = links[i].getToUrl();
            // ignore links to self (or anchors within the page)
            if (fromUrl.equals(toUrl)) {
              continue;
            }
            if (ignoreExternalLinks) {
              try {
                toHost = new URL(toUrl).getHost().toLowerCase();
              } catch (MalformedURLException e) {
                toHost = null;
              }
              if (toHost == null || !toHost.equals(fromHost)) { // external links
                continue; // skip it
              }
            }
            try {
              toUrl = normalizers.normalize(toUrl,
                          URLNormalizers.SCOPE_OUTLINK); // normalize the url
              toUrl = filters.filter(toUrl);   // filter the url
              if (toUrl == null) {
                continue;
              }
            } catch (Exception e) {
              continue;
            }
            CrawlDatum target = new CrawlDatum(CrawlDatum.STATUS_LINKED, interval);
            Text targetUrl = new Text(toUrl);
            try {
              scfilters.initialScore(targetUrl, target);
            } catch (ScoringFilterException e) {
              LOG.warn("Cannot filter init score for url " + key +
                       ", using default: " + e.getMessage());
              target.setScore(0.0f);
            }
            
            targets.add(new SimpleEntry(targetUrl, target));
            outlinkList.add(links[i]);
            validCount++;
          }
          try {
            // compute score contributions and adjustment to the original score
            adjust = scfilters.distributeScoreToOutlinks((Text)key, parseData, 
                      targets, null, links.length);
          } catch (ScoringFilterException e) {
            LOG.warn("Cannot distribute score from " + key + ": " + e.getMessage());
          }
          for (Entry<Text, CrawlDatum> target : targets) {
            crawlOut.append(target.getKey(), target.getValue());
          }
          if (adjust != null) crawlOut.append(key, adjust);

          Outlink[] filteredLinks = outlinkList.toArray(new Outlink[outlinkList.size()]);
          parseData = new ParseData(parseData.getStatus(), parseData.getTitle(), 
                                    filteredLinks, parseData.getContentMeta(), 
                                    parseData.getParseMeta());
          dataOut.append(key, parseData);
          if (!parse.isCanonical()) {
            CrawlDatum datum = new CrawlDatum();
            datum.setStatus(CrawlDatum.STATUS_FETCH_SUCCESS);
            String timeString = parse.getData().getContentMeta().get(Nutch.FETCH_TIME_KEY);
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

}
