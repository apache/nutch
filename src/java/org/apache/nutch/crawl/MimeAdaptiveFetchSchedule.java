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

package org.apache.nutch.crawl;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.*;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.metadata.HttpHeaders;
import org.apache.nutch.util.NutchConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extension of @see AdaptiveFetchSchedule that allows for more flexible
 * configuration of DEC and INC factors for various MIME-types.
 * 
 * This class can be typically used in cases where a recrawl consists of many
 * different MIME-types. It's not very common for MIME-types other than
 * text/html to change frequently. Using this class you can configure different
 * factors per MIME-type so to prefer frequently changing MIME-types over
 * others.
 * 
 * For it to work this class relies on the Content-Type MetaData key being
 * present in the CrawlDB. This can either be done when injecting new URL's or
 * by adding "Content-Type" to the db.parsemeta.to.crawldb configuration setting
 * to force MIME-types of newly discovered URL's to be added to the CrawlDB.
 * 
 * @author markus
 */
public class MimeAdaptiveFetchSchedule extends AdaptiveFetchSchedule {
  // Loggg
  public static final Logger LOG = LoggerFactory
      .getLogger(MimeAdaptiveFetchSchedule.class);

  // Conf directives
  public static final String SCHEDULE_INC_RATE = "db.fetch.schedule.adaptive.inc_rate";
  public static final String SCHEDULE_DEC_RATE = "db.fetch.schedule.adaptive.dec_rate";
  public static final String SCHEDULE_MIME_FILE = "db.fetch.schedule.mime.file";

  // Default values for DEC and INC rate
  private float defaultIncRate;
  private float defaultDecRate;

  // Structure to store inc and dec rates per MIME-type
  private class AdaptiveRate {
    public float inc;
    public float dec;

    public AdaptiveRate(Float inc, Float dec) {
      this.inc = inc;
      this.dec = dec;
    }
  }

  // Here we store the mime's and their delta's
  private HashMap<String, AdaptiveRate> mimeMap;

  public void setConf(Configuration conf) {
    super.setConf(conf);
    if (conf == null)
      return;

    // Read and set the default INC and DEC rates in case we cannot set values
    // based on MIME-type
    defaultIncRate = conf.getFloat(SCHEDULE_INC_RATE, 0.2f);
    defaultDecRate = conf.getFloat(SCHEDULE_DEC_RATE, 0.2f);

    // Where's the mime/factor file?
    Reader mimeFile = conf.getConfResourceAsReader(conf.get(SCHEDULE_MIME_FILE,
        "adaptive-mimetypes.txt"));

    try {
      readMimeFile(mimeFile);
    } catch (IOException e) {
      LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
    }
  }

  @Override
  public CrawlDatum setFetchSchedule(Text url, CrawlDatum datum,
      long prevFetchTime, long prevModifiedTime, long fetchTime,
      long modifiedTime, int state) {

    // Set defaults
    INC_RATE = defaultIncRate;
    DEC_RATE = defaultDecRate;

    // Check if the Content-Type field is available in the CrawlDatum
    if (datum.getMetaData().containsKey(HttpHeaders.WRITABLE_CONTENT_TYPE)) {
      // Get the MIME-type of the current URL
      String currentMime = datum.getMetaData()
          .get(HttpHeaders.WRITABLE_CONTENT_TYPE).toString();

      // Get rid of charset
      currentMime = currentMime.substring(0, currentMime.indexOf(';'));

      // Check if this MIME-type exists in our map
      if (mimeMap.containsKey(currentMime)) {
        // Yes, set the INC and DEC rates for this MIME-type
        INC_RATE = mimeMap.get(currentMime).inc;
        DEC_RATE = mimeMap.get(currentMime).dec;
      }
    }

    return super.setFetchSchedule(url, datum, prevFetchTime, prevModifiedTime,
        fetchTime, modifiedTime, state);
  }

  /**
   * Reads the mime types and their associated INC/DEC factors in a HashMap
   * 
   * @param mimeFile
   *          Reader
   * @return void
   */
  private void readMimeFile(Reader mimeFile) throws IOException {
    // Instance of our mime/factor map
    mimeMap = new HashMap<String, AdaptiveRate>();

    // Open a reader
    BufferedReader reader = new BufferedReader(mimeFile);

    String line = null;
    String[] splits = null;

    // Read all lines
    while ((line = reader.readLine()) != null) {
      // Skip blank lines and comments
      if (StringUtils.isNotBlank(line) && !line.startsWith("#")) {
        // Split the line by TAB
        splits = line.split("\t");

        // Sanity check, we need two or three items
        if (splits.length == 3) {
          // Add a lower cased MIME-type and the factor to the map
          mimeMap.put(StringUtils.lowerCase(splits[0]), new AdaptiveRate(
              new Float(splits[1]), new Float(splits[2])));
        } else {
          LOG.warn("Invalid configuration line in: " + line);
        }
      }
    }
  }

  public static void main(String[] args) throws Exception {
    FetchSchedule fs = new MimeAdaptiveFetchSchedule();
    fs.setConf(NutchConfiguration.create());
    // we start the time at 0, for simplicity
    long curTime = 0;
    long delta = 1000L * 3600L * 24L; // 2 hours
    // we trigger the update of the page every 30 days
    long update = 1000L * 3600L * 24L * 30L; // 30 days
    boolean changed = true;
    long lastModified = 0;
    int miss = 0;
    int totalMiss = 0;
    int maxMiss = 0;
    int fetchCnt = 0;
    int changeCnt = 0;

    // initial fetchInterval is 10 days
    CrawlDatum p = new CrawlDatum(1, 3600 * 24 * 30, 1.0f);

    // Set a default MIME-type to test with
    org.apache.hadoop.io.MapWritable x = new org.apache.hadoop.io.MapWritable();
    x.put(HttpHeaders.WRITABLE_CONTENT_TYPE, new Text(
        "text/html; charset=utf-8"));
    p.setMetaData(x);

    p.setFetchTime(0);
    LOG.info(p.toString());

    // let's move the timeline a couple of deltas
    for (int i = 0; i < 10000; i++) {
      if (lastModified + update < curTime) {
        // System.out.println("i=" + i + ", lastModified=" + lastModified +
        // ", update=" + update + ", curTime=" + curTime);
        changed = true;
        changeCnt++;
        lastModified = curTime;
      }

      LOG.info(i + ". " + changed + "\twill fetch at "
          + (p.getFetchTime() / delta) + "\tinterval "
          + (p.getFetchInterval() / SECONDS_PER_DAY) + " days" + "\t missed "
          + miss);

      if (p.getFetchTime() <= curTime) {
        fetchCnt++;
        fs.setFetchSchedule(new Text("http://www.example.com"), p, p
            .getFetchTime(), p.getModifiedTime(), curTime, lastModified,
            changed ? FetchSchedule.STATUS_MODIFIED
                : FetchSchedule.STATUS_NOTMODIFIED);

        LOG.info("\tfetched & adjusted: " + "\twill fetch at "
            + (p.getFetchTime() / delta) + "\tinterval "
            + (p.getFetchInterval() / SECONDS_PER_DAY) + " days");

        if (!changed)
          miss++;
        if (miss > maxMiss)
          maxMiss = miss;
        changed = false;
        totalMiss += miss;
        miss = 0;
      }

      if (changed)
        miss++;
      curTime += delta;
    }
    LOG.info("Total missed: " + totalMiss + ", max miss: " + maxMiss);
    LOG.info("Page changed " + changeCnt + " times, fetched " + fetchCnt
        + " times.");
  }

}