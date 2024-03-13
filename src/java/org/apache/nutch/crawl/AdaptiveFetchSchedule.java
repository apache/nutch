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
package org.apache.nutch.crawl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.commons.lang.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Reader;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * This class implements an adaptive re-fetch algorithm. This works as follows:
 * <ul>
 * <li>for pages that has changed since the last fetchTime, decrease their
 * fetchInterval by a factor of DEC_FACTOR (default value is 0.2f).</li>
 * <li>for pages that haven't changed since the last fetchTime, increase their
 * fetchInterval by a factor of INC_FACTOR (default value is 0.2f).<br>
 * If SYNC_DELTA property is true, then:
 * <ul>
 * <li>calculate a <code>delta = fetchTime - modifiedTime</code></li>
 * <li>try to synchronize with the time of change, by shifting the next
 * fetchTime by a fraction of the difference between the last modification time
 * and the last fetch time. I.e. the next fetch time will be set to
 * <code>fetchTime + fetchInterval - delta * SYNC_DELTA_RATE</code></li>
 * <li>if the adjusted fetch interval is bigger than the delta, then
 * <code>fetchInterval = delta</code>.</li>
 * </ul>
 * </li>
 * <li>the minimum value of fetchInterval may not be smaller than MIN_INTERVAL
 * (default is 1 minute).</li>
 * <li>the maximum value of fetchInterval may not be bigger than MAX_INTERVAL
 * (default is 365 days).</li>
 * </ul>
 * <p>
 * NOTE: values of DEC_FACTOR and INC_FACTOR higher than 0.4f may destabilize
 * the algorithm, so that the fetch interval either increases or decreases
 * infinitely, with little relevance to the page changes. Please use
 * {@link #main(String[])} method to test the values before applying them in a
 * production system.
 * </p>
 * 
 * @author Andrzej Bialecki
 */
public class AdaptiveFetchSchedule extends AbstractFetchSchedule {

  // Loggg
  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  protected float INC_RATE;

  protected float DEC_RATE;

  private float MAX_INTERVAL;

  private float MIN_INTERVAL;

  private boolean SYNC_DELTA;

  private double SYNC_DELTA_RATE;

  private Configuration conf;

  private Map<String,Float> hostSpecificMaxInterval = new HashMap<>();
  
  private Map<String,Float> hostSpecificMinInterval = new HashMap<>();

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    this.conf = conf;
    if (conf == null)
      return;
    INC_RATE = conf.getFloat("db.fetch.schedule.adaptive.inc_rate", 0.2f);
    DEC_RATE = conf.getFloat("db.fetch.schedule.adaptive.dec_rate", 0.2f);
    MIN_INTERVAL = conf.getFloat("db.fetch.schedule.adaptive.min_interval", (float) 60.0);
    MAX_INTERVAL = conf.getFloat("db.fetch.schedule.adaptive.max_interval",
        (float) SECONDS_PER_DAY * 365); // 1 year
    SYNC_DELTA = conf.getBoolean("db.fetch.schedule.adaptive.sync_delta", true);
    SYNC_DELTA_RATE = conf.getFloat(
        "db.fetch.schedule.adaptive.sync_delta_rate", 0.2f);
    try {
      setHostSpecificIntervals("adaptive-host-specific-intervals.txt", 
        MIN_INTERVAL, MAX_INTERVAL);
    } catch (IOException e){
      LOG.error("Failed reading the configuration file. ", e);
    }
  }

  /**
   * Load host-specific min_intervals and max_intervals
   * from the configuration file into the HashMaps.
   */
  private void setHostSpecificIntervals(String fileName,
    float defaultMin, float defaultMax) throws IOException {
    Reader configReader = null;
    configReader = conf.getConfResourceAsReader(fileName);
    if (configReader == null) {
      configReader = new FileReader(fileName);
    }
    BufferedReader reader = new BufferedReader(configReader);
    String line;
    int lineNo = 0;
    while ((line = reader.readLine()) != null) {
      lineNo++;
      if (StringUtils.isNotBlank(line) && !line.startsWith("#")) {
        line = line.trim();
        String[] parts = line.split("\\s+");
        if (parts.length == 3) {
          // TODO: Maybe add host validatio here?
          // It might get computationally expensive for large files, though.
          String host = parts[0].trim().toLowerCase();
          String minInt = parts[1].trim();
          String maxInt = parts[2].trim();
          if (minInt.equalsIgnoreCase("default")){ minInt = "0"; }
          if (maxInt.equalsIgnoreCase("default")){ maxInt = "0"; }
          float m,M;
          try {
            m = Float.parseFloat(minInt);
            M = Float.parseFloat(maxInt);

            //negative values and mismatched boundaries are ignored
            //(default to global settings)
            if (m < 0 || M < 0 || m > M){
              LOG.error("Improper fetch intervals given on line " + String.valueOf(lineNo)
                + " in the config. file: " + line);
            } else {

              // min. interval should be positive and above the global minimum
              if (m > 0 && m > defaultMin){
                  hostSpecificMinInterval.put(host,m);
                  LOG.debug("Added custom min. interval " + m + " for host " + host + ".");
              } else if (m > 0) {
                LOG.error("Min. interval out of bounds on line " + String.valueOf(lineNo)
                  + " in the config. file: " + line);
              }

              // max. interval should be positive and below the global maximum
              if (M > 0 && M < defaultMax){
                hostSpecificMaxInterval.put(host,M);
                LOG.debug("Added custom max. interval " + M + " for host " + host + ".");
              } else if (M > 0){
                LOG.error("Max. interval out of bounds on line " + String.valueOf(lineNo)
                  + " in the config. file: " + line);
              }

              // zero values are ignored (default to global settings)
            }
          } catch (NumberFormatException e){
            LOG.error("No proper fetch intervals given on line " + String.valueOf(lineNo)
              + " in the config. file: " + line, e);
          }
        } else {
          LOG.error("Malformed (domain, min_interval, max_interval) triplet on line "
            + String.valueOf(lineNo) + " of the config. file: " + line);
        }
      }
    }
  }

  /**
   * Strip a URL, leaving only the host name.
   *
   * @param url url to get hostname for
   * @return hostname
   */
  public static String getHostName(String url) throws URISyntaxException {
    URI uri = new URI(url);
    String domain = uri.getHost();
    return domain;
  }

  /**
   * Returns the max_interval for this URL, which might depend on the host.
   *
   * @param url the URL to be scheduled
   * @param defaultMaxInterval the value to which to default if max_interval has not been configured for this host
   * @return the configured maximum interval or the default interval
   */
  public float getMaxInterval(Text url, float defaultMaxInterval){
    if (hostSpecificMaxInterval.isEmpty()) {
      return defaultMaxInterval;
    }
    String host;
    try {
      host = getHostName(url.toString());
    } catch (URISyntaxException e){
      return defaultMaxInterval;
    }
    if (hostSpecificMaxInterval.containsKey(host)){
      return hostSpecificMaxInterval.get(host);
    }
    return defaultMaxInterval;
  }

  /**
   * Returns the min_interval for this URL, which might depend on the host.
   *
   * @param url the URL to be scheduled
   * @param defaultMinInterval the value to which to default if min_interval has not been configured for this host
   * @return the configured minimum interval or the default interval
   */
  public float getMinInterval(Text url, float defaultMinInterval){
    if (hostSpecificMinInterval.isEmpty()) {
      return defaultMinInterval;
    }
    String host;
    try {
      host = getHostName(url.toString());
    } catch (URISyntaxException e){
      return defaultMinInterval;
    }
    if (hostSpecificMinInterval.containsKey(host)){
      return hostSpecificMinInterval.get(host);
    }
    return defaultMinInterval;
  }

  @Override
  public CrawlDatum setFetchSchedule(Text url, CrawlDatum datum,
      long prevFetchTime, long prevModifiedTime, long fetchTime,
      long modifiedTime, int state) {
    super.setFetchSchedule(url, datum, prevFetchTime, prevModifiedTime,
        fetchTime, modifiedTime, state);

    float interval = datum.getFetchInterval();
    long refTime = fetchTime;

    // https://issues.apache.org/jira/browse/NUTCH-1430
    interval = (interval == 0) ? defaultInterval : interval;

    if (datum.getMetaData().containsKey(Nutch.WRITABLE_FIXED_INTERVAL_KEY)) {
      // Is fetch interval preset in CrawlDatum MD? Then use preset interval
      FloatWritable customIntervalWritable = (FloatWritable) (datum
          .getMetaData().get(Nutch.WRITABLE_FIXED_INTERVAL_KEY));
      interval = customIntervalWritable.get();
    } else {
      if (modifiedTime <= 0)
        modifiedTime = fetchTime;
      switch (state) {
      case FetchSchedule.STATUS_MODIFIED:
        interval *= (1.0f - DEC_RATE);
        modifiedTime = fetchTime;
        break;
      case FetchSchedule.STATUS_NOTMODIFIED:
        interval *= (1.0f + INC_RATE);
        break;
      case FetchSchedule.STATUS_UNKNOWN:
        break;
      }
      if (SYNC_DELTA) {
        // try to synchronize with the time of change
        long delta = (fetchTime - modifiedTime) / 1000L;
        if (delta > interval)
          interval = delta;
        refTime = fetchTime - Math.round(delta * SYNC_DELTA_RATE * 1000);
      }

      // replace min_interval and max_interval with a domain-specific ones,
      // if so configured.
      float newMaxInterval = getMaxInterval(url, MAX_INTERVAL);
      float newMinInterval = getMinInterval(url, MIN_INTERVAL);
      if (interval < newMinInterval) {
        interval = newMinInterval;
      } else if (interval > newMaxInterval) {
        interval = newMaxInterval;
      }
    }

    datum.setFetchInterval(interval);
    datum.setFetchTime(refTime + Math.round(interval * 1000.0));
    datum.setModifiedTime(modifiedTime);
    return datum;
  }

  public static void main(String[] args) throws Exception {
    FetchSchedule fs = new AdaptiveFetchSchedule();
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
