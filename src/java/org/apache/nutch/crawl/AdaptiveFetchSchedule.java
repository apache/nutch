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
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.commons.lang3.StringUtils;

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
import java.time.Duration;

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
 * The class also allows specifying custom min. and max. re-fetch intervals per
 * hostname, in adaptive-host-specific-intervals.txt. If they are specified,
 * the calculated re-fetch interval for a URL matching the hostname will not be
 * allowed to fall outside of the corresponding range, instead of the default
 * range.
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

  private Map<String, Float> hostSpecificMaxInterval = new HashMap<>();
  
  private Map<String, Float> hostSpecificMinInterval = new HashMap<>();

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
    } catch (IOException e) {
      LOG.error("Failed reading the configuration file:", e);
    }
  }

  /**
   * Load host-specific minimal and maximal refetch intervals from
   * the configuration file into the corresponding HashMaps.
   *
   * @param fileName   the name of the configuration file containing
   *                   the specific intervals
   * @param defaultMin the value of the default min interval
   * @param defaultMax the value of the default max interval
   */
  private void setHostSpecificIntervals(String fileName,
      float defaultMin, float defaultMax) throws IOException {
    // Setup for reading the config file.
    Reader configReader = null;
    configReader = conf.getConfResourceAsReader(fileName);
    if (configReader == null) {
      configReader = new FileReader(fileName);
    }
    BufferedReader reader = new BufferedReader(configReader);
    String line;
    int lineNo = 0;

    // Read the file line by line.
    while ((line = reader.readLine()) != null) {
      lineNo++;

      // Skip blank lines and comments.
      if (StringUtils.isBlank(line) || line.startsWith("#")) {
        continue;
      }

      // Trim and partition the line.
      line = line.trim();
      String[] parts = line.split("\\s+");

      // There should be three parts.
      if (parts.length != 3) {
        LOG.error(
            "Malformed (domain, min_interval, max_interval) triplet on line {} of the config. file: `{}`",
            lineNo, line);
        continue;
      }

      // Normalize the parts.
      String host = parts[0].trim().toLowerCase();
      String minInt = parts[1].trim();
      String maxInt = parts[2].trim();

      // "0" and "default" both mean `use default interval`; normalize to "0".
      if (minInt.equalsIgnoreCase("default")) { minInt = "0"; }
      if (maxInt.equalsIgnoreCase("default")) { maxInt = "0"; }

      // Convert intervals to float and ignore the line in case of failure.
      float m, M;
      try {
        m = Float.parseFloat(minInt);
        M = Float.parseFloat(maxInt);
      } catch (NumberFormatException e) {
        LOG.error(
            "Improper fetch intervals given on line {} in the config. file `{}`: {}",
            lineNo, line, e.toString());
        continue;
      }

      // If both intervals are set to default,
      // ignore the line and issue a warning.
      if (m == 0 && M == 0) {
        LOG.warn(
            "Ignoring default interval values on line {} of config. file: `{}`",
            lineNo, line);
        continue;
      }

      // Replace the zero with the default value.
      if (m == 0) {
        m = defaultMin;
      } else if (M == 0) {
        M = defaultMax;
      }

      // Intervals cannot be negative and the min cannot be above the max
      // (we assume here that the default values satisfy this).
      if (m < 0 || M < 0) {
        LOG.error(
            "Improper fetch intervals given on line {} in the config. file: `{}`: intervals cannot be negative",
            lineNo, line);
        continue;
      }

      if (m > M) {
        LOG.error(
            "Improper fetch intervals given on line {} in the config. file: `{}`: min. interval cannot be above max. interval",
            lineNo, line);
        continue;
      }

      // The custom intervals should respect the boundaries of the default values.
      if (m < defaultMin) {
        LOG.error(
            "Min. interval out of bounds ({}) on line {} in the config. file: `{}`",
            defaultMin, lineNo, line);
        continue;
      }

      if (M > defaultMax) {
        LOG.error(
            "Max. interval out of bounds ({}) on line {} in the config. file: `{}`",
            defaultMax, lineNo, line);
        continue;
      }

      // If all is well, store the specific intervals.
      hostSpecificMinInterval.put(host, m);
      LOG.debug("Added custom min. interval {} for host {}.", m, host);

      hostSpecificMaxInterval.put(host, M);
      LOG.debug("Added custom max. interval {} for host {}.", M, host);

    }
  }

  /**
   * Strip a URL, leaving only the hostname.
   *
   * @param url the URL for which to get the hostname
   * @return hostname
   * @throws URISyntaxException if the given string violates RFC 2396
   */
  public static String getHostName(String url) throws URISyntaxException {
    URI uri = new URI(url);
    String domain = uri.getHost();
    return domain;
  }

  /**
   * Returns the custom max. refetch interval for this URL,
   * if specified for the corresponding hostname.
   *
   * @param url the URL to be scheduled
   * @return the configured max. interval or null
   */
  public Float getCustomMaxInterval(Text url) {
    if (hostSpecificMaxInterval.isEmpty()) {
      return null;
    }
    String host;
    try {
      host = getHostName(url.toString());
    } catch (URISyntaxException e){
      return null;
    }
    if (!hostSpecificMaxInterval.containsKey(host)) {
      return null;
    }
    return hostSpecificMaxInterval.get(host);
  }

  /**
   * Returns the custom min. refetch interval for this URL,
   * if specified for the corresponding hostname.
   *
   * @param url the URL to be scheduled
   * @return the configured min. interval or null
   */
  public Float getCustomMinInterval(Text url) {
    if (hostSpecificMinInterval.isEmpty()) {
      return null;
    }
    String host;
    try {
      host = getHostName(url.toString());
    } catch (URISyntaxException e){
      return null;
    }
    if (!hostSpecificMinInterval.containsKey(host)) {
      return null;
    }
    return hostSpecificMinInterval.get(host);
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

      // Ensure the interval does not fall outside of bounds
      float minInterval = (getCustomMinInterval(url) != null) ? getCustomMinInterval(url) : MIN_INTERVAL;
      float maxInterval = (getCustomMaxInterval(url) != null) ? getCustomMaxInterval(url) : MAX_INTERVAL;
      
      if (SYNC_DELTA) {
        // try to synchronize with the time of change
        long delta = (fetchTime - modifiedTime);
        if (delta > (interval * 1000))
          interval = delta / 1000L;
        // offset: a fraction (sync_delta_rate) of the difference between the last modification time, and the last fetch time.
        long offset = Math.round(delta * SYNC_DELTA_RATE);
        long maxIntervalMillis = (long) maxInterval * 1000L;
        if (LOG.isTraceEnabled()) {
          LOG.trace("delta (days): {}; offset (days): {}; maxInterval (days): {}", 
              Duration.ofMillis(delta).toDays(), Duration.ofMillis(offset).toDays(), Duration.ofMillis(maxIntervalMillis).toDays());
        }
        // convert the offset to a ratio of max interval: avoid next fetchTime in the past, and mimic fetches within max interval
        if (delta > 0 && offset > maxIntervalMillis) {
          offset = offset / delta * maxIntervalMillis; // ex: 9/30*7 = 2.1
        }
        refTime = fetchTime - offset;
      }

      if (interval < minInterval) {
        interval = minInterval;
      } else if (interval > maxInterval) {
        interval = maxInterval;
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
      LOG.info("{}. {}\twill fetch at {}\tinterval {} days\tmissed {}", i,
          changed, (p.getFetchTime() / delta),
          (p.getFetchInterval() / SECONDS_PER_DAY), miss);
      if (p.getFetchTime() <= curTime) {
        fetchCnt++;
        // Text (url) required by the API, but not relevant here.
        fs.setFetchSchedule(new Text(), p, p
            .getFetchTime(), p.getModifiedTime(), curTime, lastModified,
            changed ? FetchSchedule.STATUS_MODIFIED
                : FetchSchedule.STATUS_NOTMODIFIED);
        LOG.info("\tfetched and adjusted:\twill fetch at {}\tinterval {} days",
            (p.getFetchTime() / delta),
            (p.getFetchInterval() / SECONDS_PER_DAY));
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
    LOG.info("Total missed: {}, max miss: {}", totalMiss, maxMiss);
    LOG.info("Page changed {} times, fetched {} times.", changeCnt, fetchCnt);
  }
}
