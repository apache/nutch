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
package org.apache.nutch.hostdb;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Date;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.StringUtils;

import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.NutchWritable;

import com.tdunning.math.stats.TDigest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 *
 */
public class UpdateHostDbReducer
  implements Reducer<Text, NutchWritable, Text, HostDatum> {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());
  protected ResolverThread resolverThread = null;
  protected Integer numResolverThreads = 10;
  protected static Integer purgeFailedHostsThreshold = -1;
  protected static Integer recheckInterval = 86400000;
  protected static boolean checkFailed = false;
  protected static boolean checkNew = false;
  protected static boolean checkKnown = false;
  protected static boolean force = false;
  protected static long now = new Date().getTime();
  protected static String[] numericFields;
  protected static String[] stringFields;
  protected static int[] percentiles;
  protected static Text[] numericFieldWritables;
  protected static Text[] stringFieldWritables;
  
  protected BlockingQueue<Runnable> queue = new SynchronousQueue<>();
  protected ThreadPoolExecutor executor = null;

  /**
    * Configures the thread pool and prestarts all resolver threads.
    *
    * @param JobConf
    */
  public void configure(JobConf job) {
    purgeFailedHostsThreshold = job.getInt(UpdateHostDb.HOSTDB_PURGE_FAILED_HOSTS_THRESHOLD, -1);
    numResolverThreads = job.getInt(UpdateHostDb.HOSTDB_NUM_RESOLVER_THREADS, 10);
    recheckInterval = job.getInt(UpdateHostDb.HOSTDB_RECHECK_INTERVAL, 86400) * 1000;
    checkFailed = job.getBoolean(UpdateHostDb.HOSTDB_CHECK_FAILED, false);
    checkNew = job.getBoolean(UpdateHostDb.HOSTDB_CHECK_NEW, false);
    checkKnown = job.getBoolean(UpdateHostDb.HOSTDB_CHECK_KNOWN, false);
    force = job.getBoolean(UpdateHostDb.HOSTDB_FORCE_CHECK, false);
    numericFields = job.getStrings(UpdateHostDb.HOSTDB_NUMERIC_FIELDS);
    stringFields = job.getStrings(UpdateHostDb.HOSTDB_STRING_FIELDS);
    percentiles = job.getInts(UpdateHostDb.HOSTDB_PERCENTILES);
    
    // What fields do we need to collect metadata from
    if (numericFields != null) {
      numericFieldWritables = new Text[numericFields.length];
      for (int i = 0; i < numericFields.length; i++) {
        numericFieldWritables[i] = new Text(numericFields[i]);
      }
    }
    
    if (stringFields != null) {
      stringFieldWritables = new Text[stringFields.length];
      for (int i = 0; i < stringFields.length; i++) {
        stringFieldWritables[i] = new Text(stringFields[i]);
      }
    }

    // Initialize the thread pool with our queue
    executor = new ThreadPoolExecutor(numResolverThreads, numResolverThreads,
      5, TimeUnit.SECONDS, queue);

    // Run all threads in the pool
    executor.prestartAllCoreThreads();
  }

  /**
    *
    */
  public void reduce(Text key, Iterator<NutchWritable> values,
    OutputCollector<Text,HostDatum> output, Reporter reporter) throws IOException {

    Map<String,Map<String,Integer>> stringCounts = new HashMap<>();
    Map<String,Float> maximums = new HashMap<>();
    Map<String,Float> sums = new HashMap<>(); // used to calc averages
    Map<String,Integer> counts = new HashMap<>(); // used to calc averages
    Map<String,Float> minimums = new HashMap<>();
    Map<String,TDigest> tdigests = new HashMap<String,TDigest>();
    
    HostDatum hostDatum = new HostDatum();
    float score = 0;
    
    if (stringFields != null) {
      for (int i = 0; i < stringFields.length; i++) {
        stringCounts.put(stringFields[i], new HashMap<>());
      }
    }
    
    // Loop through all values until we find a non-empty HostDatum or use
    // an empty if this is a new host for the host db
    while (values.hasNext()) {
      Writable value = values.next().get();
      
      // Count crawl datum status's and collect metadata from fields
      if (value instanceof CrawlDatum) {
        CrawlDatum buffer = (CrawlDatum)value;
        
        // Set the correct status field
        switch (buffer.getStatus()) {
          case CrawlDatum.STATUS_DB_UNFETCHED:
            hostDatum.setUnfetched(hostDatum.getUnfetched() + 1);
            break;

          case CrawlDatum.STATUS_DB_FETCHED:
            hostDatum.setFetched(hostDatum.getFetched() + 1);
            break;

          case CrawlDatum.STATUS_DB_GONE:
            hostDatum.setGone(hostDatum.getGone() + 1);
            break;

          case CrawlDatum.STATUS_DB_REDIR_TEMP:
            hostDatum.setRedirTemp(hostDatum.getRedirTemp() + 1);
            break;

          case CrawlDatum.STATUS_DB_REDIR_PERM:
            hostDatum.setRedirPerm(hostDatum.getRedirPerm() + 1);
            break;

          case CrawlDatum.STATUS_DB_NOTMODIFIED:
            hostDatum.setNotModified(hostDatum.getNotModified() + 1);
            break;
        }
        
        // Record connection failures
        if (buffer.getRetriesSinceFetch() != 0) {
          hostDatum.incConnectionFailures();
        }
        
        // Only gather metadata statistics for proper fetched pages
        if (buffer.getStatus() == CrawlDatum.STATUS_DB_FETCHED || buffer.getStatus() == CrawlDatum.STATUS_DB_NOTMODIFIED) {            
          // Deal with the string fields
          if (stringFields != null) {
            for (int i = 0; i < stringFields.length; i++) {
              // Does this field exist?
              if (buffer.getMetaData().get(stringFieldWritables[i]) != null) {
                // Get it!
                String metadataValue = null;
                try {
                  metadataValue = buffer.getMetaData().get(stringFieldWritables[i]).toString();
                } catch (Exception e) {
                  LOG.error("Metadata field " + stringFields[i] + " is probably not a numeric value");
                }
              
                // Does the value exist?
                if (stringCounts.get(stringFields[i]).containsKey(metadataValue)) {
                  // Yes, increment it
                  stringCounts.get(stringFields[i]).put(metadataValue, stringCounts.get(stringFields[i]).get(metadataValue) + 1);
                } else {
                  // Create it!
                  stringCounts.get(stringFields[i]).put(metadataValue, 1);
                }
              }
            }
          }
          
          // Deal with the numeric fields
          if (numericFields != null) {
            for (int i = 0; i < numericFields.length; i++) {
              // Does this field exist?
              if (buffer.getMetaData().get(numericFieldWritables[i]) != null) {
                try {
                  // Get it!
                  Float metadataValue = Float.parseFloat(buffer.getMetaData().get(numericFieldWritables[i]).toString());
                  
                  // Does the median value exist?
                  if (tdigests.containsKey(numericFields[i])) {
                    tdigests.get(numericFields[i]).add(metadataValue);
                  } else {
                    // Create it!
                    TDigest tdigest = TDigest.createDigest(100);
                    tdigest.add((double)metadataValue);
                    tdigests.put(numericFields[i], tdigest);
                  }
                
                  // Does the minimum value exist?
                  if (minimums.containsKey(numericFields[i])) {
                    // Write if this is lower than existing value
                    if (metadataValue < minimums.get(numericFields[i])) {
                      minimums.put(numericFields[i], metadataValue);
                    }
                  } else {
                    // Create it!
                    minimums.put(numericFields[i], metadataValue);
                  }
                  
                  // Does the maximum value exist?
                  if (maximums.containsKey(numericFields[i])) {
                    // Write if this is lower than existing value
                    if (metadataValue > maximums.get(numericFields[i])) {
                      maximums.put(numericFields[i], metadataValue);
                    }
                  } else {
                    // Create it!
                    maximums.put(numericFields[i], metadataValue);
                  }
                  
                  // Sum it up!
                  if (sums.containsKey(numericFields[i])) {
                    // Increment
                    sums.put(numericFields[i], sums.get(numericFields[i]) + metadataValue);
                    counts.put(numericFields[i], counts.get(numericFields[i]) + 1);
                  } else {
                    // Create it!
                    sums.put(numericFields[i], metadataValue);
                    counts.put(numericFields[i], 1);
                  }
                } catch (Exception e) {
                  LOG.error(e.getMessage() + " when processing values for " + key.toString());
                }
              }
            }
          }
        }
      }
      
      // 
      if (value instanceof HostDatum) {
        HostDatum buffer = (HostDatum)value;

        // Check homepage URL
        if (buffer.hasHomepageUrl()) {
          hostDatum.setHomepageUrl(buffer.getHomepageUrl());
        }

        // Check lastCheck timestamp
        if (!buffer.isEmpty()) {
          hostDatum.setLastCheck(buffer.getLastCheck());
        }

        // Check and set DNS failures
        if (buffer.getDnsFailures() > 0) {
          hostDatum.setDnsFailures(buffer.getDnsFailures());
        }

        // Check and set connection failures
        if (buffer.getConnectionFailures() > 0) {
          hostDatum.setConnectionFailures(buffer.getConnectionFailures());
        }
        
        // Check metadata
        if (!buffer.getMetaData().isEmpty()) {
          hostDatum.setMetaData(buffer.getMetaData());
        }

        // Check and set score (score from Web Graph has precedence)
        if (buffer.getScore() > 0) {
          hostDatum.setScore(buffer.getScore());
        }
      }

      // Check for the score
      if (value instanceof FloatWritable) {
        FloatWritable buffer = (FloatWritable)value;
        score = buffer.get();
      }
    }

    // Check if score was set from Web Graph
    if (score > 0) {
      hostDatum.setScore(score);
    }
    
    // Set metadata
    for (Map.Entry<String, Map<String,Integer>> entry : stringCounts.entrySet()) {
      for (Map.Entry<String,Integer> subEntry : entry.getValue().entrySet()) {
        hostDatum.getMetaData().put(new Text(entry.getKey() + "." + subEntry.getKey()), new IntWritable(subEntry.getValue()));
      }
    }
    for (Map.Entry<String, Float> entry : maximums.entrySet()) {
      hostDatum.getMetaData().put(new Text("max." + entry.getKey()), new FloatWritable(entry.getValue()));
    }
    for (Map.Entry<String, Float> entry : sums.entrySet()) {
      hostDatum.getMetaData().put(new Text("avg." + entry.getKey()), new FloatWritable(entry.getValue() / counts.get(entry.getKey())));
    }
    for (Map.Entry<String, TDigest> entry : tdigests.entrySet()) {
      // Emit all percentiles
      for (int i = 0; i < percentiles.length; i++) {
        hostDatum.getMetaData().put(new Text("pct" + Integer.toString(percentiles[i]) + "." + entry.getKey()), new FloatWritable((float)entry.getValue().quantile(0.5)));
      }
    }      
    for (Map.Entry<String, Float> entry : minimums.entrySet()) {
      hostDatum.getMetaData().put(new Text("min." + entry.getKey()), new FloatWritable(entry.getValue()));
    }
    
    reporter.incrCounter("UpdateHostDb", "total_hosts", 1);

    // See if this record is to be checked
    if (shouldCheck(hostDatum)) {
      // Make an entry
      resolverThread = new ResolverThread(key.toString(), hostDatum, output, reporter, purgeFailedHostsThreshold);

      // Add the entry to the queue (blocking)
      try {
        queue.put(resolverThread);
      } catch (InterruptedException e) {
        LOG.error("UpdateHostDb: " + StringUtils.stringifyException(e));
      }

      // Do not progress, the datum will be written in the resolver thread
      return;
    } else {
      reporter.incrCounter("UpdateHostDb", "skipped_not_eligible", 1);
      LOG.info("UpdateHostDb: " + key.toString() + ": skipped_not_eligible");
    }

    // Write the host datum if it wasn't written by the resolver thread
    output.collect(key, hostDatum);
  }

  /**
    * Determines whether a record should be checked.
    *
    * @param HostDatum
    * @return boolean
    */
  protected boolean shouldCheck(HostDatum datum) {
    // Whether a new record is to be checked
    if (checkNew && datum.isEmpty()) {
      return true;
    }

    // Whether existing known hosts should be rechecked
    if (checkKnown && !datum.isEmpty() && datum.getDnsFailures() == 0) {
      return isEligibleForCheck(datum);
    }

    // Whether failed records are forced to be rechecked
    if (checkFailed && datum.getDnsFailures() > 0) {
      return isEligibleForCheck(datum);
    }

    // It seems this record is not to be checked
    return false;
  }

  /**
    * Determines whether a record is eligible for recheck.
    *
    * @param HostDatum
    * @return boolean
    */
  protected boolean isEligibleForCheck(HostDatum datum) {
    // Whether an existing host, known or unknown, if forced to be rechecked
    if (force || datum.getLastCheck().getTime() +
      (recheckInterval * datum.getDnsFailures() + 1) > now) {
      return true;
    }

    return false;
  }

  /**
    * Shut down all running threads and wait for completion.
    */
  public void close() {
    LOG.info("UpdateHostDb: feeder finished, waiting for shutdown");

    // If we're here all keys have been fed and we can issue a shut down
    executor.shutdown();

    boolean finished = false;

    // Wait until all resolvers have finished
    while (!finished) {
      try {
        // Wait for the executor to shut down completely
        if (!executor.isTerminated()) {
          LOG.info("UpdateHostDb: resolver threads waiting: " + Integer.toString(executor.getPoolSize()));
          Thread.sleep(1000);
        } else {
          // All is well, get out
          finished = true;
        }
      } catch (InterruptedException e) {
        // Huh?
        LOG.warn(StringUtils.stringifyException(e));
      }
    }
  }
}