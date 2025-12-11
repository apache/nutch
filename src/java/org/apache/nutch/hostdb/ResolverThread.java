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

import java.lang.invoke.MethodHandles;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.util.StringUtils;

import org.apache.nutch.metrics.NutchMetrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple runnable that performs DNS lookup for a single host.
 */
public class ResolverThread implements Runnable {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  protected String host = null;
  protected HostDatum datum = null;
  protected Text hostText = new Text();
  protected Context context;
  protected int purgeFailedHostsThreshold;

  /**
   * Overloaded constructor.
   * @param host name of the host to lookup
   * @param datum accompanying host information
   * @param context {@link org.apache.hadoop.mapreduce.Reducer.Context} for
   * writing custom counters and output.
   * @param purgeFailedHostsThreshold int value which marks the maximum failed
   * DNS lookups before a given host is purged from the hostdb
   */
  public ResolverThread(String host, HostDatum datum,
    Context context, int purgeFailedHostsThreshold) {

    hostText.set(host);
    this.host = host;
    this.datum = datum;
    this.context = context;
    this.purgeFailedHostsThreshold = purgeFailedHostsThreshold;
  }

  /**
   *
   */
  @Override
  public void run() {
    // Resolve the host and act appropriatly
    try {
      // Throws an exception if host is not found
      @SuppressWarnings("unused")
      InetAddress inetAddr = InetAddress.getByName(host);

      if (datum.isEmpty()) {
        context.getCounter(NutchMetrics.GROUP_HOSTDB,
            NutchMetrics.HOSTDB_NEW_KNOWN_HOST_TOTAL).increment(1);
        datum.setLastCheck();
        LOG.info("{}: new_known_host {}", host, datum);
      } else if (datum.getDnsFailures() > 0) {
        context.getCounter(NutchMetrics.GROUP_HOSTDB,
            NutchMetrics.HOSTDB_REDISCOVERED_HOST_TOTAL).increment(1);
        datum.setLastCheck();
        datum.setDnsFailures(0l);
        LOG.info("{}: rediscovered_host {}", host, datum);
      } else {
        context.getCounter(NutchMetrics.GROUP_HOSTDB,
            NutchMetrics.HOSTDB_EXISTING_KNOWN_HOST_TOTAL).increment(1);
        datum.setLastCheck();
        LOG.info("{}: existing_known_host {}", host, datum);
      }

      // Write the host datum
      context.write(hostText, datum);
    } catch (UnknownHostException e) {
      try {
        // If the counter is empty we'll initialize with date = today and 1 failure
        if (datum.isEmpty()) {
          datum.setLastCheck();
          datum.setDnsFailures(1l);
          context.write(hostText, datum);
          context.getCounter(NutchMetrics.GROUP_HOSTDB,
              NutchMetrics.HOSTDB_NEW_UNKNOWN_HOST_TOTAL).increment(1);
          LOG.info("{}: new_unknown_host {}", host, datum);
        } else {
          datum.setLastCheck();
          datum.incDnsFailures();

          // Check if this host should be forgotten
          if (purgeFailedHostsThreshold == -1 ||
            purgeFailedHostsThreshold < datum.getDnsFailures()) {

            context.write(hostText, datum);
            context.getCounter(NutchMetrics.GROUP_HOSTDB,
                NutchMetrics.HOSTDB_EXISTING_UNKNOWN_HOST_TOTAL).increment(1);
            LOG.info("{}: existing_unknown_host {}", host, datum);
          } else {
            context.getCounter(NutchMetrics.GROUP_HOSTDB,
                NutchMetrics.HOSTDB_PURGED_UNKNOWN_HOST_TOTAL).increment(1);
            LOG.info("{}: purged_unknown_host {}", host, datum);
          }
        }

        // Dynamic counter based on failure count - can't cache
        context.getCounter(NutchMetrics.GROUP_HOSTDB, createFailureCounterLabel(datum)).increment(1);
      } catch (Exception ioe) {
        LOG.warn(StringUtils.stringifyException(ioe));
      }
    } catch (Exception e) {
      LOG.warn(StringUtils.stringifyException(e));
    }

    context.getCounter(NutchMetrics.GROUP_HOSTDB,
        NutchMetrics.HOSTDB_CHECKED_HOSTS_TOTAL).increment(1);
  }

  private String createFailureCounterLabel(HostDatum datum) {
    // Hadoop will allow no more than 120 distinct counters. If we have a large
    // number of distinct failures, we'll exceed the limit, Hadoop will complain,
    // the job will fail. Let's limit the amount of possibilities by grouping
    // the numFailures in buckets. NUTCH-3096
    String label = null;
    long n = datum.numFailures();
    if (n < 4) {
      label = Long.toString(n);
    } else if (n > 3 && n < 11) {
      label = "4-10";
    } else {
      label = ">10";
    }

    return label + "_times_failed";
  }
}
