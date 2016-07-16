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

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple runnable that performs DNS lookup for a single host.
 */
public class ResolverThread implements Runnable {

  public static final Logger LOG = LoggerFactory.getLogger(ResolverThread.class);

  protected String host = null;
  protected HostDatum datum = null;
  protected Text hostText = new Text();
  protected OutputCollector<Text,HostDatum> output;
  protected Reporter reporter;
  protected int purgeFailedHostsThreshold;

  /**
   * Constructor.
   */
  public ResolverThread(String host, HostDatum datum,
    OutputCollector<Text,HostDatum> output, Reporter reporter, int purgeFailedHostsThreshold) {

    hostText.set(host);
    this.host = host;
    this.datum = datum;
    this.output = output;
    this.reporter = reporter;
    this.purgeFailedHostsThreshold = purgeFailedHostsThreshold;
  }

  /**
   *
   */
  public void run() {
    // Resolve the host and act appropriatly
    try {
      // Throws an exception if host is not found
      InetAddress inetAddr = InetAddress.getByName(host);

      if (datum.isEmpty()) {
        reporter.incrCounter("UpdateHostDb", "new_known_host" ,1);
        datum.setLastCheck();
        LOG.info(host + ": new_known_host " + datum);
      } else if (datum.getDnsFailures() > 0) {
        reporter.incrCounter("UpdateHostDb", "rediscovered_host" ,1);
        datum.setLastCheck();
        datum.setDnsFailures(0);
        LOG.info(host + ": rediscovered_host " + datum);
      } else {
        reporter.incrCounter("UpdateHostDb", "existing_known_host", 1);
        datum.setLastCheck();
        LOG.info(host + ": existing_known_host " + datum);
      }

      // Write the host datum
      output.collect(hostText, datum);
    } catch (UnknownHostException e) {
      try {
        // If the counter is empty we'll initialize with date = today and 1 failure
        if (datum.isEmpty()) {
          datum.setLastCheck();
          datum.setDnsFailures(1);
          output.collect(hostText, datum);
          reporter.incrCounter("UpdateHostDb", "new_unknown_host", 1);
          LOG.info(host + ": new_unknown_host " + datum);
        } else {
          datum.setLastCheck();
          datum.incDnsFailures();

          // Check if this host should be forgotten
          if (purgeFailedHostsThreshold == -1 ||
            purgeFailedHostsThreshold < datum.getDnsFailures()) {

            output.collect(hostText, datum);
            reporter.incrCounter("UpdateHostDb", "existing_unknown_host" ,1);
            LOG.info(host + ": existing_unknown_host " + datum);
          } else {
            reporter.incrCounter("UpdateHostDb", "purged_unknown_host" ,1);
            LOG.info(host + ": purged_unknown_host " + datum);
          }
        }

        reporter.incrCounter("UpdateHostDb",
          Integer.toString(datum.numFailures()) + "_times_failed", 1);
      } catch (Exception ioe) {
        LOG.warn(StringUtils.stringifyException(ioe));
      }
    } catch (Exception e) {
      LOG.warn(StringUtils.stringifyException(e));
    }
    
    reporter.incrCounter("UpdateHostDb", "checked_hosts", 1);
  }
}