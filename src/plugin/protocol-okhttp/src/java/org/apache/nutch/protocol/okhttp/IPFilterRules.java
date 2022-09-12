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
package org.apache.nutch.protocol.okhttp;

import java.lang.invoke.MethodHandles;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Optionally limit or block connections to IP address ranges
 * (localhost/loopback or site-local addresses, subnet ranges given in CIDR
 * notation, or single IP addresses).
 * 
 * IP filter rules are built from two Nutch properties:
 * <ul>
 * <li><code>http.filter.ipaddress.include</code> defines all allowed IP ranges.
 * If not defined or empty all IP addresses (and not explicitly excluded) are
 * allowed.
 * <li><code>http.filter.ipaddress.exclude</code> defines excluded IP address
 * ranges.
 * </ul>
 *
 * IP ranges can be defined as
 * <ul>
 * <li>IP address, e.g. <code>127.0.0.1</code> or <code>::1</code> (IPv6)</li>
 * <li>CIDR notation, e.g. <code>192.168.0.0/16</code> or
 * <code>fd00::/8</code></li>
 * <li><code>localhost</code> or <code>loopback</code> applies to all IP
 * addresses for which {@link InetAddress#isLoopbackAddress()} is true</li>
 * <li><code>sitelocal</code> applies to all IP
 * addresses for which {@link InetAddress#isSiteLocalAddress()} is true</li>
 * </ul>
 *
 * Multiple IP ranges are separated by a comma, e.g. <code>loopback,sitelocal,fd00::/8</code>
 *
 */
public class IPFilterRules {

  protected static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  List<Predicate<InetAddress>> includeRules;
  List<Predicate<InetAddress>> excludeRules;

  public IPFilterRules(Configuration conf) {
    includeRules = parseIPRules(conf, "http.filter.ipaddress.include");
    excludeRules = parseIPRules(conf, "http.filter.ipaddress.exclude");
  }

  public boolean isEmpty() {
    return !(includeRules.size() > 0 || excludeRules.size() > 0);
  }

  public boolean accept(InetAddress address) {
    boolean accept = true;
    if (includeRules.size() > 0) {
      accept = false;
      for (Predicate<InetAddress> rule : includeRules) {
        if (rule.test(address)) {
          accept = true;
          break;
        }
      }
    }
    if (accept && excludeRules.size() > 0) {
      for (Predicate<InetAddress> rule : excludeRules) {
        if (rule.test(address)) {
          accept = false;
          break;
        }
      }
    }
    return accept;
  }

  private static List<Predicate<InetAddress>> parseIPRules(Configuration conf,
      String ipRuleProperty) {
    List<Predicate<InetAddress>> rules = new ArrayList<>();
    String[] ipRules = conf.getStrings(ipRuleProperty);
    if (ipRules == null) {
      return rules;
    }
    for (String ipRule : ipRules) {
      switch (ipRule.toLowerCase()) {
      case "localhost":
      case "loopback":
        rules.add((InetAddress a) -> a.isLoopbackAddress());
        break;
      case "sitelocal":
        rules.add((InetAddress a) -> a.isSiteLocalAddress());
        break;
      default:
        try {
          CIDR cidr = new CIDR(ipRule);
          rules.add((InetAddress a) -> cidr.contains(a));
        } catch (IllegalArgumentException e) {
          LOG.error(
              "Failed to parse {} as CIDR, ignoring to configure IP rules ({})",
              ipRule, ipRuleProperty);
        }
      }
    }
    if (rules.size() > 0) {
      LOG.info("Found {} IP filter rules for {}", rules.size(), ipRuleProperty);
    }
    return rules;
  }

}
