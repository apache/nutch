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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.InetAddress;
import java.util.function.Function;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.protocol.AbstractHttpProtocolPluginTest;
import org.junit.Test;

import com.google.common.net.InetAddresses;

/**
 * Test cases for protocol-okhttp IP address filtering
 */
public class TestIPAddressFiltering extends AbstractHttpProtocolPluginTest {

  @Override
  protected String getPluginClassName() {
    return "org.apache.nutch.protocol.okhttp.OkHttp";
  }

  public InetAddress parseIP(String ip) {
    // the Java built-in may perform DNS lookup (and throw UnknownHostException)
    // if not a well-formed IP address:
    //   InetAddress.getByName(ip);

    // use Guava because it does not perform DNS lookups, may throw
    // IllegalArgumentException if IP address is not well-formed
    return InetAddresses.forString(ip);
  }

  public void testCIDRcontains(String cidr, String ip) {
    CIDR c = new CIDR(cidr);
    InetAddress i = parseIP(ip);
    assertTrue(i + " should be in " + c, c.contains(i));
  }

  public void testCIDRnotContains(String cidr, String ip) {
    CIDR c = new CIDR(cidr);
    InetAddress i = parseIP(ip);
    assertFalse(i + " should not be in " + c, c.contains(i));
  }

  /** Tests for {@link CIDR} */
  @Test
  public void testCIDRs() {
    // private subnets IPv4
    testCIDRcontains("127.0.0.0/8", "127.0.0.1");
    testCIDRcontains("10.0.0.0/8", "10.0.0.13");
    testCIDRcontains("172.16.0.0/12", "172.17.0.0");
    testCIDRcontains("192.168.0.0/16", "192.168.0.1");

    // private subnets IPv6
    testCIDRcontains("::1/128", "::1");
    testCIDRcontains("127.0.0.0/8", "::ffff:127.0.0.1");
    testCIDRcontains("::ffff:7f00:0/104", "::ffff:127.0.0.1");
    testCIDRcontains("fd00::/8", "fd12:3456:789a:1::1");
    testCIDRcontains("fe80::/10", "fe80::2f29:b6f0:a4c:32ae");

    // test single IP address (with and without mask)
    testCIDRcontains("127.0.0.1", "127.0.0.1");
    testCIDRcontains("127.0.0.1/24", "127.0.0.1");

    // test off-by-one boundaries
    testCIDRnotContains("127.0.0.0/8", "128.0.0.0");
    testCIDRnotContains("10.0.0.0/8", "11.0.0.0");
    testCIDRnotContains("10.0.0.0/8", "9.255.255.255");
    testCIDRnotContains("172.16.0.0/12", "172.32.0.0");
    testCIDRnotContains("172.16.0.0/12", "171.255.255.255");
  }

  public void testFilter(Configuration conf, String[] included, String[] excluded) {
    IPFilterRules ipFilterRules = new IPFilterRules(conf);
    for (String address : included) {
      assertTrue("Address " + address + " should be included",
          ipFilterRules.accept(parseIP(address)));
    }
    for (String address : excluded) {
      assertFalse("Address " + address + " should be excluded",
          ipFilterRules.accept(parseIP(address)));
    }
  }

  /** Tests for {@link IPFilterRules} */
  @Test
  public void testIPAddressFilterRules() {
    String[] publicAddresses = {"93.184.216.34", "93.184.216.43"};
    String[] loopbackAddresses = {"127.0.0.1", "127.0.0.2", "::1"};
    String[] sitelocalAddresses = {"10.0.0.13", "172.17.0.0", "192.168.0.1"};

    conf.set("http.filter.ipaddress.include", "");
    conf.set("http.filter.ipaddress.exclude", "localhost");
    testFilter(conf, new String[0], loopbackAddresses);

    conf.set("http.filter.ipaddress.exclude", "loopback,sitelocal");
    testFilter(conf, publicAddresses, loopbackAddresses);
    testFilter(conf, publicAddresses, sitelocalAddresses);

    conf.set("http.filter.ipaddress.include", "93.184.216.0/8");
    conf.set("http.filter.ipaddress.exclude", "");
    testFilter(conf, publicAddresses, loopbackAddresses);

    conf.set("http.filter.ipaddress.include", "localhost");
    conf.set("http.filter.ipaddress.exclude", "");
    testFilter(conf, loopbackAddresses, publicAddresses);
  }
  
  public void testPredefinedAddressRange(String ipAddress, String type) {
    try {
      InetAddress addr = InetAddresses.forString(ipAddress);
      Function<InetAddress,Boolean> pred = null;
      switch (type.toLowerCase()) {
      case "localhost":
      case "loopback":
        pred = InetAddress::isLoopbackAddress;
        break;
      case "sitelocal":
        pred = InetAddress::isSiteLocalAddress;
        break;
      default:
        fail("Unknown IP address type " + type);
      }
      assertTrue(ipAddress + " is not recognized as " + type + " address", pred.apply(addr));
    } catch (IllegalArgumentException e) {
      fail("Not a valid IP address string: " + ipAddress);
    }
  }

  /**
   * Verify that certain IP addresses are matched by predefined IP classes:
   * localhost, loopback, sitelocal. This verifies that the predefined classes
   * are properly mapped to the underlying predicates of the class
   * {@link InetAddress}.
   */
  @Test
  public void testPredefinedRanges() throws Exception {
    testPredefinedAddressRange("127.0.0.1", "localhost");
    testPredefinedAddressRange("127.0.0.1", "loopback");
    testPredefinedAddressRange("10.0.0.13", "sitelocal");
    testPredefinedAddressRange("172.17.0.0", "sitelocal");
    testPredefinedAddressRange("192.168.0.1", "sitelocal");

    testPredefinedAddressRange("::1", "loopback");
    testPredefinedAddressRange("::ffff:127.0.0.1", "loopback");
    // fec0::/10 - Java follows the "old" standard to define private IPv6 addresses
    testPredefinedAddressRange("fec0::", "sitelocal");
    // fd::/8 - not (yet?) recognized as site-local address by InetAddress::isSiteLocalAddress
    //testPredefinedAddressRange("fd12:3456:789a:1::1", "sitelocal");
  }

  /**
   * Test whether connections are blocked according to the IP filter
   * configuration
   */
  @Test
  public void testConnectionBlocking() throws Exception {
    localHost = "127.0.0.1";
    launchServer("/", (responseHeader + simpleContent).getBytes(UTF_8));

    // without filter configured
    conf.set("http.filter.ipaddress.exclude", "");
    http.setConf(conf);
    fetchPage("/", 200, "text/html");

    // filter localhost
    conf.set("http.filter.ipaddress.exclude", "localhost");
    http.setConf(conf);
    fetchPage("/", -1, "text/html");

    // filter loopback
    conf.set("http.filter.ipaddress.exclude", "localhost");
    http.setConf(conf);
    fetchPage("/", -1, "text/html");

    // filter by IP
    conf.set("http.filter.ipaddress.exclude", "127.0.0.1");
    http.setConf(conf);
    fetchPage("/", -1, "text/html");

    // filter by CIDR
    conf.set("http.filter.ipaddress.exclude", "127.0.0.0/8");
    http.setConf(conf);
    fetchPage("/", -1, "text/html");
 }

}
