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

import java.net.InetAddress;

import com.google.common.net.InetAddresses;

/**
 * Parse a <a href=
 * "https://en.wikipedia.org/wiki/Classless_Inter-Domain_Routing">CIDR</a> block
 * notation and test whether an IP address is contained in the subnet range
 * defined by the CIDR.
 */
public class CIDR {
  InetAddress addr;
  int mask;

  public CIDR(InetAddress address, int mask) {
    this.addr = address;
    this.mask = mask;
  }

  public CIDR(String cidr) throws IllegalArgumentException {
    String ipStr = cidr;
    int sep = cidr.indexOf('/');
    if (sep > -1) {
      ipStr = cidr.substring(0, sep);
    }
    addr = InetAddresses.forString(ipStr);
    if (sep > -1) {
      mask = Integer.parseInt(cidr.substring(sep + 1));
    } else {
      mask = addr.getAddress().length * 8;
    }
    if (cidr.indexOf(':') > -1 && addr.getAddress().length == 4) {
      // IPv4-mapped IPv6 addresses are automatically converted to IPv4,
      // need to shift the mask
      mask = Math.max(0, mask - 96);
    }
  }

  public boolean contains(InetAddress address) {
    byte[] addr0 = addr.getAddress();
    byte[] addr1 = address.getAddress();
    if (addr0.length != addr1.length) {
      // not comparing IPv4 and IPv6 addresses
      return false;
    }
    for (int i = 0; i < addr0.length; i++) {
      int remainingMaskBits = mask - (i * 8);
      if (remainingMaskBits <= 0)
        return true;
      int m = ~(0xff >> remainingMaskBits); // mask for byte under cursor
      if ((addr0[i] & m) != (addr1[i] & m))
        return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return addr + "/" + mask;
  }
}
