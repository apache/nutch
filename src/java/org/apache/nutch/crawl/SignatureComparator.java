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

import java.util.Comparator;

public class SignatureComparator implements Comparator {
  public int compare(Object o1, Object o2) {
    return _compare(o1, o2);
  }
  
  public static int _compare(Object o1, Object o2) {
    if (o1 == null && o2 == null) return 0;
    if (o1 == null) return -1;
    if (o2 == null) return 1;
    if (!(o1 instanceof byte[])) return -1;
    if (!(o2 instanceof byte[])) return 1;
    byte[] data1 = (byte[])o1;
    byte[] data2 = (byte[])o2;
    return _compare(data1, 0, data1.length, data2, 0, data2.length);
  }
  
  public static int _compare(byte[] data1, int s1, int l1, byte[] data2, int s2, int l2) {
    if (l2 > l1) return -1;
    if (l2 < l1) return 1;
    int res = 0;
    for (int i = 0; i < l1; i++) {
      res = (data1[s1 + i] - data2[s2 + i]);
      if (res != 0) return res;
    }
    return 0;
  }
}
