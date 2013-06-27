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

import java.nio.ByteBuffer;

public class SignatureComparator {
  public static int compare(byte[] data1, byte[] data2) {
    if (data1 == null && data2 == null) return 0;
    if (data1 == null) return -1;
    if (data2 == null) return 1;
    return _compare(data1, 0, data1.length, data2, 0, data2.length);
  }

  public static int compare(ByteBuffer buf1, ByteBuffer buf2) {
    if (buf1 == null && buf2 == null) return 0;
    if (buf1 == null) return -1;
    if (buf2 == null) return 1;
    return _compare(buf1.array(), buf1.arrayOffset() + buf1.position(), buf1.remaining(),
                    buf2.array(), buf2.arrayOffset() + buf2.position(), buf2.remaining());
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
