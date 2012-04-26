/*******************************************************************************
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
 ******************************************************************************/
package org.apache.nutch.util;

import java.util.*;

public class Histogram<E> {

  private Map<E, HistogramEntry> map = new HashMap<E, HistogramEntry>();
  private float totalValue = 0;
  private int totalCount = 0;

  public void add(E x) {
    add(x, 1);
  }

  public void add(E x, float value) {
    HistogramEntry entry;
    if (map.containsKey(x)) {
      entry = map.get(x);
      entry.value += value;
      entry.count++;
    } else {
      entry = new HistogramEntry();
      entry.value = value;
      entry.count = 1;
      map.put(x, entry);
    }
    totalValue += value;
    totalCount += 1;
  }

  public Set<E> getKeys() {
    return map.keySet();
  }

  public float getValue(E x) {
    return map.get(x).value;
  }

  public int getCount(E x) {
    return map.get(x).count;
  }

  public void add(Histogram<E> other) {
    for (E x : other.getKeys()) {
      add(x, other.getValue(x));
    }
  }

  public Histogram<E> normalize() {
    Histogram<E> normalized = new Histogram<E>();
    Set<E> keys = getKeys();
    for (E x : keys) {
      normalized.add(x, getValue(x) / totalValue);
    }
    return normalized;
  }

  public List<E> sortInverseByValue() {
    List<Map.Entry<E, HistogramEntry>> list = 
        new Vector<Map.Entry<E, HistogramEntry>>(map.entrySet());

    // Sort the list using an annonymous inner class implementing Comparator for
    // the compare method
    java.util.Collections.sort(list,
        new Comparator<Map.Entry<E, HistogramEntry>>() {
          public int compare(Map.Entry<E, HistogramEntry> entry,
              Map.Entry<E, HistogramEntry> entry1) {
            return (entry.getValue().equals(entry1.getValue()) ? 0 : (entry
                .getValue().value < entry1.getValue().value ? 1 : -1));
          }
        });
    List<E> list2 = new Vector<E>();
    for (Map.Entry<E, HistogramEntry> entry : list) {
      list2.add(entry.getKey());
    }
    return list2;
  }

  public List<E> sortByValue() {
    List<Map.Entry<E, HistogramEntry>> list = 
        new Vector<Map.Entry<E, HistogramEntry>>(map.entrySet());

    // Sort the list using an annonymous inner class implementing Comparator for
    // the compare method
    java.util.Collections.sort(list,
        new Comparator<Map.Entry<E, HistogramEntry>>() {
          public int compare(Map.Entry<E, HistogramEntry> entry,
              Map.Entry<E, HistogramEntry> entry1) {
            return (entry.getValue().equals(entry1.getValue()) ? 0 : (entry
                .getValue().value > entry1.getValue().value ? 1 : -1));
          }
        });
    List<E> list2 = new Vector<E>();
    for (Map.Entry<E, HistogramEntry> entry : list) {
      list2.add(entry.getKey());
    }
    return list2;
  }

  public String toString(List<E> items) {
    StringBuilder strBuilder = new StringBuilder();
    for (E item : items) {
      strBuilder.append(map.get(item).value).append(",").append(item)
          .append("\n");
    }
    return strBuilder.toString();
  }

  class HistogramEntry {
    float value;
    int count;
  }
}
