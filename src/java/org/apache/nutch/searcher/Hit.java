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

package org.apache.nutch.searcher;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/** A document which matched a query in an index. */
@SuppressWarnings("unchecked")
public class Hit implements Writable, Comparable<Hit> {

  private int indexNo;                            // index id
  private String uniqueKey;
  private WritableComparable sortValue;           // value sorted on
  private String dedupValue;                      // value to dedup on
  private boolean moreFromDupExcluded;

  public Hit() {}

  public Hit(int indexNo, String uniqueKey) {
    this(indexNo, uniqueKey, null, null);
  }
  public Hit(int indexNo, String uniqueKey,
      WritableComparable sortValue,
             String dedupValue) {
    this(uniqueKey, sortValue, dedupValue);
    this.indexNo = indexNo;
  }
  public Hit(String uniqueKey, WritableComparable sortValue, String dedupValue) {
    this.uniqueKey = uniqueKey;
    this.sortValue = sortValue;
    this.dedupValue = dedupValue == null ? "" : dedupValue;
  }

  /** Return the index number that this hit came from. */
  public int getIndexNo() { return indexNo; }
  public void setIndexNo(int indexNo) { this.indexNo = indexNo; }

  /** Return the unique identifier of this hit within an index. */
  public String getUniqueKey() { return uniqueKey; }

  /** Return the value of the field that hits are sorted on. */
  public WritableComparable getSortValue() { return sortValue; }

  /** Return the value of the field that hits should be deduplicated on. */
  public String getDedupValue() { return dedupValue; }

  /** True if other, lower-scoring, hits with the same dedup value have been
   * excluded from the list which contains this hit.. */
  public boolean moreFromDupExcluded() { return moreFromDupExcluded; }

  /** True if other, lower-scoring, hits with the same dedup value have been
   * excluded from the list which contains this hit.. */
  public void setMoreFromDupExcluded(boolean more){moreFromDupExcluded=more;}

  /** Display as a string. */
  public String toString() {
    return "#" + uniqueKey;
  }

  public int compareTo(Hit other) {
    int compare = sortValue.compareTo(other.sortValue);
    if (compare != 0) {
      return compare;                             // use sortValue
    } else if (other.indexNo != this.indexNo) {
      return other.indexNo - this.indexNo;        // prefer later indexes
    } else {
      return other.uniqueKey.compareTo(this.uniqueKey);  // prefer later doc
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result
        + ((dedupValue == null) ? 0 : dedupValue.hashCode());
    result = prime * result + indexNo;
    result = prime * result + (moreFromDupExcluded ? 1231 : 1237);
    result = prime * result + ((sortValue == null) ? 0 : sortValue.hashCode());
    result = prime * result + uniqueKey.hashCode();
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    Hit other = (Hit) obj;
    if (!uniqueKey.equals(other.uniqueKey))
      return false;
    if (dedupValue == null) {
      if (other.dedupValue != null)
        return false;
    } else if (!dedupValue.equals(other.dedupValue))
      return false;
    if (indexNo != other.indexNo)
      return false;
    if (moreFromDupExcluded != other.moreFromDupExcluded)
      return false;
    if (sortValue == null) {
      if (other.sortValue != null)
        return false;
    } else if (!sortValue.equals(other.sortValue))
      return false;
    return true;
  }


  public void write(DataOutput out) throws IOException {
    Text.writeString(out, uniqueKey);
  }

  public void readFields(DataInput in) throws IOException {
    uniqueKey = Text.readString(in);
  }

}
