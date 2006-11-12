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

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/** A document which matched a query in an index. */
public class Hit implements Writable, Comparable {

  private int indexNo;                            // index id
  private int indexDocNo;                         // index-relative id
  private WritableComparable sortValue;           // value sorted on
  private String dedupValue;                      // value to dedup on
  private boolean moreFromDupExcluded;

  public Hit() {}

  public Hit(int indexNo, int indexDocNo) {
    this(indexNo, indexDocNo, null, null);
  }
  public Hit(int indexNo, int indexDocNo,
             WritableComparable sortValue,
             String dedupValue) {
    this(indexDocNo, sortValue, dedupValue);
    this.indexNo = indexNo;
  }
  public Hit(int indexDocNo, WritableComparable sortValue, String dedupValue) {
    this.indexDocNo = indexDocNo;
    this.sortValue = sortValue;
    this.dedupValue = dedupValue == null ? "" : dedupValue;
  }

  /** Return the index number that this hit came from. */
  public int getIndexNo() { return indexNo; }
  public void setIndexNo(int indexNo) { this.indexNo = indexNo; }

  /** Return the document number of this hit within an index. */
  public int getIndexDocNo() { return indexDocNo; }

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
    return "#" + indexDocNo;
  }

  public boolean equals(Object o) {
    if (!(o instanceof Hit))
      return false;
    Hit other = (Hit)o;
    return this.indexNo == other.indexNo
      && this.indexDocNo == other.indexDocNo;
  }

  public int hashCode() {
    return indexNo ^ indexDocNo;
  }

  public int compareTo(Object o) {
    Hit other = (Hit)o;
    int compare = sortValue.compareTo(other.sortValue);
    if (compare != 0) {
      return compare;                             // use sortValue
    } else if (other.indexNo != this.indexNo) {
      return other.indexNo - this.indexNo;        // prefer later indexes
    } else {
      return other.indexDocNo - this.indexDocNo;  // prefer later docs
    }
  }

  public void write(DataOutput out) throws IOException {
    out.writeInt(indexDocNo);
  }

  public void readFields(DataInput in) throws IOException {
    indexDocNo = in.readInt();
  }

}
