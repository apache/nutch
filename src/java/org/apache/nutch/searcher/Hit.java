/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.apache.nutch.io.Writable;
import org.apache.nutch.io.UTF8;

import java.util.logging.Logger;
import org.apache.nutch.util.LogFormatter;

/** A document which matched a query in an index. */
public class Hit implements Writable, Comparable {
  private static final Logger LOG =
    LogFormatter.getLogger("org.apache.nutch.searcher.Hit");

  private int indexNo;                            // index id
  private int indexDocNo;                         // index-relative id
  private float score;                            // its score for the query
  private String site;                            // its website name
  private boolean moreFromSiteExcluded;

  public Hit() {}

  public Hit(int indexNo, int indexDocNo, float score, String site) {
    this(indexDocNo, score, site);
    this.indexNo = indexNo;
  }
  public Hit(int indexDocNo, float score, String site) {
    this.indexDocNo = indexDocNo;
    this.score = score;
    // 20041006, xing
    // The following fixes a bug that causes cached.jsp, text.jsp, etc.,
    // to fail in distributed search. "Release 0.6, note 14" in CHANGES.txt
    if (site == null)
      site = "";
    this.site = site;
  }

  /** Return the index number that this hit came from. */
  public int getIndexNo() { return indexNo; }
  public void setIndexNo(int indexNo) { this.indexNo = indexNo; }

  /** Return the document number of this hit within an index. */
  public int getIndexDocNo() { return indexDocNo; }

  /** Return the degree to which this document matched the query. */
  public float getScore() { return score; }

  /** Return the name of this this document's website. */
  public String getSite() { return site; }

  /** True iff other, lower-scoring, hits from the same site have been excluded
   * from the list which contains this hit.. */
  public boolean moreFromSiteExcluded() { return moreFromSiteExcluded; }

  /** True iff other, lower-scoring, hits from the same site have been excluded
   * from the list which contains this hit.. */
  public void setMoreFromSiteExcluded(boolean more){moreFromSiteExcluded=more;}

  public void write(DataOutput out) throws IOException {
    out.writeInt(indexDocNo);
    out.writeFloat(score);
    UTF8.writeString(out, site);
  }

  public void readFields(DataInput in) throws IOException {
    indexDocNo = in.readInt();
    score = in.readFloat();
    site = UTF8.readString(in);
  }

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
    if (other.score > this.score) {               // prefer higher scores
      return 1;
    } else if (other.score < this.score) {
      return -1;
    } else if (other.indexNo != this.indexNo) {
      return other.indexNo - this.indexNo;        // prefer later indexes
    } else {
      return other.indexDocNo - this.indexDocNo;  // prefer later docs
    }
  }
}
