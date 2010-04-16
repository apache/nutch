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
package org.apache.nutch.searcher;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.nutch.metadata.Metadata;

/**
 * Query context object that describes the context of the query.
 */
public class QueryParams implements Writable {

  public static final String DEFAULT_DEDUP_FIELD = "site";
  public static final int DEFAULT_MAX_HITS_PER_DUP = 2;
  public static final int DEFAULT_NUM_HITS = 10;
  public static final boolean DEFAULT_REVERSE = false;

  private Metadata metadata = new Metadata();

  public void setNumHits(int numHits) {
    this.numHits = numHits;
  }

  public void setMaxHitsPerDup(int maxHitsPerDup) {
    this.maxHitsPerDup = maxHitsPerDup;
  }

  public void setDedupField(String dedupField) {
    this.dedupField = dedupField;
  }

  public void setSortField(String sortField) {
    this.sortField = sortField;
  }

  public void setReverse(boolean reverse) {
    this.reverse = reverse;
  }

  private int numHits;
  private int maxHitsPerDup;
  private String dedupField;
  private String sortField;
  private boolean reverse;

  public QueryParams() {
    setNumHits(DEFAULT_NUM_HITS);
    setMaxHitsPerDup(DEFAULT_MAX_HITS_PER_DUP);
    setDedupField(DEFAULT_DEDUP_FIELD);
    setSortField(sortField);
    setReverse(false);

  }

  public QueryParams(int numHits, int maxHitsPerDup, String dedupField,
      String sortField, boolean reverse) {
    initFrom(numHits, maxHitsPerDup, dedupField, sortField, reverse);
  }

  public void initFrom(int numHits, int maxHitsPerDup, String dedupField,
      String sortField, boolean reverse) {
    setNumHits(numHits);
    setMaxHitsPerDup(maxHitsPerDup);
    setDedupField(dedupField);
    setSortField(sortField);
    setReverse(reverse);
  }

  public int getNumHits() {
    return numHits;
  }

  public int getMaxHitsPerDup() {
    return maxHitsPerDup;
  }

  public String getDedupField() {
    return dedupField;
  }

  public String getSortField() {
    return sortField;
  }

  public boolean isReverse() {
    return reverse;
  }

  public String get(String name) {
    return metadata.get(name);
  }

  public void put(String name, String value) {
    metadata.set(name, value);
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    metadata.readFields(input);
    numHits = WritableUtils.readVInt(input);
    maxHitsPerDup = WritableUtils.readVInt(input);
    dedupField = WritableUtils.readString(input);
    sortField = WritableUtils.readString(input);
    reverse = input.readBoolean();
  }

  @Override
  public void write(DataOutput output) throws IOException {
    metadata.write(output);
    WritableUtils.writeVInt(output, numHits);
    WritableUtils.writeVInt(output, maxHitsPerDup);
    WritableUtils.writeString(output, dedupField);
    WritableUtils.writeString(output, sortField);
    output.writeBoolean(reverse);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof QueryParams) {

      QueryParams other = (QueryParams) obj;
      return other.numHits == this.numHits
          && other.metadata.equals(this.metadata)
          && other.reverse == this.reverse
          && other.maxHitsPerDup == this.maxHitsPerDup
          && ((other.dedupField != null && other.dedupField
              .equals(this.dedupField)) || (other.dedupField == null && this.dedupField == null))
          && ((other.sortField != null && other.sortField
              .equals(this.sortField)) || (other.sortField == null && this.sortField == null));

    } else {
      return false;
    }
  }
}
