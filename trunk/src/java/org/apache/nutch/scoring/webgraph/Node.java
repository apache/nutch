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
package org.apache.nutch.scoring.webgraph;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.nutch.metadata.Metadata;

/**
 * A class which holds the number of inlinks and outlinks for a given url along
 * with an inlink score from a link analysis program and any metadata.  
 * 
 * The Node is the core unit of the NodeDb in the WebGraph.
 */
public class Node
  implements Writable {

  private int numInlinks = 0;
  private int numOutlinks = 0;
  private float inlinkScore = 1.0f;
  private Metadata metadata = new Metadata();

  public Node() {

  }

  public int getNumInlinks() {
    return numInlinks;
  }

  public void setNumInlinks(int numInlinks) {
    this.numInlinks = numInlinks;
  }

  public int getNumOutlinks() {
    return numOutlinks;
  }

  public void setNumOutlinks(int numOutlinks) {
    this.numOutlinks = numOutlinks;
  }

  public float getInlinkScore() {
    return inlinkScore;
  }

  public void setInlinkScore(float inlinkScore) {
    this.inlinkScore = inlinkScore;
  }

  public float getOutlinkScore() {
    return (numOutlinks > 0) ? inlinkScore / numOutlinks : inlinkScore;
  }

  public Metadata getMetadata() {
    return metadata;
  }

  public void setMetadata(Metadata metadata) {
    this.metadata = metadata;
  }

  public void readFields(DataInput in)
    throws IOException {

    numInlinks = in.readInt();
    numOutlinks = in.readInt();
    inlinkScore = in.readFloat();
    metadata.clear();
    metadata.readFields(in);
  }

  public void write(DataOutput out)
    throws IOException {

    out.writeInt(numInlinks);
    out.writeInt(numOutlinks);
    out.writeFloat(inlinkScore);
    metadata.write(out);
  }

  public String toString() {
    return "num inlinks: " + numInlinks + ", num outlinks: " + numOutlinks
      + ", inlink score: " + inlinkScore + ", outlink score: "
      + getOutlinkScore() + ", metadata: " + metadata.toString();
  }

}
