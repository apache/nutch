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
