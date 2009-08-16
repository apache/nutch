package org.apache.nutch.fetcher;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.Writable;

public class FetchEntry implements Writable {

  private ImmutableBytesWritable key;
  private Result row;
  
  public FetchEntry() {
    key = new ImmutableBytesWritable();
    row = new Result();
  }
  
  public FetchEntry(FetchEntry fe) {
    this.key = new ImmutableBytesWritable(fe.key.get().clone());
  }
  
  public FetchEntry(ImmutableBytesWritable key, Result row) {
    this.key = key;
    this.row = row;
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    key.readFields(in);
    row.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    key.write(out);
    row.write(out);
  }

  public ImmutableBytesWritable getKey() {
    return key;
  }

  public Result getRow() {
    return row;
  }
}
