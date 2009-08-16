package org.apache.nutch.util.hbase;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;

public class RowMutation implements Writable {
  
  private static class RowOperation implements Writable {
    private byte[] family, qualifier, value;
    private long ts;
    
    RowOperation() { }
    
    public RowOperation(byte[] family, byte[] qualifier, byte[] value) {
      this(family, qualifier, value, HConstants.LATEST_TIMESTAMP);
    }
    
    public RowOperation(byte[] family, byte[] qualifier, byte[] value, long ts) {
      this.family = family;
      this.qualifier = qualifier;
      this.value = value;
      this.ts = ts;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      family = Bytes.readByteArray(in);
      qualifier = Bytes.readByteArray(in);
      value = Bytes.readByteArray(in);
      ts = in.readLong();
    }

    @Override
    public void write(DataOutput out) throws IOException {
      Bytes.writeByteArray(out, family);
      Bytes.writeByteArray(out, qualifier);
      Bytes.writeByteArray(out, value);
      out.writeLong(ts);
    }
  }
  
  private byte[] row;
  
  private List<RowOperation> ops = new ArrayList<RowOperation>();
  
  public RowMutation() { }
  
  public RowMutation(byte[] row) {
    this.row = row;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    row = Bytes.readByteArray(in);
    int size = in.readInt();
    for (int i = 0; i < size; i++) {
      RowOperation rowOp = new RowOperation();
      rowOp.readFields(in);
      ops.add(rowOp);
    }
  }
  
  public void add(byte[] family, byte[] qualifier, byte[] value) {
    ops.add(new RowOperation(family, qualifier, value));
  }
  
  public void delete(byte[] family, byte[] qualifier) {
    ops.add(new RowOperation(family, qualifier, null));
  }
  
  public void add(byte[] family, byte[] qualifier, long ts, byte[] value) {
    ops.add(new RowOperation(family, qualifier, value, ts));
  }
  
  public void delete(byte[] family, byte[] qualifier, long ts) {
    ops.add(new RowOperation(family, qualifier, null, ts));
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Bytes.writeByteArray(out, row);
    for (RowOperation rowOp : ops) {
      rowOp.write(out);
    }
  }

  public void commit(HTable table) throws IOException {
    Put put = new Put(row);
    Delete delete = new Delete(row); // deletes entire row, so be careful
    boolean hasPuts = false;
    boolean hasDeletes = false;
    
    for (RowOperation rowOp : ops) {
      byte[] value = rowOp.value;
      if (value == null) {
        hasDeletes = true;
        delete.deleteColumn(rowOp.family, rowOp.qualifier, rowOp.ts);
      } else {
        hasPuts = true;
        put.add(rowOp.family, rowOp.qualifier, rowOp.ts, rowOp.value);
      }
    }
    if (hasPuts) {
      table.put(put);
    }
    if (hasDeletes) {
      table.delete(delete);
    }
  }

  public <K2 extends WritableComparable<? super K2>> 
  void writeToContext(K2 key, Reducer<?, ?, K2, Writable>.Context context)
  throws IOException, InterruptedException {
    Put put = new Put(row);
    Delete delete = new Delete(row); // deletes entire row, be careful
    boolean hasPuts = false;
    boolean hasDeletes = false;
    
    for (RowOperation rowOp : ops) {
      byte[] value = rowOp.value;
      if (value == null) {
        hasDeletes = true;
        delete.deleteColumn(rowOp.family, rowOp.qualifier);
      } else {
        hasPuts = true;
        put.add(rowOp.family, rowOp.qualifier, rowOp.value);
      }
    }
    if (hasPuts) {
      context.write(key, put);
    }
    if (hasDeletes) {
      context.write(key, delete);
    }
  }

}
