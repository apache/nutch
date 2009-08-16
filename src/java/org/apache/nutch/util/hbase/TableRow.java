package org.apache.nutch.util.hbase;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

class TableRow implements Writable {
  
  private static Comparator<Long> TS_COMPARATOR = new Comparator<Long>() {
    public int compare(Long l1, Long l2) {
      return l2.compareTo(l1);
    }
  };
  
  protected static byte[] EMPTY = new byte[0];
  
  protected byte[] row;
  
  protected long now;
  
  // used for family:qualifier columns
  protected final NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, ColumnData>>> map =
    new TreeMap<byte[], NavigableMap<byte[], NavigableMap<Long, ColumnData>>>(Bytes.BYTES_COMPARATOR);

  public TableRow() { // do not use!
    now = System.currentTimeMillis();
  }
  
  public TableRow(byte[] row) {
    this();
    this.row = row;
  }

  public TableRow(Result result) {
    this();
    row = result.getRow();
    for (Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> e1 : result.getMap().entrySet()) {
      NavigableMap<byte[], NavigableMap<Long, ColumnData>> familyMap = new TreeMap<byte[], NavigableMap<Long,ColumnData>>(Bytes.BYTES_COMPARATOR);
      for (Entry<byte[], NavigableMap<Long, byte[]>> e2 : e1.getValue().entrySet()) {
        NavigableMap<Long, ColumnData> qualMap = new TreeMap<Long, ColumnData>(TS_COMPARATOR);
        for (Entry<Long, byte[]> e3 : e2.getValue().entrySet()) {
          qualMap.put(e3.getKey(), new ColumnData(e3.getValue()));
        }
        familyMap.put(e2.getKey(), qualMap);
      }
      map.put(e1.getKey(), familyMap);
    }
  }
  
  public boolean hasColumn(byte[] family, byte[] qualifier) {
    if (qualifier == null) { qualifier = EMPTY; }
    NavigableMap<byte[], NavigableMap<Long, ColumnData>> qualMap = map.get(family);
    if (qualMap != null) {
      NavigableMap<Long, ColumnData> versionMap = qualMap.get(qualifier);
      if (versionMap != null) {
        return versionMap.firstEntry().getValue().getData() != null;
      }
    }
    return false;
  }

  public void delete(byte[] family, byte[] qualifier) {
    delete(family, qualifier, HConstants.LATEST_TIMESTAMP);
  }

  public void delete(byte[] family, byte[] qualifier, long ts) {
    if (qualifier == null) { qualifier = EMPTY; }
    NavigableMap<byte[], NavigableMap<Long, ColumnData>> familyMap = map.get(family);
    if (familyMap != null) {
      NavigableMap<Long, ColumnData> versionMap = familyMap.get(qualifier);
      if (versionMap != null) {
        Entry<Long, ColumnData> e = versionMap.ceilingEntry(ts);
        if (e != null) {
          e.getValue().set(null);
        } else {
          // TODO: if family:qualifier doesn't exist, what to do?
          // we add a delete for now
          versionMap.put(now, new ColumnData(null, true));
        }
      }
    }
  }
  
  public void deleteFamily(byte[] family) {
    NavigableMap<byte[], NavigableMap<Long, ColumnData>> familyMap = map.get(family);
    if (familyMap == null) {
      return;
    }
    
    for (Entry<byte[], NavigableMap<Long, ColumnData>> e: familyMap.entrySet()) {
      ColumnData latestColumnData = e.getValue().firstEntry().getValue();
      latestColumnData.set(null);
    }

  }
  
  public byte[] get(byte[] family, byte[] qualifier) {
    return get(family, qualifier, HConstants.LATEST_TIMESTAMP);
  }

  public byte[] get(byte[] family, byte[] qualifier, long ts) {
    if (qualifier == null) { qualifier = EMPTY; }
    NavigableMap<byte[], NavigableMap<Long, ColumnData>> familyMap = map.get(family);
    if (familyMap != null) {
      NavigableMap<Long, ColumnData> versionMap = familyMap.get(qualifier);
      if (versionMap != null) {
        Entry<Long, ColumnData> e = versionMap.ceilingEntry(ts);
        if (e != null) {
          return e.getValue().getData();
        }
      }
    }
    return null;
  }
  
  public void put(byte[] family, byte[] qualifier, byte[] value) {
    put(family, qualifier, value, now);
  }

  public void put(byte[] family, byte[] qualifier, byte[] value, long ts) {
    if (qualifier == null) { qualifier = EMPTY; }
    NavigableMap<byte[], NavigableMap<Long, ColumnData>> familyMap = map.get(family);
    if (familyMap == null) {
      familyMap = new TreeMap<byte[], NavigableMap<Long, ColumnData>>(Bytes.BYTES_COMPARATOR);
      map.put(family, familyMap);
    }
    NavigableMap<Long, ColumnData> versionMap = familyMap.get(qualifier);
    if (versionMap == null) {
      versionMap = new TreeMap<Long, ColumnData>(TS_COMPARATOR);
      familyMap.put(qualifier, versionMap);        
    }
    versionMap.put(ts, new ColumnData(value, true));
  }
  
  public void put(byte[] family, byte[] qualifier, float value) {
    put(family, qualifier, Bytes.toBytes(value));
  }
  
  public void put(byte[] family, byte[] qualifier, int value) {
    put(family, qualifier, Bytes.toBytes(value));
  }
  
  public void put(byte[] family, byte[] qualifier, long value) {
    put(family, qualifier, Bytes.toBytes(value));
  }
  
  public void put(byte[] family, byte[] qualifier, String value) {
    put(family, qualifier, Bytes.toBytes(value));
  }

  public RowMutation makeRowMutation() {
    return makeRowMutation(row);
  }
  
  private void addEntry(byte[] family, byte[] qualifier, long ts, byte[] value,
      RowMutation mut) {
    if (value == null) {
      mut.delete(family, qualifier, ts);
    } else {
      mut.add(family, qualifier, ts, value);
    }
  }

  public RowMutation makeRowMutation(byte[] row) {
    RowMutation mut = new RowMutation(row);
    for (Entry<byte[], NavigableMap<byte[], NavigableMap<Long, ColumnData>>> e1 : map.entrySet()) {
      for (Entry<byte[], NavigableMap<Long, ColumnData>> e2 : e1.getValue().entrySet()) {
        for (Entry<Long, ColumnData> e3 : e2.getValue().entrySet()) {
          ColumnData rawDatum = e3.getValue();
          if (rawDatum.isModified()) {
            addEntry(e1.getKey(), e2.getKey(), e3.getKey(), rawDatum.getData(), mut);
          }
        }
      }
    }

    return mut;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    now = in.readLong();
    row = Bytes.readByteArray(in);
    
    map.clear();
    int mapSize = in.readInt();
    for (int i = 0; i < mapSize; i++) {
      byte[] family = Bytes.readByteArray(in);
      int familyMapSize = in.readInt();
      TreeMap<byte[], NavigableMap<Long, ColumnData>> familyMap =
        new TreeMap<byte[], NavigableMap<Long,ColumnData>>(Bytes.BYTES_COMPARATOR);
      for (int j = 0; j < familyMapSize; j++) {
        byte[] qual = Bytes.readByteArray(in);
        int versionMapSize = in.readInt();
        TreeMap<Long, ColumnData> versionMap = new TreeMap<Long, ColumnData>(TS_COMPARATOR);
        for (int k = 0; k < versionMapSize; k++) {
          long ts = in.readLong();
          ColumnData columnData = new ColumnData();
          columnData.readFields(in);
          versionMap.put(ts, columnData);
        }
        familyMap.put(qual, versionMap);
      }
      map.put(family, familyMap);
    }
    
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(now);
    Bytes.writeByteArray(out, row);
    
    out.writeInt(map.size());
    for (Entry<byte[], NavigableMap<byte[], NavigableMap<Long, ColumnData>>> e : map.entrySet()) {
      Bytes.writeByteArray(out, e.getKey());
      NavigableMap<byte[], NavigableMap<Long, ColumnData>> familyMap = e.getValue();
      out.writeInt(familyMap.size());
      for (Entry<byte[], NavigableMap<Long, ColumnData>> e2 : familyMap.entrySet()) {
        Bytes.writeByteArray(out, e2.getKey());
        NavigableMap<Long, ColumnData> versionMap = e2.getValue();
        out.writeInt(versionMap.size());
        for (Entry<Long, ColumnData> e3 : versionMap.entrySet()) {
          out.writeLong(e3.getKey());
          e3.getValue().write(out);
        }
      }
    }
  }

}
