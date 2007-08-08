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

package org.apache.nutch.crawl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

// Commons Logging imports
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;
import org.apache.nutch.protocol.ProtocolStatus;

/**
 * A writable map, with a similar behavior as <code>java.util.HashMap</code>.
 * In addition to the size of key and value writable tuple two additional bytes
 * are stored to identify the Writable classes. This means that a maximum of
 * 255 different class types can be used for key and value objects.
 * A binary-id to class mapping is defined in a static block of this class.
 * However it is possible to use custom implementations of Writable.
 * For these custom Writables we write the byte id - utf class name tuple
 * into the header of each MapWritable that uses these types.
 *
 * @author Stefan Groschupf
 */
public class MapWritable implements Writable {

  public static final Log LOG = LogFactory.getLog(MapWritable.class);

  private KeyValueEntry fFirst;

  private KeyValueEntry fLast;

  private KeyValueEntry fOld;

  private int fSize = 0;

  private int fIdCount = 0;

  private ClassIdEntry fIdLast;

  private ClassIdEntry fIdFirst;

  private static Map<Class, Byte> CLASS_ID_MAP = new HashMap<Class, Byte>();

  private static Map<Byte, Class> ID_CLASS_MAP = new HashMap<Byte, Class>();

  static {

    addToMap(NullWritable.class, new Byte((byte) -127));
    addToMap(LongWritable.class, new Byte((byte) -126));
    addToMap(Text.class, new Byte((byte) -125));
    addToMap(MD5Hash.class, new Byte((byte) -124));
    addToMap(org.apache.nutch.fetcher.FetcherOutput.class,
        new Byte((byte) -123));
    addToMap(org.apache.nutch.protocol.Content.class, new Byte((byte) -122));
    addToMap(org.apache.nutch.parse.ParseText.class, new Byte((byte) -121));
    addToMap(org.apache.nutch.parse.ParseData.class, new Byte((byte) -120));
    addToMap(MapWritable.class, new Byte((byte) -119));
    addToMap(BytesWritable.class, new Byte((byte) -118));
    addToMap(FloatWritable.class, new Byte((byte) -117));
    addToMap(IntWritable.class, new Byte((byte) -116));
    addToMap(ObjectWritable.class, new Byte((byte) -115));
    addToMap(ProtocolStatus.class, new Byte((byte) -114));

  }

  private static void addToMap(Class clazz, Byte byteId) {
    CLASS_ID_MAP.put(clazz, byteId);
    ID_CLASS_MAP.put(byteId, clazz);
  }
  
  public MapWritable() { }
  
  /**
   * Copy constructor. This constructor makes a deep copy, using serialization /
   * deserialization to break any possible references to contained objects.
   * 
   * @param map map to copy from
   */
  public MapWritable(MapWritable map) {
    if (map != null) {
      try {
        DataOutputBuffer dob = new DataOutputBuffer();
        map.write(dob);
        DataInputBuffer dib = new DataInputBuffer();
        dib.reset(dob.getData(), dob.getLength());
        readFields(dib);
      } catch (IOException e) {
        throw new IllegalArgumentException("this map cannot be copied: " +
                StringUtils.stringifyException(e));
      }
    }
  }

  public void clear() {
    fOld = fFirst;
    fFirst = fLast = null;
    fSize = 0;
  }

  public boolean containsKey(Writable key) {
    return findEntryByKey(key) != null;
  }

  public boolean containsValue(Writable value) {
    KeyValueEntry entry = fFirst;
    while (entry != null) {
      if (entry.fValue.equals(value)) {
        return true;
      }
      entry = entry.fNextEntry;
    }
    return false;
  }

  public Writable get(Writable key) {
    KeyValueEntry entry = findEntryByKey(key);
    if (entry != null) {
      return entry.fValue;
    }
    return null;
  }

  public int hashCode() {
    final int seed = 23;
    int hash = 0;
    KeyValueEntry entry = fFirst;
    while (entry != null) {
      hash += entry.fKey.hashCode() * seed;
      hash += entry.fValue.hashCode() * seed;
      entry = entry.fNextEntry;
    }
    return hash;

  }

  public boolean isEmpty() {
    return fFirst == null;
  }

  public Set<Writable> keySet() {
    HashSet<Writable> set = new HashSet<Writable>();
    if (isEmpty()) return set;
    set.add(fFirst.fKey);
    KeyValueEntry entry = fFirst;
    while ((entry = entry.fNextEntry) != null) {
      set.add(entry.fKey);
    }
    return set;
  }

  public Writable put(Writable key, Writable value) {
    KeyValueEntry entry = findEntryByKey(key);
    if (entry != null) {
      Writable oldValue = entry.fValue;
      entry.fValue = value;
      return oldValue;
    }
    KeyValueEntry newEntry = new KeyValueEntry(key, value);
    fSize++;
    if (fLast != null) {
      fLast = fLast.fNextEntry = newEntry;
      return null;
    }
    fLast = fFirst = newEntry;
    return null;

  }

  public void putAll(MapWritable map) {
    if (map == null || map.size() == 0) {
      return;
    }
    Iterator<Writable> iterator = map.keySet().iterator();
    while (iterator.hasNext()) {
      Writable key = iterator.next();
      Writable value = map.get(key);
      put(key, value);
    }
  }

  public Writable remove(Writable key) {
    Writable oldValue = null;
    KeyValueEntry entry = fFirst;
    KeyValueEntry predecessor = null;
    while (entry != null) {
      if (entry.fKey.equals(key)) {
        oldValue = entry.fValue;
        if (predecessor == null) {
          fFirst = fFirst.fNextEntry;
        } else {
          predecessor.fNextEntry = entry.fNextEntry;
        }
        if (fLast.equals(entry)) {
          fLast = predecessor;
        }
        fSize--;
        return oldValue;
      }
      predecessor = entry;
      entry = entry.fNextEntry;
    }
    return oldValue;
  }

  public int size() {
    return fSize;
  }

  public Collection<Writable> values() {
    LinkedList<Writable> list = new LinkedList<Writable>();
    KeyValueEntry entry = fFirst;
    while (entry != null) {
      list.add(entry.fValue);
      entry = entry.fNextEntry;
    }
    return list;
  }

  public boolean equals(Object obj) {
    if (obj instanceof MapWritable) {
      MapWritable map = (MapWritable) obj;
      if (fSize != map.fSize) return false;
      HashSet<KeyValueEntry> set1 = new HashSet<KeyValueEntry>();
      KeyValueEntry e1 = fFirst;
      while (e1 != null) {
        set1.add(e1);
        e1 = e1.fNextEntry;
      }
      HashSet<KeyValueEntry> set2 = new HashSet<KeyValueEntry>();
      KeyValueEntry e2 = map.fFirst;
      while (e2 != null) {
        set2.add(e2);
        e2 = e2.fNextEntry;
      }
      return set1.equals(set2);
    }
    return false;
  }

  public String toString() {
    if (fFirst != null) {
      StringBuffer buffer = new StringBuffer();
      KeyValueEntry entry = fFirst;
      while (entry != null) {
        buffer.append(entry.toString());
        buffer.append(" ");
        entry = entry.fNextEntry;
      }
      return buffer.toString();
    }
    return null;
  }

  private KeyValueEntry findEntryByKey(final Writable key) {
    KeyValueEntry entry = fFirst;
    while (entry != null && !entry.fKey.equals(key)) {
      entry = entry.fNextEntry;
    }
    return entry;
  }

  // serialization methods

  public void write(DataOutput out) throws IOException {
    out.writeInt(size());

    if (size() > 0) {
      // scan for unknown classes;
      createInternalIdClassEntries();
      // write internal map
      out.writeByte(fIdCount);
      if (fIdCount > 0) {
        ClassIdEntry entry = fIdFirst;
        while (entry != null) {
          out.writeByte(entry.fId);
          Text.writeString(out, entry.fclazz.getName());
          entry = entry.fNextIdEntry;
        }
      }
      // write meta data
      KeyValueEntry entry = fFirst;
      while (entry != null) {
        out.writeByte(entry.fKeyClassId);
        out.writeByte(entry.fValueClassId);

        entry.fKey.write(out);
        entry.fValue.write(out);

        entry = entry.fNextEntry;
      }

    }

  }

  public void readFields(DataInput in) throws IOException {
    clear();
    fSize = in.readInt();
    if (fSize > 0) {
      // read class-id map
      fIdCount = in.readByte();
      byte id;
      Class clazz;
      for (int i = 0; i < fIdCount; i++) {
        try {
          id = in.readByte();
          clazz = Class.forName(Text.readString(in));
          addIdEntry(id, clazz);
        } catch (Exception e) {
          if (LOG.isWarnEnabled()) { 
            LOG.warn("Unable to load internal map entry" + e.toString());
          }
          fIdCount--;
        }
      }
      KeyValueEntry entry;
      for (int i = 0; i < fSize; i++) {
        try {
          entry = getKeyValueEntry(in.readByte(), in.readByte());
          entry.fKey.readFields(in);
          entry.fValue.readFields(in);
          if (fFirst == null) {
            fFirst = fLast = entry;
          } else {
            fLast = fLast.fNextEntry = entry;
          }
        } catch (IOException e) {
          if (LOG.isWarnEnabled()) {
            LOG.warn("Unable to load meta data entry, ignoring.. : "  +
                     e.toString());
          }
          fSize--;
        }
      }
    }
  }

  private void createInternalIdClassEntries() {
    KeyValueEntry entry = fFirst;
    byte id;
    while (entry != null) {
      id = getClassId(entry.fKey.getClass());
      if (id == -128) {
        id = addIdEntry((byte) (-128 + CLASS_ID_MAP.size() + ++fIdCount),
            entry.fKey.getClass());
      }
      entry.fKeyClassId = id;
      id = getClassId(entry.fValue.getClass());
      if (id == -128) {
        id = addIdEntry((byte) (-128 + CLASS_ID_MAP.size() + ++fIdCount),
            entry.fValue.getClass());
      }
      entry.fValueClassId = id;
      entry = entry.fNextEntry;
    }
  }

  private byte addIdEntry(byte id, Class clazz) {
    if (fIdFirst == null) {
      fIdFirst = fIdLast = new ClassIdEntry(id, clazz);
    } else {
      fIdLast.fNextIdEntry = fIdLast = new ClassIdEntry(id, clazz);
    }
    return id;
  }

  private byte getClassId(Class clazz) {
    Byte classId = CLASS_ID_MAP.get(clazz);
    if (classId != null) {
      return classId.byteValue();
    }
    ClassIdEntry entry = fIdFirst;
    while (entry != null) {
      if (entry.fclazz.equals(clazz)) {
        return entry.fId;
      }
      entry = entry.fNextIdEntry;
    }
    return -128;
  }

  private KeyValueEntry getKeyValueEntry(final byte keyId, final byte valueId)
      throws IOException {
    KeyValueEntry entry = fOld;
    KeyValueEntry last = null;
    byte entryKeyId;
    byte entryValueId;
    while (entry != null) {
      entryKeyId = getClassId(entry.fKey.getClass());
      entryValueId = getClassId(entry.fValue.getClass());
      if (entryKeyId == keyId && entryValueId == valueId) {
        if (last != null) {
          last.fNextEntry = entry.fNextEntry;
        } else {
          fOld = entry.fNextEntry;
        }
        entry.fNextEntry = null; // reset next entry
        return entry;
      }
      last = entry;
      entry = entry.fNextEntry;
    }
    Class keyClass = getClass(keyId);
    Class valueClass = getClass(valueId);
    try {
      return new KeyValueEntry((Writable) keyClass.newInstance(),
          (Writable) valueClass.newInstance());
    } catch (Exception e) {
      throw new IOException("unable to instantiate class: " + e.toString());
    }

  }

  private Class getClass(final byte id) throws IOException {
    Class clazz = ID_CLASS_MAP.get(new Byte(id));
    if (clazz == null) {
      ClassIdEntry entry = fIdFirst;
      while (entry != null) {
        if (entry.fId == id) {
          return entry.fclazz;
        }

        entry = entry.fNextIdEntry;
      }
    } else {
      return clazz;
    }
    throw new IOException("unable to load class for id: " + id);
  }

  /** an entry holds writable key and value */
  private class KeyValueEntry {
    private byte fKeyClassId;

    private byte fValueClassId;

    private Writable fKey;

    private Writable fValue;

    private KeyValueEntry fNextEntry;

    public KeyValueEntry(Writable key, Writable value) {
      this.fKey = key;
      this.fValue = value;
    }

    public String toString() {
      return fKey.toString() + ":" + fValue.toString();
    }

    public boolean equals(Object obj) {
      if (obj instanceof KeyValueEntry) {
        KeyValueEntry entry = (KeyValueEntry) obj;
        return entry.fKey.equals(fKey) && entry.fValue.equals(fValue);
      }
      return false;
    }

    public int hashCode() {
      return toString().hashCode();
    }
  }

  /** container for Id class tuples */
  private class ClassIdEntry {
    public ClassIdEntry(byte id, Class clazz) {
      fId = id;
      fclazz = clazz;
    }

    private byte fId;

    private Class fclazz;

    private ClassIdEntry fNextIdEntry;
  }

}
