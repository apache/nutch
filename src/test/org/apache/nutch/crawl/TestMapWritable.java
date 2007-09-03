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

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.MapWritable;
import org.apache.nutch.util.NutchConfiguration;

public class TestMapWritable extends TestCase {

  private Configuration configuration = NutchConfiguration.create();

  public void testMap() throws Exception {
    MapWritable map = new MapWritable();
    assertTrue(map.isEmpty());
    for (int i = 0; i < 100; i++) {
      Text key = new Text("" + i);
      IntWritable value = new IntWritable(i);
      map.put(key, value);
      assertEquals(i + 1, map.size());
      assertTrue(map.containsKey(new Text("" + i)));
      assertTrue(map.containsValue(new IntWritable(i)));
      map.remove(key);
      assertEquals(i, map.size());
      map.put(key, value);
      assertEquals(value, map.get(key));
      assertFalse(map.isEmpty());
      assertTrue(map.keySet().contains(key));
      assertEquals(i + 1, map.values().size());
      assertTrue(map.values().contains(value));
    }
    testWritable(map);
    MapWritable map2 = new MapWritable();
    testWritable(map2);
    map2.putAll(map);
    assertEquals(100, map2.size());
    testWritable(map2);

    map.clear();
    assertTrue(map.isEmpty());
    assertEquals(0, map.size());
    assertFalse(map.containsKey(new Text("" + 1)));

  }

  public void testWritable() throws Exception {
    MapWritable datum1 = new MapWritable();
    for (int i = 0; i < 100; i++) {
      datum1.put(new LongWritable(i), new Text("" + 1));
    }
    assertEquals(100, datum1.size());
    testWritable(datum1);

    MapWritable datum2 = new MapWritable();
    for (int i = 0; i < 100; i++) {
      datum2.put(new DummyWritable(i), new DummyWritable(i));
    }
    assertEquals(100, datum2.size());
    testWritable(datum2);

    CrawlDatum c = new CrawlDatum(CrawlDatum.STATUS_DB_FETCHED, 1);
    c.setMetaData(new MapWritable());
    for (int i = 0; i < 100; i++) {
      c.getMetaData().put(new LongWritable(i), new Text("" + 1));
    }
    testWritable(c);
  }
  
  public void testEquals() {
    MapWritable map1 = new MapWritable();
    MapWritable map2 = new MapWritable();
    map1.put(new Text("key1"), new Text("val1"));
    map1.put(new Text("key2"), new Text("val2"));
    map2.put(new Text("key2"), new Text("val2"));
    map2.put(new Text("key1"), new Text("val1"));
    assertTrue(map1.equals(map2));
  }

  public void testPerformance() throws Exception {
    FileSystem fs = FileSystem.get(configuration);
    Path file = new Path(System.getProperty("java.io.tmpdir"), "mapTestFile");
    fs.delete(file);
    org.apache.hadoop.io.SequenceFile.Writer writer = SequenceFile.createWriter(
        fs, configuration, file, IntWritable.class, MapWritable.class);
    // write map
    System.out.println("start writing map's");
    long start = System.currentTimeMillis();
    IntWritable key = new IntWritable();
    MapWritable map = new MapWritable();
    LongWritable mapValue = new LongWritable();
    for (int i = 0; i < 1000000; i++) {
      key.set(i);
      mapValue.set(i);
      map.put(key, mapValue);
      writer.append(key, map);
    }
    long needed = System.currentTimeMillis() - start;
    writer.close();
    System.out.println("needed time for writing map's: " + needed);

    // read map

    org.apache.hadoop.io.SequenceFile.Reader reader = new SequenceFile.Reader(
        fs, file, configuration);
    System.out.println("start reading map's");
    start = System.currentTimeMillis();
    while (reader.next(key, map)) {

    }
    reader.close();
    needed = System.currentTimeMillis() - start;
    System.out.println("needed time for reading map's: " + needed);
    fs.delete(file);

    // Text
    System.out.println("start writing Text's");
    writer = SequenceFile.createWriter(fs, configuration, file, IntWritable.class, Text.class);
    // write map
    start = System.currentTimeMillis();
    key = new IntWritable();
    Text value = new Text();
    String s = "15726:15726";
    for (int i = 0; i < 1000000; i++) {
      key.set(i);
      value.set(s);
      writer.append(key, value);
    }
    needed = System.currentTimeMillis() - start;
    writer.close();
    System.out.println("needed time for writing Text's: " + needed);

    // read map
    System.out.println("start reading Text's");
    reader = new SequenceFile.Reader(fs, file, configuration);
    start = System.currentTimeMillis();
    while (reader.next(key, value)) {

    }
    needed = System.currentTimeMillis() - start;
    System.out.println("needed time for reading Text: " + needed);
    fs.delete(file);
  }

  /** Utility method for testing writables, from hadoop code */
  public void testWritable(Writable before) throws Exception {
    DataOutputBuffer dob = new DataOutputBuffer();
    before.write(dob);

    DataInputBuffer dib = new DataInputBuffer();
    dib.reset(dob.getData(), dob.getLength());

    Writable after = (Writable) before.getClass().newInstance();
    after.readFields(dib);

    assertEquals(before, after);
  }

  public void testRecycling() throws Exception {
    Text value = new Text("value");
    Text key1 = new Text("a");
    Text key2 = new Text("b");

    MapWritable writable = new MapWritable();
    writable.put(key1, value);
    assertEquals(writable.get(key1), value);
    assertNull(writable.get(key2));

    DataOutputBuffer dob = new DataOutputBuffer();
    writable.write(dob);
    writable.clear();
    writable.put(key1, value);
    writable.put(key2, value);
    assertEquals(writable.get(key1), value);
    assertEquals(writable.get(key2), value);

    DataInputBuffer dib = new DataInputBuffer();
    dib.reset(dob.getData(), dob.getLength());
    writable.readFields(dib);
    assertEquals(writable.get(key1), value);
    assertNull(writable.get(key2));
  }

  public static void main(String[] args) throws Exception {
    TestMapWritable writable = new TestMapWritable();
    writable.testPerformance();
  }

}
