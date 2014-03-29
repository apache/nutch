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
package org.apache.nutch.metadata;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Properties;

import org.junit.Assert;
import org.junit.Test;

/**
 * JUnit based tests of class
 * {@link org.apache.nutch.metadata.SpellCheckedMetadata}.
 *
 * @author Chris Mattmann
 * @author J&eacute;r&ocirc;me Charron
 */
public class TestSpellCheckedMetadata {

  private static final int NUM_ITERATIONS = 10000;

  /** Test for the <code>getNormalizedName(String)</code> method. */
  @Test
  public void testGetNormalizedName() {
    Assert.assertEquals("Content-Type", SpellCheckedMetadata
        .getNormalizedName("Content-Type"));
    Assert.assertEquals("Content-Type", SpellCheckedMetadata
        .getNormalizedName("ContentType"));
    Assert.assertEquals("Content-Type", SpellCheckedMetadata
        .getNormalizedName("Content-type"));
    Assert.assertEquals("Content-Type", SpellCheckedMetadata
        .getNormalizedName("contenttype"));
    Assert.assertEquals("Content-Type", SpellCheckedMetadata
        .getNormalizedName("contentype"));
    Assert.assertEquals("Content-Type", SpellCheckedMetadata
        .getNormalizedName("contntype"));
  }
  
  /** Test for the <code>add(String, String)</code> method. */
  @Test
  public void testAdd() {
    String[] values = null;
    SpellCheckedMetadata meta = new SpellCheckedMetadata();

    values = meta.getValues("contentype");
    Assert.assertEquals(0, values.length);

    meta.add("contentype", "value1");
    values = meta.getValues("contentype");
    Assert.assertEquals(1, values.length);
    Assert.assertEquals("value1", values[0]);

    meta.add("Content-Type", "value2");
    values = meta.getValues("contentype");
    Assert.assertEquals(2, values.length);
    Assert.assertEquals("value1", values[0]);
    Assert.assertEquals("value2", values[1]);

    // NOTE : For now, the same value can be added many times.
    // Should it be changed?
    meta.add("ContentType", "value1");
    values = meta.getValues("Content-Type");
    Assert.assertEquals(3, values.length);
    Assert.assertEquals("value1", values[0]);
    Assert.assertEquals("value2", values[1]);
    Assert.assertEquals("value1", values[2]);
  }

  /** Test for the <code>set(String, String)</code> method. */
  @Test
  public void testSet() {
    String[] values = null;
    SpellCheckedMetadata meta = new SpellCheckedMetadata();

    values = meta.getValues("contentype");
    Assert.assertEquals(0, values.length);

    meta.set("contentype", "value1");
    values = meta.getValues("contentype");
    Assert.assertEquals(1, values.length);
    Assert.assertEquals("value1", values[0]);

    meta.set("Content-Type", "value2");
    values = meta.getValues("contentype");
    Assert.assertEquals(1, values.length);
    Assert.assertEquals("value2", values[0]);

    meta.set("contenttype", "new value 1");
    meta.add("contenttype", "new value 2");
    values = meta.getValues("contentype");
    Assert.assertEquals(2, values.length);
    Assert.assertEquals("new value 1", values[0]);
    Assert.assertEquals("new value 2", values[1]);
  }

  /** Test for <code>setAll(Properties)</code> method. */
  @Test
  public void testSetProperties() {
    String[] values = null;
    SpellCheckedMetadata meta = new SpellCheckedMetadata();
    Properties props = new Properties();

    meta.setAll(props);
    Assert.assertEquals(0, meta.size());

    props.setProperty("name-one", "value1.1");
    meta.setAll(props);
    Assert.assertEquals(1, meta.size());
    values = meta.getValues("name-one");
    Assert.assertEquals(1, values.length);
    Assert.assertEquals("value1.1", values[0]);

    props.setProperty("name-two", "value2.1");
    meta.setAll(props);
    Assert.assertEquals(2, meta.size());
    values = meta.getValues("name-one");
    Assert.assertEquals(1, values.length);
    Assert.assertEquals("value1.1", values[0]);
    values = meta.getValues("name-two");
    Assert.assertEquals(1, values.length);
    Assert.assertEquals("value2.1", values[0]);
  }

  /** Test for <code>get(String)</code> method. */
  @Test
  public void testGet() {
    SpellCheckedMetadata meta = new SpellCheckedMetadata();
    Assert.assertNull(meta.get("a-name"));

    meta.add("a-name", "value-1");
    Assert.assertEquals("value-1", meta.get("a-name"));
    meta.add("a-name", "value-2");
    Assert.assertEquals("value-1", meta.get("a-name"));
  }

  /** Test for <code>isMultiValued()</code> method. */
  @Test
  public void testIsMultiValued() {
    SpellCheckedMetadata meta = new SpellCheckedMetadata();
    Assert.assertFalse(meta.isMultiValued("key"));
    meta.add("key", "value1");
    Assert.assertFalse(meta.isMultiValued("key"));
    meta.add("key", "value2");
    Assert.assertTrue(meta.isMultiValued("key"));
  }

  /** Test for <code>names</code> method. */
  @Test
  public void testNames() {
    String[] names = null;
    SpellCheckedMetadata meta = new SpellCheckedMetadata();
    names = meta.names();
    Assert.assertEquals(0, names.length);

    meta.add("name-one", "value");
    names = meta.names();
    Assert.assertEquals(1, names.length);
    Assert.assertEquals("name-one", names[0]);
    meta.add("name-two", "value");
    names = meta.names();
    Assert.assertEquals(2, names.length);
  }

  /** Test for <code>remove(String)</code> method. */
  @Test
  public void testRemove() {
    SpellCheckedMetadata meta = new SpellCheckedMetadata();
    meta.remove("name-one");
    Assert.assertEquals(0, meta.size());
    meta.add("name-one", "value-1.1");
    meta.add("name-one", "value-1.2");
    meta.add("name-two", "value-2.2");
    Assert.assertEquals(2, meta.size());
    Assert.assertNotNull(meta.get("name-one"));
    Assert.assertNotNull(meta.get("name-two"));
    meta.remove("name-one");
    Assert.assertEquals(1, meta.size());
    Assert.assertNull(meta.get("name-one"));
    Assert.assertNotNull(meta.get("name-two"));
    meta.remove("name-two");
    Assert.assertEquals(0, meta.size());
    Assert.assertNull(meta.get("name-one"));
    Assert.assertNull(meta.get("name-two"));
  }

  /** Test for <code>equals(Object)</code> method. */
  @Test
  public void testObject() {
    SpellCheckedMetadata meta1 = new SpellCheckedMetadata();
    SpellCheckedMetadata meta2 = new SpellCheckedMetadata();
    Assert.assertFalse(meta1.equals(null));
    Assert.assertFalse(meta1.equals("String"));
    Assert.assertTrue(meta1.equals(meta2));
    meta1.add("name-one", "value-1.1");
    Assert.assertFalse(meta1.equals(meta2));
    meta2.add("name-one", "value-1.1");
    Assert.assertTrue(meta1.equals(meta2));
    meta1.add("name-one", "value-1.2");
    Assert.assertFalse(meta1.equals(meta2));
    meta2.add("name-one", "value-1.2");
    Assert.assertTrue(meta1.equals(meta2));
    meta1.add("name-two", "value-2.1");
    Assert.assertFalse(meta1.equals(meta2));
    meta2.add("name-two", "value-2.1");
    Assert.assertTrue(meta1.equals(meta2));
    meta1.add("name-two", "value-2.2");
    Assert.assertFalse(meta1.equals(meta2));
    meta2.add("name-two", "value-2.x");
    Assert.assertFalse(meta1.equals(meta2));
  }

  /** Test for <code>Writable</code> implementation. */
  @Test
  public void testWritable() {
    SpellCheckedMetadata result = null;
    SpellCheckedMetadata meta = new SpellCheckedMetadata();
    result = writeRead(meta);
    Assert.assertEquals(0, result.size());
    meta.add("name-one", "value-1.1");
    result = writeRead(meta);
    meta.add("Contenttype", "text/html");
    Assert.assertEquals(1, result.size());
    Assert.assertEquals(1, result.getValues("name-one").length);
    Assert.assertEquals("value-1.1", result.get("name-one"));
    meta.add("name-two", "value-2.1");
    meta.add("name-two", "value-2.2");
    result = writeRead(meta);
    Assert.assertEquals(3, result.size());
    Assert.assertEquals(1, result.getValues("name-one").length);
    Assert.assertEquals("value-1.1", result.getValues("name-one")[0]);
    Assert.assertEquals(2, result.getValues("name-two").length);
    Assert.assertEquals("value-2.1", result.getValues("name-two")[0]);
    Assert.assertEquals("value-2.2", result.getValues("name-two")[1]);
    Assert.assertEquals("text/html", result.get(Metadata.CONTENT_TYPE));
  }

  /**
   * IO Test method, usable only when you plan to do changes in metadata
   * to measure relative performance impact.
   */
  @Test
  public final void testHandlingSpeed() {
    @SuppressWarnings("unused")
    SpellCheckedMetadata result;
    long start = System.currentTimeMillis();
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      SpellCheckedMetadata scmd = constructSpellCheckedMetadata();
      result = writeRead(scmd);
    }
    System.out.println(NUM_ITERATIONS + " spellchecked metadata I/O time:"
        + (System.currentTimeMillis() - start) + "ms.");
  }

  private SpellCheckedMetadata writeRead(SpellCheckedMetadata meta) {
    SpellCheckedMetadata readed = new SpellCheckedMetadata();
    try {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      meta.write(new DataOutputStream(out));
      readed.readFields(new DataInputStream(new ByteArrayInputStream(out
          .toByteArray())));
    } catch (IOException ioe) {
      Assert.fail(ioe.toString());
    }
    return readed;
  }

  /**
   * Assembles a Spellchecked metadata Object.
   */
  public static final SpellCheckedMetadata constructSpellCheckedMetadata() {
    SpellCheckedMetadata scmd = new SpellCheckedMetadata();
    scmd.add("Content-type", "foo/bar");
    scmd.add("Connection", "close");
    scmd.add("Last-Modified", "Sat, 09 Dec 2006 15:09:57 GMT");
    scmd.add("Server", "Foobar");
    scmd.add("Date", "Sat, 09 Dec 2006 18:07:20 GMT");
    scmd.add("Accept-Ranges", "bytes");
    scmd.add("ETag", "\"1234567-89-01234567\"");
    scmd.add("Content-Length", "123");
    scmd.add(Nutch.SEGMENT_NAME_KEY, "segmentzzz");
    scmd.add(Nutch.SIGNATURE_KEY, "123");
    return scmd;
  }

}
