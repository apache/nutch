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
package org.apache.nutch.metadata;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Properties;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * JUnit based tests of class
 * {@link org.apache.nutch.metadata.SpellCheckedMetadata}.
 * 
 * @author Chris Mattmann
 * @author J&eacute;r&ocirc;me Charron
 */
@Tag("metadata")
public class TestSpellCheckedMetadata {

  private static final int NUM_ITERATIONS = 10000;

  /** Test for the <code>getNormalizedName(String)</code> method. */
  @Test
  public void testGetNormalizedName() {
    Assertions.assertEquals("Content-Type",
        SpellCheckedMetadata.getNormalizedName("Content-Type"));
    Assertions.assertEquals("Content-Type",
        SpellCheckedMetadata.getNormalizedName("ContentType"));
    Assertions.assertEquals("Content-Type",
        SpellCheckedMetadata.getNormalizedName("Content-type"));
    Assertions.assertEquals("Content-Type",
        SpellCheckedMetadata.getNormalizedName("contenttype"));
    Assertions.assertEquals("Content-Type",
        SpellCheckedMetadata.getNormalizedName("contentype"));
    Assertions.assertEquals("Content-Type",
        SpellCheckedMetadata.getNormalizedName("contntype"));
  }

  /** Test for the <code>add(String, String)</code> method. */
  @Test
  public void testAdd() {
    String[] values = null;
    SpellCheckedMetadata meta = new SpellCheckedMetadata();

    values = meta.getValues("contentype");
    Assertions.assertEquals(0, values.length);

    meta.add("contentype", "value1");
    values = meta.getValues("contentype");
    Assertions.assertEquals(1, values.length);
    Assertions.assertEquals("value1", values[0]);

    meta.add("Content-Type", "value2");
    values = meta.getValues("contentype");
    Assertions.assertEquals(2, values.length);
    Assertions.assertEquals("value1", values[0]);
    Assertions.assertEquals("value2", values[1]);

    // NOTE : For now, the same value can be added many times.
    // Should it be changed?
    meta.add("ContentType", "value1");
    values = meta.getValues("Content-Type");
    Assertions.assertEquals(3, values.length);
    Assertions.assertEquals("value1", values[0]);
    Assertions.assertEquals("value2", values[1]);
    Assertions.assertEquals("value1", values[2]);
  }

  /** Test for the <code>set(String, String)</code> method. */
  @Test
  public void testSet() {
    String[] values = null;
    SpellCheckedMetadata meta = new SpellCheckedMetadata();

    values = meta.getValues("contentype");
    Assertions.assertEquals(0, values.length);

    meta.set("contentype", "value1");
    values = meta.getValues("contentype");
    Assertions.assertEquals(1, values.length);
    Assertions.assertEquals("value1", values[0]);

    meta.set("Content-Type", "value2");
    values = meta.getValues("contentype");
    Assertions.assertEquals(1, values.length);
    Assertions.assertEquals("value2", values[0]);

    meta.set("contenttype", "new value 1");
    meta.add("contenttype", "new value 2");
    values = meta.getValues("contentype");
    Assertions.assertEquals(2, values.length);
    Assertions.assertEquals("new value 1", values[0]);
    Assertions.assertEquals("new value 2", values[1]);
  }

  /** Test for <code>setAll(Properties)</code> method. */
  @Test
  public void testSetProperties() {
    String[] values = null;
    SpellCheckedMetadata meta = new SpellCheckedMetadata();
    Properties props = new Properties();

    meta.setAll(props);
    Assertions.assertEquals(0, meta.size());

    props.setProperty("name-one", "value1.1");
    meta.setAll(props);
    Assertions.assertEquals(1, meta.size());
    values = meta.getValues("name-one");
    Assertions.assertEquals(1, values.length);
    Assertions.assertEquals("value1.1", values[0]);

    props.setProperty("name-two", "value2.1");
    meta.setAll(props);
    Assertions.assertEquals(2, meta.size());
    values = meta.getValues("name-one");
    Assertions.assertEquals(1, values.length);
    Assertions.assertEquals("value1.1", values[0]);
    values = meta.getValues("name-two");
    Assertions.assertEquals(1, values.length);
    Assertions.assertEquals("value2.1", values[0]);
  }

  /** Test for <code>get(String)</code> method. */
  @Test
  public void testGet() {
    SpellCheckedMetadata meta = new SpellCheckedMetadata();
    Assertions.assertNull(meta.get("a-name"));

    meta.add("a-name", "value-1");
    Assertions.assertEquals("value-1", meta.get("a-name"));
    meta.add("a-name", "value-2");
    Assertions.assertEquals("value-1", meta.get("a-name"));
  }

  /** Test for <code>isMultiValued()</code> method. */
  @Test
  public void testIsMultiValued() {
    SpellCheckedMetadata meta = new SpellCheckedMetadata();
    Assertions.assertFalse(meta.isMultiValued("key"));
    meta.add("key", "value1");
    Assertions.assertFalse(meta.isMultiValued("key"));
    meta.add("key", "value2");
    Assertions.assertTrue(meta.isMultiValued("key"));
  }

  /** Test for <code>names</code> method. */
  @Test
  public void testNames() {
    String[] names = null;
    SpellCheckedMetadata meta = new SpellCheckedMetadata();
    names = meta.names();
    Assertions.assertEquals(0, names.length);

    meta.add("name-one", "value");
    names = meta.names();
    Assertions.assertEquals(1, names.length);
    Assertions.assertEquals("name-one", names[0]);
    meta.add("name-two", "value");
    names = meta.names();
    Assertions.assertEquals(2, names.length);
  }

  /** Test for <code>remove(String)</code> method. */
  @Test
  public void testRemove() {
    SpellCheckedMetadata meta = new SpellCheckedMetadata();
    meta.remove("name-one");
    Assertions.assertEquals(0, meta.size());
    meta.add("name-one", "value-1.1");
    meta.add("name-one", "value-1.2");
    meta.add("name-two", "value-2.2");
    Assertions.assertEquals(2, meta.size());
    Assertions.assertNotNull(meta.get("name-one"));
    Assertions.assertNotNull(meta.get("name-two"));
    meta.remove("name-one");
    Assertions.assertEquals(1, meta.size());
    Assertions.assertNull(meta.get("name-one"));
    Assertions.assertNotNull(meta.get("name-two"));
    meta.remove("name-two");
    Assertions.assertEquals(0, meta.size());
    Assertions.assertNull(meta.get("name-one"));
    Assertions.assertNull(meta.get("name-two"));
  }

  /** Test for <code>equals(Object)</code> method. */
  @Test
  public void testObject() {
    SpellCheckedMetadata meta1 = new SpellCheckedMetadata();
    SpellCheckedMetadata meta2 = new SpellCheckedMetadata();
    Assertions.assertFalse(meta1.equals(null));
    Assertions.assertFalse(meta1.equals("String"));
    Assertions.assertTrue(meta1.equals(meta2));
    meta1.add("name-one", "value-1.1");
    Assertions.assertFalse(meta1.equals(meta2));
    meta2.add("name-one", "value-1.1");
    Assertions.assertTrue(meta1.equals(meta2));
    meta1.add("name-one", "value-1.2");
    Assertions.assertFalse(meta1.equals(meta2));
    meta2.add("name-one", "value-1.2");
    Assertions.assertTrue(meta1.equals(meta2));
    meta1.add("name-two", "value-2.1");
    Assertions.assertFalse(meta1.equals(meta2));
    meta2.add("name-two", "value-2.1");
    Assertions.assertTrue(meta1.equals(meta2));
    meta1.add("name-two", "value-2.2");
    Assertions.assertFalse(meta1.equals(meta2));
    meta2.add("name-two", "value-2.x");
    Assertions.assertFalse(meta1.equals(meta2));
  }

  /** Test for <code>Writable</code> implementation. */
  @Test
  public void testWritable() {
    SpellCheckedMetadata result = null;
    SpellCheckedMetadata meta = new SpellCheckedMetadata();
    result = writeRead(meta);
    Assertions.assertEquals(0, result.size());
    meta.add("name-one", "value-1.1");
    result = writeRead(meta);
    meta.add("Contenttype", "text/html");
    Assertions.assertEquals(1, result.size());
    Assertions.assertEquals(1, result.getValues("name-one").length);
    Assertions.assertEquals("value-1.1", result.get("name-one"));
    meta.add("name-two", "value-2.1");
    meta.add("name-two", "value-2.2");
    result = writeRead(meta);
    Assertions.assertEquals(3, result.size());
    Assertions.assertEquals(1, result.getValues("name-one").length);
    Assertions.assertEquals("value-1.1", result.getValues("name-one")[0]);
    Assertions.assertEquals(2, result.getValues("name-two").length);
    Assertions.assertEquals("value-2.1", result.getValues("name-two")[0]);
    Assertions.assertEquals("value-2.2", result.getValues("name-two")[1]);
    Assertions.assertEquals("text/html", result.get(Metadata.CONTENT_TYPE));
  }

  /**
   * IO Test method, usable only when you plan to do changes in metadata to
   * measure relative performance impact.
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
      Assertions.fail(ioe.toString());
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
