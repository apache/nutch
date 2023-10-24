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
import org.junit.jupiter.api.Test;

/**
 * JUnit based tests of class {@link org.apache.nutch.metadata.Metadata}.
 */
public class TestMetadata {

  private static final String CONTENTTYPE = "contenttype";

  /**
   * Test to ensure that only non-null values get written when the
   * {@link Metadata} object is written using a Writeable.
   * 
   * @since NUTCH-406
   * 
   */
  @Test
  public void testWriteNonNull() {
    Metadata met = new Metadata();
    met.add(CONTENTTYPE, null);
    met.add(CONTENTTYPE, "text/bogus");
    met.add(CONTENTTYPE, "text/bogus2");
    met = writeRead(met);

    Assertions.assertNotNull(met);
    Assertions.assertEquals(met.size(), 1);

    boolean hasBogus = false, hasBogus2 = false;

    String[] values = met.getValues(CONTENTTYPE);
    Assertions.assertNotNull(values);
    Assertions.assertEquals(values.length, 2);

    for (int i = 0; i < values.length; i++) {
      if (values[i].equals("text/bogus")) {
        hasBogus = true;
      }

      if (values[i].equals("text/bogus2")) {
        hasBogus2 = true;
      }
    }

    Assertions.assertTrue(hasBogus && hasBogus2);
  }

  /** Test for the <code>add(String, String)</code> method. */
  @Test
  public void testAdd() {
    String[] values = null;
    Metadata meta = new Metadata();

    values = meta.getValues(CONTENTTYPE);
    Assertions.assertEquals(0, values.length);

    meta.add(CONTENTTYPE, "value1");
    values = meta.getValues(CONTENTTYPE);
    Assertions.assertEquals(1, values.length);
    Assertions.assertEquals("value1", values[0]);

    meta.add(CONTENTTYPE, "value2");
    values = meta.getValues(CONTENTTYPE);
    Assertions.assertEquals(2, values.length);
    Assertions.assertEquals("value1", values[0]);
    Assertions.assertEquals("value2", values[1]);

    // NOTE : For now, the same value can be added many times.
    // Should it be changed?
    meta.add(CONTENTTYPE, "value1");
    values = meta.getValues(CONTENTTYPE);
    Assertions.assertEquals(3, values.length);
    Assertions.assertEquals("value1", values[0]);
    Assertions.assertEquals("value2", values[1]);
    Assertions.assertEquals("value1", values[2]);
  }

  /** Test for the <code>set(String, String)</code> method. */
  @Test
  public void testSet() {
    String[] values = null;
    Metadata meta = new Metadata();

    values = meta.getValues(CONTENTTYPE);
    Assertions.assertEquals(0, values.length);

    meta.set(CONTENTTYPE, "value1");
    values = meta.getValues(CONTENTTYPE);
    Assertions.assertEquals(1, values.length);
    Assertions.assertEquals("value1", values[0]);

    meta.set(CONTENTTYPE, "value2");
    values = meta.getValues(CONTENTTYPE);
    Assertions.assertEquals(1, values.length);
    Assertions.assertEquals("value2", values[0]);

    meta.set(CONTENTTYPE, "new value 1");
    meta.add("contenttype", "new value 2");
    values = meta.getValues(CONTENTTYPE);
    Assertions.assertEquals(2, values.length);
    Assertions.assertEquals("new value 1", values[0]);
    Assertions.assertEquals("new value 2", values[1]);
  }

  /** Test for <code>setAll(Properties)</code> method. */
  @Test
  public void testSetProperties() {
    String[] values = null;
    Metadata meta = new Metadata();
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
    Metadata meta = new Metadata();
    Assertions.assertNull(meta.get("a-name"));
    meta.add("a-name", "value-1");
    Assertions.assertEquals("value-1", meta.get("a-name"));
    meta.add("a-name", "value-2");
    Assertions.assertEquals("value-1", meta.get("a-name"));
  }

  /** Test for <code>isMultiValued()</code> method. */
  @Test
  public void testIsMultiValued() {
    Metadata meta = new Metadata();
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
    Metadata meta = new Metadata();
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
    Metadata meta = new Metadata();
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
    Metadata meta1 = new Metadata();
    Metadata meta2 = new Metadata();
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
    Metadata result = null;
    Metadata meta = new Metadata();
    result = writeRead(meta);
    Assertions.assertEquals(0, result.size());
    meta.add("name-one", "value-1.1");
    result = writeRead(meta);
    Assertions.assertEquals(1, result.size());
    Assertions.assertEquals(1, result.getValues("name-one").length);
    Assertions.assertEquals("value-1.1", result.get("name-one"));
    meta.add("name-two", "value-2.1");
    meta.add("name-two", "value-2.2");
    result = writeRead(meta);
    Assertions.assertEquals(2, result.size());
    Assertions.assertEquals(1, result.getValues("name-one").length);
    Assertions.assertEquals("value-1.1", result.getValues("name-one")[0]);
    Assertions.assertEquals(2, result.getValues("name-two").length);
    Assertions.assertEquals("value-2.1", result.getValues("name-two")[0]);
    Assertions.assertEquals("value-2.2", result.getValues("name-two")[1]);
  }

  private Metadata writeRead(Metadata meta) {
    Metadata readed = new Metadata();
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

}
