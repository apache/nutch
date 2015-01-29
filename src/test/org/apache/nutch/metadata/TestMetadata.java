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

    Assert.assertNotNull(met);
    Assert.assertEquals(met.size(), 1);

    boolean hasBogus = false, hasBogus2 = false;

    String[] values = met.getValues(CONTENTTYPE);
    Assert.assertNotNull(values);
    Assert.assertEquals(values.length, 2);

    for (int i = 0; i < values.length; i++) {
      if (values[i].equals("text/bogus")) {
        hasBogus = true;
      }

      if (values[i].equals("text/bogus2")) {
        hasBogus2 = true;
      }
    }

    Assert.assertTrue(hasBogus && hasBogus2);
  }

  /** Test for the <code>add(String, String)</code> method. */
  @Test
  public void testAdd() {
    String[] values = null;
    Metadata meta = new Metadata();

    values = meta.getValues(CONTENTTYPE);
    Assert.assertEquals(0, values.length);

    meta.add(CONTENTTYPE, "value1");
    values = meta.getValues(CONTENTTYPE);
    Assert.assertEquals(1, values.length);
    Assert.assertEquals("value1", values[0]);

    meta.add(CONTENTTYPE, "value2");
    values = meta.getValues(CONTENTTYPE);
    Assert.assertEquals(2, values.length);
    Assert.assertEquals("value1", values[0]);
    Assert.assertEquals("value2", values[1]);

    // NOTE : For now, the same value can be added many times.
    // Should it be changed?
    meta.add(CONTENTTYPE, "value1");
    values = meta.getValues(CONTENTTYPE);
    Assert.assertEquals(3, values.length);
    Assert.assertEquals("value1", values[0]);
    Assert.assertEquals("value2", values[1]);
    Assert.assertEquals("value1", values[2]);
  }

  /** Test for the <code>set(String, String)</code> method. */
  @Test
  public void testSet() {
    String[] values = null;
    Metadata meta = new Metadata();

    values = meta.getValues(CONTENTTYPE);
    Assert.assertEquals(0, values.length);

    meta.set(CONTENTTYPE, "value1");
    values = meta.getValues(CONTENTTYPE);
    Assert.assertEquals(1, values.length);
    Assert.assertEquals("value1", values[0]);

    meta.set(CONTENTTYPE, "value2");
    values = meta.getValues(CONTENTTYPE);
    Assert.assertEquals(1, values.length);
    Assert.assertEquals("value2", values[0]);

    meta.set(CONTENTTYPE, "new value 1");
    meta.add("contenttype", "new value 2");
    values = meta.getValues(CONTENTTYPE);
    Assert.assertEquals(2, values.length);
    Assert.assertEquals("new value 1", values[0]);
    Assert.assertEquals("new value 2", values[1]);
  }

  /** Test for <code>setAll(Properties)</code> method. */
  @Test
  public void testSetProperties() {
    String[] values = null;
    Metadata meta = new Metadata();
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
    Metadata meta = new Metadata();
    Assert.assertNull(meta.get("a-name"));
    meta.add("a-name", "value-1");
    Assert.assertEquals("value-1", meta.get("a-name"));
    meta.add("a-name", "value-2");
    Assert.assertEquals("value-1", meta.get("a-name"));
  }

  /** Test for <code>isMultiValued()</code> method. */
  @Test
  public void testIsMultiValued() {
    Metadata meta = new Metadata();
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
    Metadata meta = new Metadata();
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
    Metadata meta = new Metadata();
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
    Metadata meta1 = new Metadata();
    Metadata meta2 = new Metadata();
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
    Metadata result = null;
    Metadata meta = new Metadata();
    result = writeRead(meta);
    Assert.assertEquals(0, result.size());
    meta.add("name-one", "value-1.1");
    result = writeRead(meta);
    Assert.assertEquals(1, result.size());
    Assert.assertEquals(1, result.getValues("name-one").length);
    Assert.assertEquals("value-1.1", result.get("name-one"));
    meta.add("name-two", "value-2.1");
    meta.add("name-two", "value-2.2");
    result = writeRead(meta);
    Assert.assertEquals(2, result.size());
    Assert.assertEquals(1, result.getValues("name-one").length);
    Assert.assertEquals("value-1.1", result.getValues("name-one")[0]);
    Assert.assertEquals(2, result.getValues("name-two").length);
    Assert.assertEquals("value-2.1", result.getValues("name-two")[0]);
    Assert.assertEquals("value-2.2", result.getValues("name-two")[1]);
  }

  private Metadata writeRead(Metadata meta) {
    Metadata readed = new Metadata();
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

}
