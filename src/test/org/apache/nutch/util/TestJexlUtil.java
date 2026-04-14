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
package org.apache.nutch.util;

import org.apache.commons.jexl3.JexlScript;
import org.apache.commons.jexl3.MapContext;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link JexlUtil} sandboxing.
 */
public class TestJexlUtil {

  @Test
  public void testSandboxAllowsDocFieldCompare() throws Exception {
    JexlScript script = JexlUtil.parseExpression("doc.lang == 'en'");
    assertNotNull(script);
    MapContext doc = new MapContext();
    doc.set("lang", "en");
    MapContext root = new MapContext();
    root.set("doc", doc);
    assertTrue(Boolean.TRUE.equals(script.execute(root)));
  }

  @Test
  public void testSandboxAllowsScoreCompare() throws Exception {
    JexlScript script = JexlUtil.parseExpression("score > 0.5");
    assertNotNull(script);
    MapContext root = new MapContext();
    root.set("score", 0.9f);
    assertTrue(Boolean.TRUE.equals(script.execute(root)));
  }

  @Test
  public void testNewInstanceIoBlocked() {
    assertNull(JexlUtil.parseExpression("new java.io.File('/')"));
  }

  @Test
  public void testNewInstanceFileOutputStreamBlocked() {
    assertNull(JexlUtil.parseExpression(
        "new java.io.FileOutputStream('/tmp/nutch-jexl-poc')"));
  }

  @Test
  public void testDisableSandboxAllowsNewExpressionParse() {
    Configuration conf = new Configuration();
    conf.setBoolean(JexlUtil.DISABLE_SANDBOX_KEY, true);
    JexlScript script = JexlUtil.parseExpression(conf,
        "new java.io.File('/')");
    assertNotNull(script);
  }

  @Test
  public void testArithmeticAllowed() throws Exception {
    JexlScript script = JexlUtil.parseExpression("2 * 3 + 1 == 7");
    assertNotNull(script);
    assertTrue(Boolean.TRUE.equals(script.execute(new MapContext())));
  }

  @Test
  public void testStringMethodsAllowed() throws Exception {
    JexlScript script = JexlUtil.parseExpression(
        "url.startsWith('http://')");
    assertNotNull(script);
    MapContext root = new MapContext();
    root.set("url", "http://example.org/");
    assertTrue(Boolean.TRUE.equals(script.execute(root)));
  }

  @Test
  public void testDateRewriteStillParses() {
    JexlScript script = JexlUtil.parseExpression(
        "fetchTime > 2016-03-20T00:00:00Z");
    assertNotNull(script);
  }

  @Test
  public void testNullExpression() {
    assertNull(JexlUtil.parseExpression(null));
    assertNull(JexlUtil.parseExpression(new Configuration(), null));
  }

  @Test
  public void testInvalidSyntaxReturnsNull() {
    assertNull(JexlUtil.parseExpression("doc.lang=<>:='en'"));
  }

  @Test
  public void testListSize() throws Exception {
    JexlScript script = JexlUtil.parseExpression("doc.tags.size() == 2");
    assertNotNull(script);
    MapContext doc = new MapContext();
    java.util.List<String> tags = new java.util.ArrayList<>();
    tags.add("a");
    tags.add("b");
    doc.set("tags", tags);
    MapContext root = new MapContext();
    root.set("doc", doc);
    assertTrue(Boolean.TRUE.equals(script.execute(root)));
  }

  @Test
  public void testGeneratorStyleMetadata() throws Exception {
    JexlScript script = JexlUtil.parseExpression(
        "warc_import_time > 0 && score > 0");
    assertNotNull(script);
    MapContext root = new MapContext();
    root.set("warc_import_time", 1);
    root.set("score", 1.0f);
    assertTrue(Boolean.TRUE.equals(script.execute(root)));
  }

  @Test
  public void testEqualsIgnoreCase() throws Exception {
    JexlScript script = JexlUtil.parseExpression(
        "status.equalsIgnoreCase('FETCHED')");
    assertNotNull(script);
    MapContext root = new MapContext();
    root.set("status", "fetched");
    assertTrue(Boolean.TRUE.equals(script.execute(root)));
  }

  @Test
  public void testRegex() throws Exception {
    JexlScript script = JexlUtil.parseExpression(
        "url =~ 'https?://.*\\.example\\.org/.*'");
    assertNotNull(script);
    MapContext root = new MapContext();
    root.set("url", "http://foo.example.org/bar");
    assertTrue(Boolean.TRUE.equals(script.execute(root)));
  }

  @Test
  public void testTernary() throws Exception {
    JexlScript script = JexlUtil.parseExpression("true ? 1 : 0");
    assertNotNull(script);
    assertEquals(1, script.execute(new MapContext()));
  }
}
