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
package org.apache.nutch.searcher;

// JUnit imports
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

// Nutch imports
import org.apache.nutch.searcher.Summary.Ellipsis;
import org.apache.nutch.searcher.Summary.Fragment;
import org.apache.nutch.searcher.Summary.Highlight;
import org.apache.nutch.util.WritableTestUtils;


/**
 * JUnit based test of class {@link Summary}.
 *
 * @author J&eacute;r&ocirc;me Charron
 */
public class TestSummary extends TestCase {
  
  public TestSummary(String testName) {
    super(testName);
  }

  public static Test suite() {
    return new TestSuite(TestSummary.class);
  }
  

  /** Test of <code>Fragment</code> inner class */
  public void testFragment() {
    Fragment fragment = new Fragment("fragment text");
    assertEquals("fragment text", fragment.getText());
    assertEquals("fragment text", fragment.toString());
    assertFalse(fragment.isEllipsis());
    assertFalse(fragment.isHighlight());
    assertTrue(fragment.equals(new Fragment("fragment text")));
    assertFalse(fragment.equals(new Fragment("some text")));
    assertFalse(fragment.equals(new Ellipsis()));
    assertFalse(fragment.equals(new Highlight("fragment text")));
  }

  /** Test of <code>Ellipsis</code> inner class */
  public void testEllipsis() {
    Fragment fragment = new Ellipsis();
    assertEquals(" ... ", fragment.getText());
    assertEquals(" ... ", fragment.toString());
    assertTrue(fragment.isEllipsis());
    assertFalse(fragment.isHighlight());
    assertFalse(fragment.equals(new Fragment("fragment text")));
    assertTrue(fragment.equals(new Ellipsis()));
    assertFalse(fragment.equals(new Highlight("fragment text")));
  }

  /** Test of <code>Highlight</code> inner class */
  public void testHighlight() {
    Fragment fragment = new Highlight("highlight text");
    assertEquals("highlight text", fragment.getText());
    assertEquals("highlight text", fragment.toString());
    assertFalse(fragment.isEllipsis());
    assertTrue(fragment.isHighlight());
    assertFalse(fragment.equals(new Fragment("fragment text")));
    assertFalse(fragment.equals(new Ellipsis()));
    assertFalse(fragment.equals(new Highlight("fragment text")));
    assertTrue(fragment.equals(new Highlight("highlight text")));
  }

  /** Test of <code>add</code> / <code>get</code> methods */
  public void testAdd() {
    Fragment[] fragments = null;
    Summary summary = new Summary();
    summary.add(new Fragment("fragment1"));
    fragments = summary.getFragments();
    assertEquals(1, fragments.length);
    assertEquals("fragment1", fragments[0].toString());
    summary.add(new Fragment("fragment2"));
    fragments = summary.getFragments();
    assertEquals(2, fragments.length);
    assertEquals("fragment1", fragments[0].toString());
    assertEquals("fragment2", fragments[1].toString());
    summary.add(new Fragment("fragment3"));
    fragments = summary.getFragments();
    assertEquals(3, fragments.length);
    assertEquals("fragment1", fragments[0].toString());
    assertEquals("fragment2", fragments[1].toString());
    assertEquals("fragment3", fragments[2].toString());
  }

  /** Test of <code>toString</code> method. */
  public void testToString() {
    Summary summary = new Summary();
    assertEquals("", summary.toString());
    summary.add(new Fragment("fragment1"));
    assertEquals("fragment1", summary.toString());
    summary.add(new Ellipsis());
    assertEquals("fragment1 ... ", summary.toString());
    summary.add(new Highlight("highlight"));
    assertEquals("fragment1 ... highlight", summary.toString());
    summary.add(new Fragment("fragment2"));
    assertEquals("fragment1 ... highlightfragment2", summary.toString());    
  }

  /** Test of <code>toStrings</code>. */
  public void testToStrings() {
    Summary[] summaries = { new Summary(), new Summary() };
    summaries[0].add(new Fragment("fragment1.1"));
    summaries[0].add(new Ellipsis());
    summaries[0].add(new Highlight("highlight1"));
    summaries[0].add(new Fragment("fragment1.2"));
    summaries[1].add(new Fragment("fragment2.1"));
    summaries[1].add(new Ellipsis());
    summaries[1].add(new Highlight("highlight2"));
    summaries[1].add(new Fragment("fragment2.2"));
    String[] strings = Summary.toStrings(summaries);
    assertEquals(2, strings.length);
    assertEquals("fragment1.1 ... highlight1fragment1.2", strings[0]);
    assertEquals("fragment2.1 ... highlight2fragment2.2", strings[1]);
  }

  /** Test of <code>equals</code> method. */
  public void testEquals() {
    Summary summary1 = new Summary();
    Summary summary2 = new Summary();
    assertFalse(summary1.equals(null));
    assertFalse(summary1.equals(""));
    assertTrue(summary1.equals(summary2));
    summary1.add(new Fragment("text fragment"));
    assertFalse(summary1.equals(summary2));
    summary2.add(new Fragment("text fragment"));
    assertTrue(summary1.equals(summary2));
    summary1.add(new Ellipsis());
    assertFalse(summary1.equals(summary2));
    summary2.add(new Ellipsis());
    assertTrue(summary1.equals(summary2));
    summary1.add(new Highlight("highlight"));
    assertFalse(summary1.equals(summary2));
    summary2.add(new Highlight("highlight"));
    assertTrue(summary1.equals(summary2));
    summary1.add(new Fragment("text fragment"));
    summary2.add(new Fragment("fragment text"));
    assertFalse(summary1.equals(summary2));
  }
  
  /** Test of <code>writable</code> implementation. */
  public void testWritable() throws Exception {
    Summary summary = new Summary();
    summary.add(new Fragment("fragment1.1"));
    summary.add(new Ellipsis());
    summary.add(new Highlight("highlight1"));
    summary.add(new Fragment("fragment1.2"));
    WritableTestUtils.testWritable(summary);
  }
  
}
