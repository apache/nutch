/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nutch.analysis.lang;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Vector;

import junit.framework.TestCase;
import org.apache.lucene.analysis.Token;

public class TestNGramProfile extends TestCase {

  String tokencontent1 = "testaddtoken";
  String tokencontent2 = "anotherteststring";

  int[] counts1 = { 3, 2, 2, 2, 1, 1, 1, 1, 1 };

  String[] chars1 = { "t", "_", "d", "e", "a", "k", "n", "o", "s" };

  /**
   * Test addFromToken method
   *
   */
  public void testAddToken() {

    NGramProfile p = new NGramProfile("test", 1, 1);

    Token t = new Token(tokencontent1, 0, tokencontent1.length());
    p.addFromToken(t);
    p.normalize();
    
    testCounts(p.getSorted(), counts1);
    testContents(p.getSorted(), chars1);
  }

  /**
   * Test analyze method
   */
  public void testAnalyze() {
    String tokencontent = "testmeagain";

    NGramProfile p = new NGramProfile("test", 1, 1);
    p.analyze(new StringBuffer(tokencontent));

    //test that profile size is ok, eg 9 different NGramEntries "_tesmagin"
    assertEquals(9, p.getSorted().size());
  }

  /**
   * Test addNGrams method with StringBuffer argument
   *
   */
  public void testAddNGramsStringBuffer() {
    String tokencontent = "testmeagain";

    NGramProfile p = new NGramProfile("test", 1, 1);
    p.addNGrams(new StringBuffer(tokencontent));

    //test that profile size is ok, eg 8 different NGramEntries "tesmagin"
    assertEquals(8, p.getSorted().size());

  }

  /**
   * test getSorted method
   */
  public void testGetSorted() {
    int[] count = { 4, 3, 2, 1 };
    String[] ngram = { "a", "b", "" + NGramProfile.SEPARATOR, "c" };

    String teststring = "AAaaBbbC";

    NGramProfile p = new NGramProfile("test", 1, 1);
    p.analyze(new StringBuffer(teststring));

    //test size of profile
    assertEquals(4, p.getSorted().size());

    testCounts(p.getSorted(), count);
    testContents(p.getSorted(), ngram);

  }

  public void testGetSimilarity() {
    NGramProfile a = new NGramProfile("a", 1, 1);
    NGramProfile b = new NGramProfile("b", 1, 1);
    
    a.analyze(new StringBuffer(tokencontent1));
    b.analyze(new StringBuffer(tokencontent2));

    //because of rounding errors might slightly return different results
    assertEquals(a.getSimilarity(b), b.getSimilarity(a), 0.0000001);

  }

  public void testExactMatch() {
    NGramProfile a = new NGramProfile("a", 1, 1);
    
    a.analyze(new StringBuffer(tokencontent1));

    assertEquals(a.getSimilarity(a), 0, 0);

  }

  
  public void testIO() {
    //Create profile and set some contents
    NGramProfile a = new NGramProfile("a", 1, 1);
    a.analyze(new StringBuffer(this.tokencontent1));

    NGramProfile b = new NGramProfile("a_from_inputstream", 1, 1);

    //save profile
    ByteArrayOutputStream os = new ByteArrayOutputStream();

    try {
      a.save(os);
      os.close();
    } catch (Exception e) {
      fail();
    }

    //load profile
    InputStream is = new ByteArrayInputStream(os.toByteArray());
    try {
      b.load(is);
      is.close();
    } catch (Exception e) {
      fail();
    }

    //check it
    testCounts(b.getSorted(), counts1);
    testContents(b.getSorted(), chars1);
  }

  private void testContents(Vector entries, String contents[]) {
    int c = 0;
    Iterator i = entries.iterator();

    while (i.hasNext()) {
      NGramProfile.NGramEntry nge = (NGramProfile.NGramEntry) i.next();
      assertEquals(contents[c], nge.getSeq().toString());
      c++;
    }
  }

  private void testCounts(Vector entries, int counts[]) {
    int c = 0;
    Iterator i = entries.iterator();

    while (i.hasNext()) {
      NGramProfile.NGramEntry nge = (NGramProfile.NGramEntry) i.next();
      assertEquals(counts[c], nge.getCount());
      c++;
    }
  }
}
