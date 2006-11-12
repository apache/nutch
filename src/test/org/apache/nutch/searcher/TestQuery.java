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

import java.io.*;
import junit.framework.TestCase;
import java.util.Arrays;
import org.apache.nutch.analysis.NutchAnalysis;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.util.NutchConfiguration;

public class TestQuery extends TestCase {
    
  private static Configuration conf = NutchConfiguration.create();
    
  public TestQuery(String name) { super(name); }

  public void testRequiredTerm() throws Exception {
    Query query = new Query(conf);
    query.addRequiredTerm("bobo");
    testQuery(query, "bobo");
  }

  public void testProhibitedTerm() throws Exception {
    Query query = new Query(conf);
    query.addProhibitedTerm("bobo");
    testQuery(query, "-bobo");
  }

  public void testRequiredPhrase() throws Exception {
    Query query = new Query(conf);
    query.addRequiredPhrase(new String[] {"bobo", "bogo"});
    testQuery(query, "\"bobo bogo\"");
  }

  public void testProhibitedPhrase() throws Exception {
    Query query = new Query(conf);
    query.addProhibitedPhrase(new String[] {"bobo", "bogo"});
    testQuery(query, "-\"bobo bogo\"");
  }

  public void testComplex() throws Exception {
    Query query = new Query(conf);
    query.addRequiredTerm("bobo");
    query.addProhibitedTerm("bono");
    query.addRequiredPhrase(new String[] {"bobo", "bogo"});
    query.addProhibitedPhrase(new String[] {"bogo", "bobo"});
    testQuery(query, "bobo -bono \"bobo bogo\" -\"bogo bobo\"");
  }

  public static void testQuery(Query query, String string) throws Exception {
    testQueryToString(query, string);
    testQueryParser(query, string);
    testQueryIO(query, string);
  }

  public static void testQueryToString(Query query, String string) {
    assertEquals(query.toString(), string);
  }

  public static void testQueryParser(Query query, String string)
    throws Exception {
    Query after = NutchAnalysis.parseQuery(string, conf);
    assertEquals(after, query);
    assertEquals(after.toString(), string);
  }

  public static void testQueryIO(Query query, String string) throws Exception {
    ByteArrayOutputStream oBuf = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(oBuf);
    query.write(out);

    ByteArrayInputStream iBuf = new ByteArrayInputStream(oBuf.toByteArray());
    DataInputStream in = new DataInputStream(iBuf);

    Query after = Query.read(in, conf);

    assertEquals(after, query);
  }

  public void testQueryTerms() throws Exception {
    testQueryTerms("foo bar", new String[] {"foo", "bar"});
    testQueryTerms("\"foo bar\"", new String[] {"foo", "bar"});
    testQueryTerms("\"foo bar\" baz", new String[] {"foo", "bar", "baz"});
  }

  public static void testQueryTerms(String query, String[] terms)
    throws Exception {
    assertTrue(Arrays.equals(NutchAnalysis.parseQuery(query, conf).getTerms(),
                             terms));
  }

  public static void main(String[] args) throws Exception {
    TestQuery test = new TestQuery("test");
    test.testComplex();
  }

}
