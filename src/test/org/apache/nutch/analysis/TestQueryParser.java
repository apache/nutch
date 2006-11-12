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

package org.apache.nutch.analysis;

import org.apache.nutch.searcher.Query;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.util.NutchConfiguration;

import junit.framework.TestCase;

/**
 * JUnit tests for query parser
 *  
 */
public class TestQueryParser extends TestCase {

  private static Configuration conf = NutchConfiguration.create();
  public void assertQueryEquals(String query, String result) throws Exception {
    try {
      Query q = NutchAnalysis.parseQuery(query, conf);
      String s = q.toString();
      if (!s.equals(result)) {
        fail("Query /" + query + "/ yielded /" + s + "/, expecting /" + result
            + "/");
      }
    } catch (Exception e) {
      throw new Exception("error: While parsing query:" + query, e);
    }
  }

  /**
   * Test query parser
   * 
   * @throws Exception
   */
  public void testParseQuery() throws Exception {
    //simple tests
    assertQueryEquals("x", "x");
    assertQueryEquals("X", "x");
    assertQueryEquals("+x", "x");
    assertQueryEquals("-x", "-x");
    assertQueryEquals("x y", "x y");
    assertQueryEquals(" x  y ", "x y");
    assertQueryEquals("test +", "test");

    // missing fourth double quote
    assertQueryEquals("\" abc def \" \" def ghi ", "\"abc def\" \"def ghi\"");

    //empty query
    assertQueryEquals("\"", "");

    //fields
    assertQueryEquals("field:x -another:y", "field:x -another:y");
    assertQueryEquals("the:x", "the:x");

    //ACRONYM
    assertQueryEquals("w.s.o.p.", "wsop");

    //STOPWORD
    assertQueryEquals("the", "");
    assertQueryEquals("field:the -y", "field:the -y");
    assertQueryEquals("\"the y\"", "\"the y\"");
    assertQueryEquals("+the -y", "the -y");

    //PHRASE
    assertQueryEquals("\"hello world\"", "\"hello world\"");
    assertQueryEquals("\"phrase a.b.c. phrase\"", "\"phrase abc phrase\"");
    assertQueryEquals("\"the end\"", "\"the end\"");
    assertQueryEquals("term\"the end\"", "term \"the end\"");
    //unbalanced
    assertQueryEquals("term\"the end", "term \"the end\"");

    //SIGRAM
    assertQueryEquals("\u3040\u3041\u3042", "\u3040 \u3041 \u3042");

    //COMPOUND
    assertQueryEquals("term some.email@adress.here",
        "term \"some email adress here\"");
  }
}
