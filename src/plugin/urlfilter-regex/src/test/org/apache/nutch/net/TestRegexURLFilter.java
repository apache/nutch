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
package org.apache.nutch.net;

// JDK imports
import java.io.IOException;
import java.io.Reader;

// JUnit imports
import junit.framework.Test;
import junit.framework.TestSuite;
import junit.textui.TestRunner;


/**
 * JUnit based test of class <code>RegexURLFilter</code>.
 *
 * @author J&eacute;r&ocirc;me Charron
 */
public class TestRegexURLFilter extends RegexURLFilterBaseTest {
  
  public TestRegexURLFilter(String testName) {
    super(testName);
  }
  
  public static Test suite() {
    return new TestSuite(TestRegexURLFilter.class);
  }
  
  public static void main(String[] args) {
    TestRunner.run(suite());
  }

  protected URLFilter getURLFilter(Reader rules) {
    try {
      return new RegexURLFilter(rules);
    } catch (IOException e) {
      fail(e.toString());
      return null;
    }
  }
  
  public void test() {
    test("WholeWebCrawling");
    test("IntranetCrawling");
    bench(50, "Benchmarks");
    bench(100, "Benchmarks");
    bench(200, "Benchmarks");
    bench(400, "Benchmarks");
    bench(800, "Benchmarks");
  }

}
