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

package org.apache.nutch.parse.mspowerpoint;

import java.io.File;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * @author Stephan Strittmatter - http://www.sybit.de
 * 
 * @version 1.0
 */
public class AllTests {

  /** This system property is defined in ./src/plugin/build-plugin.xml */
  private final static String SAMPLE_DIR = System.getProperty("test.data",
      "build/parse-mspowerpoint/test/data");

  /**
   * Main to run the test
   * 
   * @param args
   *          not required
   */
  public static void main(String[] args) {
    junit.textui.TestRunner.run(AllTests.suite());
  }

  /**
   * @return Test for the PowerPoint plugin
   */
  public static Test suite() {
    final TestSuite suite = new TestSuite(
        "Test for org.apache.nutch.parse.mspowerpoint");
    
    System.out.println("Testing with ppt-files of dir: " + SAMPLE_DIR);
    
    final File sampleDir = new File(SAMPLE_DIR);

    //find all ppt-files in the test-directory
    final FileExtensionFilter pptFilter = new FileExtensionFilter(".ppt");
    final String[] pptFiles = sampleDir.list(pptFilter);

    if(pptFiles== null)
    {
      throw new IllegalArgumentException(SAMPLE_DIR + " does not contain any files: " + pptFilter);
    }
    TestSuite suiteAllFiles;
    

    // iterate over all ppt-files which are found and test against them
    for (int i = 0; i < pptFiles.length; i++) {
      //test the content...
      suiteAllFiles = new TestSuite("Testing file [" + pptFiles[i] + "]");
      TestCase test = new TestMSPowerPointParser(new File(pptFiles[i]));
      test.setName("testContent");

      suiteAllFiles.addTest(test);

      //..then the properties
      TestCase test2 = new TestMSPowerPointParser(new File(pptFiles[i]));
      test2.setName("testMeta");
      suiteAllFiles.addTest(test2);

      suite.addTest(suiteAllFiles);
    }

    return suite;
  }
}