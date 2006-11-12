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

package org.apache.nutch.ontology.jena;

import org.apache.nutch.ontology.*;
import org.apache.nutch.protocol.ProtocolException;

import org.apache.nutch.parse.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.util.NutchConfiguration;

import junit.framework.TestCase;

import java.util.Iterator;
import java.util.List;
import java.util.LinkedList;

import java.lang.Exception;

/** 
 * Unit tests for Ontology
 * 
 * @author michael j pan
 */
public class TestOntology extends TestCase {

  private String fileSeparator = System.getProperty("file.separator");
  // This system property is defined in ./src/plugin/build-plugin.xml
  private String sampleDir = System.getProperty("test.data",".");
  // Make sure sample files are copied to "test.data" as specified in
  // ./src/plugin/ontology/build.xml during plugin compilation.
  // Check ./src/plugin/ontology/sample/README.txt for what they are.
  private String[] sampleFiles = {"time.owl"};

  private static Ontology ontology;
  private Configuration conf;
  public TestOntology(String name) { 
    super(name); 
  }

  protected void setUp() {
      this.conf = NutchConfiguration.create();
  }

  protected void tearDown() {}

  public void testIt() throws ProtocolException, ParseException, Exception {
    String className = "Season";
    String[] subclassNames =
      new String[] {"Spring", "Summer", "Fall", "Winter"};

    if (ontology==null) {
      try {
        ontology = new OntologyFactory(this.conf).getOntology();
      } catch (Exception e) {
        throw new Exception("Failed to instantiate ontology");
      }
    }

    //foreach sample file
    for (int i=0; i<sampleFiles.length; i++) {
      //construct the url
      String urlString = "file:" + sampleDir + fileSeparator + sampleFiles[i];

      ontology.load(new String[] {urlString});

      List subclassList = new LinkedList();
  
      Iterator iter = ontology.subclasses(className);
      while (iter.hasNext()) {
        String subclassLabel = (String) iter.next();
        System.out.println(subclassLabel);
        subclassList.add(subclassLabel);
      }
  
      for (int j=0; j<subclassNames.length; j++) {
        assertTrue(subclassList.contains(subclassNames[j]));
      }
    }

  }

}
