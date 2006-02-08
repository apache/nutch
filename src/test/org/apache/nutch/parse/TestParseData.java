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

package org.apache.nutch.parse;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.util.NutchConfiguration;

import org.apache.nutch.util.WritableTestUtils;
import org.apache.nutch.metadata.Metadata;

import junit.framework.TestCase;

/** Unit tests for ParseData. */

public class TestParseData extends TestCase {
    
  private Configuration conf = NutchConfiguration.create();
  
  public TestParseData(String name) { super(name); }

  public void testParseData() throws Exception {

    String title = "The Foo Page";

    Outlink[] outlinks = new Outlink[] {
      new Outlink("http://foo.com/", "Foo", conf),
      new Outlink("http://bar.com/", "Bar", conf)
    };

    Metadata metaData = new Metadata();
    metaData.add("Language", "en/us");
    metaData.add("Charset", "UTF-8");

    ParseData r = new ParseData(ParseStatus.STATUS_SUCCESS, title, outlinks, metaData);
    r.setConf(conf);
                        
    WritableTestUtils.testWritable(r, conf);
  }
	
}
