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

package org.apache.nutch.crawl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.nutch.util.NutchConfiguration;

import junit.framework.TestCase;

public class TestLinkDbMerger extends TestCase {
  private static final Logger LOG = Logger.getLogger(TestLinkDbMerger.class.getName());
  
  String url10 = "http://example.com/foo";
  String[] urls10 = new String[] {
          "http://example.com/100",
          "http://example.com/101"
        };

  String url11 = "http://example.com/";
  String[] urls11 = new String[] {
          "http://example.com/110",
          "http://example.com/111"
        };
  
  String url20 = "http://example.com/";
  String[] urls20 = new String[] {
          "http://foo.com/200",
          "http://foo.com/201"
        };
  String url21 = "http://example.com/bar";
  String[] urls21 = new String[] {
          "http://foo.com/210",
          "http://foo.com/211"
        };
  
  String[] urls10_expected = urls10;
  String[] urls11_expected = new String[] {
          urls11[0],
          urls11[1],
          urls20[0],
          urls20[1]
  };
  String[] urls20_expected = urls11_expected;
  String[] urls21_expected = urls21;
  
  TreeMap init1 = new TreeMap();
  TreeMap init2 = new TreeMap();
  HashMap expected = new HashMap();
  Configuration conf;
  Path testDir;
  FileSystem fs;
  LinkDbReader reader;
  
  public void setUp() throws Exception {
    init1.put(url10, urls10);
    init1.put(url11, urls11);
    init2.put(url20, urls20);
    init2.put(url21, urls21);
    expected.put(url10, urls10_expected);
    expected.put(url11, urls11_expected);
    expected.put(url20, urls20_expected);
    expected.put(url21, urls21_expected);
    conf = NutchConfiguration.create();
    fs = FileSystem.get(conf);
    testDir = new Path("build/test/test-linkdb-" +
            new java.util.Random().nextInt());
    fs.mkdirs(testDir);
  }
  
  public void tearDown() {
    try {
      if (fs.exists(testDir))
        fs.delete(testDir);
    } catch (Exception e) { }
    try {
      reader.close();
    } catch (Exception e) { }
  }

  public void testMerge() throws Exception {
    Configuration conf = NutchConfiguration.create();
    FileSystem fs = FileSystem.get(conf);
    fs.mkdirs(testDir);
    Path linkdb1 = new Path(testDir, "linkdb1");
    Path linkdb2 = new Path(testDir, "linkdb2");
    Path output = new Path(testDir, "output");
    createLinkDb(conf, fs, linkdb1, init1);
    createLinkDb(conf, fs, linkdb2, init2);
    LinkDbMerger merger = new LinkDbMerger(conf);
    LOG.fine("* merging linkdbs to " + output);
    merger.merge(output, new Path[]{linkdb1, linkdb2}, false, false);
    LOG.fine("* reading linkdb: " + output);
    reader = new LinkDbReader(conf, output);
    Iterator it = expected.keySet().iterator();
    while (it.hasNext()) {
      String url = (String)it.next();
      LOG.fine("url=" + url);
      String[] vals = (String[])expected.get(url);
      Inlinks inlinks = reader.getInlinks(new Text(url));
      // may not be null
      assertNotNull(inlinks);
      ArrayList links = new ArrayList();
      Iterator it2 = inlinks.iterator();
      while (it2.hasNext()) {
        Inlink in = (Inlink)it2.next();
        links.add(in.getFromUrl());
      }
      for (int i = 0; i < vals.length; i++) {
        LOG.fine(" -> " + vals[i]);
        assertTrue(links.contains(vals[i]));
      }
    }
    reader.close();
    fs.delete(testDir);
  }
  
  private void createLinkDb(Configuration config, FileSystem fs, Path linkdb, TreeMap init) throws Exception {
    LOG.fine("* creating linkdb: " + linkdb);
    Path dir = new Path(linkdb, LinkDb.CURRENT_NAME);
    MapFile.Writer writer = new MapFile.Writer(config, fs, new Path(dir, "part-00000").toString(), Text.class, Inlinks.class);
    Iterator it = init.keySet().iterator();
    while (it.hasNext()) {
      String key = (String)it.next();
      Inlinks inlinks = new Inlinks();
      String[] vals = (String[])init.get(key);
      for (int i = 0; i < vals.length; i++) {
        Inlink in = new Inlink(vals[i], vals[i]);
        inlinks.add(in);
      }
      writer.append(new Text(key), inlinks);
    }
    writer.close();
  }
}
