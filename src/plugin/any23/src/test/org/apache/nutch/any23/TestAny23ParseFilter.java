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
package org.apache.nutch.any23;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseException;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.parse.ParserNotFound;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.MimeUtil;
import org.apache.nutch.util.NutchConfiguration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestAny23ParseFilter {


  private Configuration conf;

  private String fileSeparator = System.getProperty("file.separator");

  // This system property is defined in ./src/plugin/build-plugin.xml
  private String sampleDir = System.getProperty("test.data", ".");

  // Make sure sample files are copied to "test.data" as specified in
  // ./src/plugin/any23/build.xml during plugin compilation.
  private String file1 = "BBC_News_Scotland.html";
  
  private String file2 = "microdata_basic.html";

  private static final int EXPECTED_TRIPLES_1 = 117;
  
  private static final int EXPECTED_TRIPLES_2 = 40;
  
  @Before
  public void setUp() {
    this.conf = NutchConfiguration.create();
    conf.set("file.content.limit", "-1");
    conf.set("parser.timeout", "-1");
  }

  @Test
  public void testExtractTriplesFromHTML() throws IOException, ParserNotFound, ParseException {

    String urlString = "file:" + sampleDir + fileSeparator + file1;

    File file = new File(sampleDir + fileSeparator + file1);
    
    String[] triplesArray = extract(urlString, file);
    
    Assert.assertEquals("We expect 117 tab-separated triples extracted by the filter", 
        EXPECTED_TRIPLES_1, triplesArray.length);
  }

  @Test
  public void extractMicroDataFromHTML() throws ParserNotFound, IOException, ParseException {
    String urlString = "file:" + sampleDir + fileSeparator + file2;

    File file = new File(sampleDir + fileSeparator + file2);
    
    String[] triplesArray = extract(urlString, file);
    
    Assert.assertEquals("We expect 40 tab-separated triples extracted by the filter", 
        EXPECTED_TRIPLES_2, triplesArray.length);
  }
  
  public String[] extract(String urlString, File file) throws IOException, ParserNotFound, ParseException {
    @SuppressWarnings("unused")
    Parse parse;
    byte[] bytes = new byte[(int) file.length()];
    DataInputStream in = new DataInputStream(new FileInputStream(file));
    in.readFully(bytes);
    in.close();

    WebPage page = new WebPage();
    page.setBaseUrl(new Utf8(urlString));
    page.setContent(ByteBuffer.wrap(bytes));
    MimeUtil mimeutil = new MimeUtil(conf);
    String mtype = mimeutil.getMimeType(file);
    page.setContentType(new Utf8(mtype));
    parse = new ParseUtil(conf).parse(urlString, page);
    ByteBuffer bbuf = page.getFromMetadata(new Utf8("Any23-Triples"));
    byte[] byteArray = new byte[bbuf.remaining()];
    bbuf.get(byteArray);
    String s = new String(byteArray);
    String[] triplesArray = s.split("\t");

    return triplesArray;
  }
}
