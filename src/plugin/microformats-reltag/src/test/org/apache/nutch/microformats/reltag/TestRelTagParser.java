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
package org.apache.nutch.microformats.reltag;

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
import org.apache.nutch.protocol.ProtocolException;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.MimeUtil;
import org.apache.nutch.util.NutchConfiguration;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Junit test for {@link RelTagParser} based mainly John Xing's parser tests.
 * We are not concerned with actual parse text within the sample file, instead
 * we assert that the rel-tags we expect are found in the WebPage metadata.
 * To check the parser is working as expected we unwrap the ByteBuffer obtained 
 * from metadata, the same type as we use in expected (String). So just the 
 * other way around as we wrapped the metadata value.
 * 
 * @author lewismc
 *
 */
public class TestRelTagParser {

  private String fileSeparator = System.getProperty("file.separator");

  // This system property is defined in ./src/plugin/build-plugin.xml
  private String sampleDir = System.getProperty("test.data", ".");

  // Make sure sample files are copied to "test.data" as specified in
  // ./src/plugin/microformats-reltag/build.xml during plugin compilation.
  private String sampleFile = "microformats_reltag_test.html";
  
  // rel-tag's we expect to be extracted from page.getMetadata()
  private String expectedRelTags = "Category:Specifications	Category:rel-tag	";
  
  private Configuration conf;
  
  @Test
  public void testRelTagParser() throws ParseException, ProtocolException, IOException {
    conf = NutchConfiguration.create();
    conf.set("file.content.limit", "-1");
    Parse parse;
    String urlString = "file:" + sampleDir + fileSeparator + sampleFile;

    File file = new File(sampleDir + fileSeparator + sampleFile);
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
    //begin assertion for tests
    ByteBuffer bbuf = page.getFromMetadata(new Utf8("Rel-Tag"));
    byte[] byteArray = new byte[bbuf.remaining()];
    bbuf.get(byteArray);
    String s = new String(byteArray);
    //bbuf.flip();
    assertEquals("We expect 2 tab-separated rel-tag's extracted by the filter", 
      expectedRelTags, s);
  }
  
}