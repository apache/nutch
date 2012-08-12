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
import org.apache.nutch.parse.Parser;
import org.apache.nutch.protocol.ProtocolException;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.MimeUtil;
import org.apache.nutch.util.NutchConfiguration;
import org.junit.Test;

import junit.framework.TestCase;

/**
 * Junit test for {@link RelTagParser} based on John Xing's parser tests.
 * 
 * @author lewismc
 *
 */
public class TestRelTagParser extends TestCase {

  private String fileSeparator = System.getProperty("file.separator");

  // This system property is defined in ./src/plugin/build-plugin.xml
  private String sampleDir = System.getProperty("test.data", ".");

  // Make sure sample files are copied to "test.data" as specified in
  // ./src/plugin/microformats-reltag/build.xml during plugin compilation.

  private String[] sampleFile = { "microformats_reltag_test.html" };
  
  private String expectedText = "rel=\"tag\" · Microformats Wiki";
  
  private Configuration conf;
  
  public TestRelTagParser(String name) {
    super(name);
  }
  
  protected void setUp() {
    conf = NutchConfiguration.create();
	conf.set("file.content.limit", "-1");
  }

  protected void tearDown() {
  }
  
  public String getTextContent(String fileName) throws ProtocolException, ParseException, IOException {
	Parse parse;
	String urlString = sampleDir + fileSeparator + fileName;

	File file = new File(urlString);
	byte[] bytes = new byte[(int) file.length()];
	DataInputStream in = new DataInputStream(new FileInputStream(file));
	in.readFully(bytes);
	in.close();

	WebPage page = new WebPage();
	page.setBaseUrl(new Utf8("file:"+urlString));
	page.setContent(ByteBuffer.wrap(bytes));
	MimeUtil mimeutil = new MimeUtil(conf);
	String mtype = mimeutil.getMimeType(file);
	page.setContentType(new Utf8(mtype));
	parse = new ParseUtil(conf).parse("file:"+urlString, page);

	return parse.getText();
  }
  
  @Test
  public void testRelTagParser() throws ProtocolException, ParseException, IOException {

	for (int i = 0; i < sampleFile.length; i++) {
	  String found = getTextContent(sampleFile[i]);
	  assertTrue("text found : '" + found + "'", found.startsWith(expectedText));
	}
  }
	
}