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

package org.apache.nutch.parse.tika;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import junit.framework.TestCase;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseException;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.protocol.ProtocolException;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.MimeUtil;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.tika.mime.MimeType;

/**
 * Unit tests for MSWordParser.
 * 
 * @author John Xing
 */
public class TestMSWordParser extends TestCase {

    private String fileSeparator = System.getProperty("file.separator");
    // This system property is defined in ./src/plugin/build-plugin.xml
    private String sampleDir = System.getProperty("test.data", ".");
    // Make sure sample files are copied to "test.data" as specified in
    // ./src/plugin/parse-msword/build.xml during plugin compilation.
    // Check ./src/plugin/parse-msword/sample/README.txt for what they are.
    private String[] sampleFiles = { "word97.doc" };

    private String expectedText = "This is a sample doc file prepared for nutch.";

    private Configuration conf;

    public TestMSWordParser(String name) {
	super(name);
    }

    protected void setUp() {
	conf = NutchConfiguration.create();
	conf.set("file.content.limit", "-1");
    }

    protected void tearDown() {
    }

    public String getTextContent(String fileName) throws ProtocolException,
	    ParseException, IOException {
	String urlString = sampleDir + fileSeparator + fileName;

	File file = new File(urlString);
	byte[] bytes = new byte[(int) file.length()];
	DataInputStream in = new DataInputStream(new FileInputStream(file));
	in.readFully(bytes);
	in.close();
	Parse parse;
	WebPage page = new WebPage();
	page.setBaseUrl(new Utf8("file:"+urlString));
	page.setContent(ByteBuffer.wrap(bytes));
	// set the content type?
	MimeUtil mimeutil = new MimeUtil(conf);
	MimeType mtype = mimeutil.getMimeType(file);
	page.setContentType(new Utf8(mtype.getName()));
		
	parse = new ParseUtil(conf).parse("file:"+urlString, page);
	return parse.getText();
    }

    public void testIt() throws ProtocolException, ParseException, IOException {
	for (int i = 0; i < sampleFiles.length; i++) {
	    String found = getTextContent(sampleFiles[i]);
	    assertTrue("text found : '" + found + "'", found
		    .startsWith(expectedText));
	}
    }

    public void testOpeningDocs() throws ProtocolException, ParseException, IOException {
	String[] filenames = new File(sampleDir).list();
	for (int i = 0; i < filenames.length; i++) {
	    if (filenames[i].endsWith(".doc") == false)
		continue;
	    assertTrue("cann't read content of " + filenames[i],
		    getTextContent(filenames[i]).length() > 0);
	}
    }
}
