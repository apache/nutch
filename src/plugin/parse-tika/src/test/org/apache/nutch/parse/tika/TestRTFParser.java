/*******************************************************************************
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
 ******************************************************************************/
package org.apache.nutch.parse.tika;

// JUnit imports
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseException;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.protocol.ProtocolException;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.MimeUtil;
import org.apache.nutch.util.NutchConfiguration;

/**
 * Unit tests for TestRTFParser. (Adapted from John Xing msword unit tests).
 * 
 * @author Andy Hedges
 */
public class TestRTFParser {

    private String fileSeparator = System.getProperty("file.separator");
    // This system property is defined in ./src/plugin/build-plugin.xml
    private String sampleDir = System.getProperty("test.data", ".");
    // Make sure sample files are copied to "test.data" as specified in
    // ./src/plugin/parse-rtf/build.xml during plugin compilation.
    // Check ./src/plugin/parse-rtf/sample/README.txt for what they are.
    private String rtfFile = "test.rtf";

    @Test
    public void testIt() throws ProtocolException, ParseException, IOException {

	String urlString;
	Parse parse;
	Configuration conf = NutchConfiguration.create();
	MimeUtil mimeutil = new MimeUtil(conf);

	urlString = "file:" + sampleDir + fileSeparator + rtfFile;

	File file = new File(sampleDir + fileSeparator + rtfFile);
	byte[] bytes = new byte[(int) file.length()];
	DataInputStream in = new DataInputStream(new FileInputStream(file));
	in.readFully(bytes);
	in.close();

	WebPage page = new WebPage();
	page.setBaseUrl(new Utf8(urlString));
	page.setContent(ByteBuffer.wrap(bytes));
	String mtype = mimeutil.getMimeType(file);
	page.setContentType(new Utf8(mtype));

	parse = new ParseUtil(conf).parse(urlString, page);

	String title = parse.getTitle();
	String text = parse.getText();
	assertEquals("test rft document", title);
	//assertEquals("The quick brown fox jumps over the lazy dog", text.trim());

	
	// HOW DO WE GET THE PARSE METADATA?
	// Metadata meta = parse();

	// METADATA extraction is not yet supported in Tika
	// 
	// assertEquals("tests", meta.get(DublinCore.SUBJECT));
    }

}
