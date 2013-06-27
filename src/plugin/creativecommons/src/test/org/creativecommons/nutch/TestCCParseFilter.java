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

package org.creativecommons.nutch;

import org.apache.nutch.parse.ParseUtil;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.Bytes;
import org.apache.nutch.util.MimeUtil;
import org.apache.nutch.util.NutchConfiguration;
import java.io.*;
import java.nio.ByteBuffer;

import org.junit.Test;
import static org.junit.Assert.*;

public class TestCCParseFilter {

	private static final File testDir = new File(
			System.getProperty("test.input"));

  @Test
	public void testPages() throws Exception {
		pageTest(new File(testDir, "anchor.html"), "http://foo.com/",
				"http://creativecommons.org/licenses/by-nc-sa/1.0", "a", null);
		// Tika returns <a> whereas parse-html returns <rel>
		// check later
		pageTest(new File(testDir, "rel.html"), "http://foo.com/",
				"http://creativecommons.org/licenses/by-nc/2.0", "rel", null);
		// Tika returns <a> whereas parse-html returns <rdf>
		// check later
		pageTest(new File(testDir, "rdf.html"), "http://foo.com/",
				"http://creativecommons.org/licenses/by-nc/1.0", "rdf", "text");
	}

	public void pageTest(File file, String url, String license,
			String location, String type) throws Exception {

		InputStream in = new FileInputStream(file);
		ByteArrayOutputStream out = new ByteArrayOutputStream(
				(int) file.length());
		byte[] buffer = new byte[1024];
		int i;
		while ((i = in.read(buffer)) != -1) {
			out.write(buffer, 0, i);
		}
		in.close();
		byte[] bytes = out.toByteArray();
		Configuration conf = NutchConfiguration.create();

		WebPage page = new WebPage();
		page.setBaseUrl(new Utf8(url));
		page.setContent(ByteBuffer.wrap(bytes));
		MimeUtil mimeutil = new MimeUtil(conf);
		String mtype = mimeutil.getMimeType(file);
		page.setContentType(new Utf8(mtype));

		new ParseUtil(conf).parse(url, page);

		ByteBuffer bb = page.getFromMetadata(new Utf8("License-Url"));
		assertEquals(license, Bytes.toString(bb));
		bb = page.getFromMetadata(new Utf8("License-Location"));
		assertEquals(location, Bytes.toString(bb));
		bb = page.getFromMetadata(new Utf8("Work-Type"));
        assertEquals(type, Bytes.toString(bb));
	}
}
