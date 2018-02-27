/*
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
package org.apache.nutch.parsefilter.regex;

import java.net.MalformedURLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseImpl;
import org.apache.nutch.parse.ParseResult;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.NutchConfiguration;
import junit.framework.TestCase;

public class TestRegexParseFilter extends TestCase {

  private final static String SEPARATOR = System.getProperty("file.separator");
  private final static String SAMPLES = System.getProperty("test.data", ".");

  public void testPositiveFilter() throws Exception {
    Configuration conf = NutchConfiguration.create();

    String file = SAMPLES + SEPARATOR + "regex-parsefilter.txt";
    RegexParseFilter filter = new RegexParseFilter(file);
    filter.setConf(conf);

    String url = "http://nutch.apache.org/";
    String html = "<body><html><h1>nutch</h1><p>this is the extracted text blablabla</p></body></html>";
    Content content = new Content(url, url, html.getBytes("UTF-8"), "text/html", new Metadata(), conf);
    Parse parse = new ParseImpl("nutch this is the extracted text blablabla", new ParseData());
    
    ParseResult result = ParseResult.createParseResult(url, parse);
    result = filter.filter(content, result, null, null);

    Metadata meta = parse.getData().getParseMeta();
    
    assertEquals("true", meta.get("first"));
    assertEquals("true", meta.get("second"));
  }
  
  public void testNegativeFilter() throws Exception {
    Configuration conf = NutchConfiguration.create();

    String file = SAMPLES + SEPARATOR + "regex-parsefilter.txt";
    RegexParseFilter filter = new RegexParseFilter(file);
    filter.setConf(conf);

    String url = "http://nutch.apache.org/";
    String html = "<body><html><h2>nutch</h2><p>this is the extracted text no bla</p></body></html>";
    Content content = new Content(url, url, html.getBytes("UTF-8"), "text/html", new Metadata(), conf);
    Parse parse = new ParseImpl("nutch this is the extracted text bla", new ParseData());
    
    ParseResult result = ParseResult.createParseResult(url, parse);
    result = filter.filter(content, result, null, null);

    Metadata meta = parse.getData().getParseMeta();
    
    assertEquals("false", meta.get("first"));
    assertEquals("false", meta.get("second"));
  }
}
