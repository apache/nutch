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

package org.apache.nutch.parse.mspowerpoint;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.Protocol;
import org.apache.nutch.protocol.ProtocolFactory;
import org.apache.nutch.util.NutchConfiguration;

import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;

/**
 * <p>
 * Unit tests for MSPowerPointParser.
 * </p>
 * <p>
 * Make sure sample files are copied to "test.data" as specified in
 * ./src/plugin/parse-mspowerpoint/build.xml during plugin compilation. Check
 * ./src/plugin/parse-mspowerpoint/sample/README.txt for what they are.
 * </p>
 * 
 * @author Stephan Strittmatter - http://www.sybit.de
 * 
 * @version 1.0
 */
public class TestMSPowerPointParser extends TestCase {
  private static final Log LOG = LogFactory.getLog(TestMSPowerPointParser.class);

  private static final String CHARSET = "UTF-8";

  private final static String LINE_SEPARATOR = System.getProperty("line.separator");

  /** This system property is defined in ./src/plugin/build-plugin.xml */
  private final static String SAMPLE_DIR = System.getProperty("test.data",
      "build/parse-mspowerpoint/test/data");

  private final File sampleDir = new File(SAMPLE_DIR);

  /**
   * Wether dumping the extracted data to file for visual checks.
   */
  private final static boolean DUMP_TO_FILE = false;

  private final File testFile;

  private String urlString;

  private Protocol protocol;

  private Content content;

  /**
   * 
   * @param name
   */
  public TestMSPowerPointParser(String name) {
    super(name);
    this.testFile = new File(this.sampleDir, "test.ppt");
  }

  /**
   * @param file
   */
  public TestMSPowerPointParser(File file) {
    super();
    this.testFile = file;
  }

  /**
   * @see TestCase#setUp()
   */
  protected void setUp() throws Exception {
    super.setUp();

    this.urlString = createUrl(this.testFile.getName());

    System.out.println("Testing file: " + this.urlString + "...");
    this.protocol =new ProtocolFactory(NutchConfiguration.create()).getProtocol(this.urlString);
    this.content = this.protocol.getProtocolOutput(new Text(this.urlString), new CrawlDatum()).getContent();
  }

  /**
   * @see TestCase#tearDown()
   */
  protected void tearDown() throws Exception {
    super.tearDown();
  }

  /**
   * Testing all available ppt-docs stored in dir <code>SAMPLE_DIR</code> if
   * parsable without exceptions.
   * 
   * @see #SAMPLE_DIR
   * @throws Exception
   */
  public void testContent() throws Exception {

    Parse parse = new ParseUtil(NutchConfiguration.create())
                        .parseByExtensionId("parse-mspowerpoint", this.content)
                        .get(this.content.getUrl());

    ParseData data = parse.getData();
    String text = parse.getText();

    assertTrue("No content extracted length ==0", text.length() > 0);
    
    this.dumpToFile(this.testFile.getName(), data, text);

    final FileExtensionFilter contentFilter = new FileExtensionFilter(
        this.testFile.getName() + ".content");
    final File[] contentFiles = this.sampleDir.listFiles(contentFilter);

    if (contentFiles.length > 0) {
      String testContent = this.fileToString(contentFiles[0]);

      for (int i = 0; i < text.length(); i++) {
        char parsedChar = text.charAt(i);
        char testChar = testContent.charAt(i);
        assertEquals("Wrong char at position [" + i + "]", "" + testChar, ""
            + parsedChar);
      }
    } else {
      LOG.info("Comparison file for Content not available: "
          + this.testFile.getName() + ".content");
    }
  }

  /**
   * Testing all available ppt-docs stored in dir <code>SAMPLE_DIR</code> if
   * parsable without exceptions.
   * 
   * @see #SAMPLE_DIR
   * @throws Exception
   */
  public void testMeta() throws Exception {

    Parse parse = new ParseUtil(NutchConfiguration.create())
                        .parseByExtensionId("parse-mspowerpoint", content)
                        .get(content.getUrl());
    
    ParseData data = parse.getData();

    final FileExtensionFilter titleFilter = new FileExtensionFilter(
        this.testFile.getName() + ".meta");
    final File[] titleFiles = this.sampleDir.listFiles(titleFilter);

    if (titleFiles.length > 0) {
      assertEquals("Document Title", this.fileToString(titleFiles[0]),
          "Title: " + data.getTitle() + LINE_SEPARATOR +
          "Outlinks: " + data.getOutlinks().length + LINE_SEPARATOR);
    } else {
      assertTrue("Document Title length ==0", data.getTitle().length() > 0);
      LOG.info("Comparison file for Title not available: "
          + this.testFile.getName() + ".meta");
    }
  }

  /**
   * create complete url
   * 
   * @param fileName
   *          name of the file
   * @return complete url.
   */
  private String createUrl(final String fileName) {
    return "file:" + SAMPLE_DIR + "/" + fileName;
  }

  /**
   * Dump the parsed data to a UTF-8 formatted file for visual checks.
   * 
   * @param data
   * @param text
   * @param fileName
   * @throws IOException
   */
  private void dumpToFile(final String fileName, final ParseData data,
      final String text) throws IOException {
    if (TestMSPowerPointParser.DUMP_TO_FILE) {

      final File file = new File(fileName + ".txt");

      final FileOutputStream fos = new FileOutputStream(file);
      final OutputStreamWriter osw = new OutputStreamWriter(fos, CHARSET);

      osw.write(data.toString());
      osw.write(text);

      osw.close();
      fos.close();
    }
  }

  /**
   * Load the testfiles for comparison.
   * 
   * @param file
   *          file to load
   * @return UNF-8 encoded String content of file.
   * @throws IOException
   */
  private String fileToString(final File file) throws IOException {
    FileInputStream fis = null;
    //InputStreamReader isr = null;
    BufferedReader br = null;
    final StringBuffer buf = new StringBuffer();

    try {
      fis = new FileInputStream(file);
      br = new BufferedReader(new InputStreamReader(fis, CHARSET));

      String line = br.readLine();
      while (line != null) {
        buf.append(line).append(LINE_SEPARATOR);
        line = br.readLine();
      }
    } finally {
      if (br != null) {
        br.close();
      }
      if (fis != null) {
        fis.close();
      }
    }

    String val = buf.toString();

    return val;
  }

}
