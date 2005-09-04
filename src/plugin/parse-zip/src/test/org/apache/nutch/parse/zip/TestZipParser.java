/*
 * TestZipParser.java
 */

package org.apache.nutch.parse.zip;

import org.apache.nutch.protocol.ProtocolFactory;
import org.apache.nutch.protocol.Protocol;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.ProtocolException;

import org.apache.nutch.parse.ParserFactory;
import org.apache.nutch.parse.Parser;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseException;

import junit.framework.TestCase;

/** 
 * Based on Unit tests for MSWordParser by John Xing
 *
 * @author Rohit Kulkarni & Ashish Vaidya
 */
public class TestZipParser extends TestCase {

  private String fileSeparator = System.getProperty("file.separator");
  // This system property is defined in ./src/plugin/build-plugin.xml
  private String sampleDir = System.getProperty("test.data",".");
  
  // Make sure sample files are copied to "test.data"
  
  private String[] sampleFiles = {"test.zip"};

  private String expectedText = "textfile.txt This is text file number 1 ";

  public TestZipParser(String name) { 
    super(name); 
  }

  protected void setUp() {}

  protected void tearDown() {}

  public void testIt() throws ProtocolException, ParseException {
    String urlString;
    Protocol protocol;
    Content content;
    Parser parser;
    Parse parse;

    for (int i = 0; i < sampleFiles.length; i++) {
      urlString = "file:" + sampleDir + fileSeparator + sampleFiles[i];

      protocol = ProtocolFactory.getProtocol(urlString);
      content = protocol.getProtocolOutput(urlString).getContent();

      parser = ParserFactory.getParser(content.getContentType(), urlString);
      parse = parser.getParse(content);
      assertTrue(parse.getText().equals(expectedText));
    }
  }

}
