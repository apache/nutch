/*
 *  TestMSExcelParser.java 
 *  Based on the Unit Tests for MSWordParser by John Xing
 */
package org.apache.nutch.parse.msexcel;

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
public class TestMSExcelParser extends TestCase {

  private String fileSeparator = System.getProperty("file.separator");
  // This system property is defined in ./src/plugin/build-plugin.xml
  private String sampleDir = System.getProperty("test.data",".");
  
  // Make sure sample files are copied to "test.data"
  
  private String[] sampleFiles = {"test.xls"};

  private String expectedText = "BitStream test.xls 321654.0 Apache incubator 1234.0 Doug Cutting 89078.0 CS 599 Search Engines Spring 2005.0 SBC 1234.0 764893.0 Java NUTCH!! ";

  public TestMSExcelParser(String name) { 
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
      content = protocol.getContent(urlString);

      parser = ParserFactory.getParser(content.getContentType(), urlString);
      parse = parser.getParse(content);

      assertTrue(parse.getText().equals(expectedText));
    }
  }

}
