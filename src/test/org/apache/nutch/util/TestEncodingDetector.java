package org.apache.nutch.util;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import junit.framework.TestCase;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.storage.WebPage;

public class TestEncodingDetector extends TestCase {
  private static Configuration conf = NutchConfiguration.create();

  private static byte[] contentInOctets;

  static {
    try {
      contentInOctets = "çñôöøДЛжҶ".getBytes("utf-8");
    } catch (UnsupportedEncodingException e) {
      // not possible
    }
  }

  public TestEncodingDetector(String name) {
    super(name);
  }

  public void testGuessing() {
    // first disable auto detection
    conf.setInt(EncodingDetector.MIN_CONFIDENCE_KEY, -1);

    //Metadata metadata = new Metadata();
    EncodingDetector detector;
    // Content content;
    String encoding;

    WebPage page = new WebPage();
    page.setBaseUrl(new Utf8("http://www.example.com/"));
    page.setContentType(new Utf8("text/plain"));
    page.setContent(ByteBuffer.wrap(contentInOctets));

    detector = new EncodingDetector(conf);
    detector.autoDetectClues(page, true);
    encoding = detector.guessEncoding(page, "windows-1252");
    // no information is available, so it should return default encoding
    assertEquals("windows-1252", encoding.toLowerCase());

    page = new WebPage();
    page.setBaseUrl(new Utf8("http://www.example.com/"));
    page.setContentType(new Utf8("text/plain"));
    page.setContent(ByteBuffer.wrap(contentInOctets));
    page.putToHeaders(EncodingDetector.CONTENT_TYPE_UTF8, new Utf8("text/plain; charset=UTF-16"));
    
    detector = new EncodingDetector(conf);
    detector.autoDetectClues(page, true);
    encoding = detector.guessEncoding(page, "windows-1252");
    assertEquals("utf-16", encoding.toLowerCase());

    page = new WebPage();
    page.setBaseUrl(new Utf8("http://www.example.com/"));
    page.setContentType(new Utf8("text/plain"));
    page.setContent(ByteBuffer.wrap(contentInOctets));
    
    detector = new EncodingDetector(conf);
    detector.autoDetectClues(page, true);
    detector.addClue("windows-1254", "sniffed");
    encoding = detector.guessEncoding(page, "windows-1252");
    assertEquals("windows-1254", encoding.toLowerCase());

    // enable autodetection
    conf.setInt(EncodingDetector.MIN_CONFIDENCE_KEY, 50);
    page = new WebPage();
    page.setBaseUrl(new Utf8("http://www.example.com/"));
    page.setContentType(new Utf8("text/plain"));
    page.setContent(ByteBuffer.wrap(contentInOctets));
    page.putToMetadata(new Utf8(Response.CONTENT_TYPE), ByteBuffer.wrap("text/plain; charset=UTF-16".getBytes()));
    
    detector = new EncodingDetector(conf);
    detector.autoDetectClues(page, true);
    detector.addClue("utf-32", "sniffed");
    encoding = detector.guessEncoding(page, "windows-1252");
    assertEquals("utf-8", encoding.toLowerCase());
  }

}
