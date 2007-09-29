package org.apache.nutch.util;

import java.io.UnsupportedEncodingException;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.protocol.Content;

import junit.framework.TestCase;

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

    Metadata metadata = new Metadata();
    EncodingDetector detector;
    Content content;
    String encoding;

    content = new Content("http://www.example.com", "http://www.example.com/",
        contentInOctets, "text/plain", metadata, conf);
    detector = new EncodingDetector(conf);
    detector.autoDetectClues(content, true);
    encoding = detector.guessEncoding(content, "windows-1252");
    // no information is available, so it should return default encoding
    assertEquals("windows-1252", encoding.toLowerCase());

    metadata.clear();
    metadata.set(Response.CONTENT_TYPE, "text/plain; charset=UTF-16");
    content = new Content("http://www.example.com", "http://www.example.com/",
        contentInOctets, "text/plain", metadata, conf);
    detector = new EncodingDetector(conf);
    detector.autoDetectClues(content, true);
    encoding = detector.guessEncoding(content, "windows-1252");
    assertEquals("utf-16", encoding.toLowerCase());

    metadata.clear();
    content = new Content("http://www.example.com", "http://www.example.com/",
        contentInOctets, "text/plain", metadata, conf);
    detector = new EncodingDetector(conf);
    detector.autoDetectClues(content, true);
    detector.addClue("windows-1254", "sniffed");
    encoding = detector.guessEncoding(content, "windows-1252");
    assertEquals("windows-1254", encoding.toLowerCase());

    // enable autodetection
    conf.setInt(EncodingDetector.MIN_CONFIDENCE_KEY, 50);
    metadata.clear();
    metadata.set(Response.CONTENT_TYPE, "text/plain; charset=UTF-16");
    content = new Content("http://www.example.com", "http://www.example.com/",
        contentInOctets, "text/plain", metadata, conf);
    detector = new EncodingDetector(conf);
    detector.autoDetectClues(content, true);
    detector.addClue("utf-32", "sniffed");
    encoding = detector.guessEncoding(content, "windows-1252");
    assertEquals("utf-8", encoding.toLowerCase());
  }

}
