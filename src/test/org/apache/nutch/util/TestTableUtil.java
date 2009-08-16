package org.apache.nutch.util;

import org.apache.nutch.util.hbase.TableUtil;
import junit.framework.TestCase;

public class TestTableUtil extends TestCase {

  String urlString1 = "http://foo.com/";
  String urlString2 = "http://foo.com:8900/";
  String urlString3 = "ftp://bar.baz.com/";
  String urlString4 = "http://bar.baz.com:8983/to/index.html?a=b&c=d";
  String urlString5 = "http://foo.com?a=/a/b&c=0";
  String urlString5rev = "http://foo.com/?a=/a/b&c=0";
  String urlString6 = "http://foo.com";

  String reversedUrlString1 = "com.foo:http/";
  String reversedUrlString2 = "com.foo:http:8900/";
  String reversedUrlString3 = "com.baz.bar:ftp/";
  String reversedUrlString4 = "com.baz.bar:http:8983/to/index.html?a=b&c=d";
  String reversedUrlString5 = "com.foo:http/?a=/a/b&c=0";
  String reversedUrlString6 = "com.foo:http";

  public void testReverseUrl() throws Exception {
    assertReverse(urlString1, reversedUrlString1);
    assertReverse(urlString2, reversedUrlString2);
    assertReverse(urlString3, reversedUrlString3);
    assertReverse(urlString4, reversedUrlString4); 
    assertReverse(urlString5, reversedUrlString5); 
    assertReverse(urlString5, reversedUrlString5); 
    assertReverse(urlString6, reversedUrlString6); 
  }

  public void testUnreverseUrl() throws Exception {
    assertUnreverse(reversedUrlString1, urlString1);
    assertUnreverse(reversedUrlString2, urlString2);
    assertUnreverse(reversedUrlString3, urlString3);
    assertUnreverse(reversedUrlString4, urlString4);
    assertUnreverse(reversedUrlString5, urlString5rev);
    assertUnreverse(reversedUrlString6, urlString6);
  }

  private static void assertReverse(String url, String expectedReversedUrl) throws Exception {
    String reversed = TableUtil.reverseUrl(url);
    assertEquals(reversed, expectedReversedUrl);
  }

  private static void assertUnreverse(String reversedUrl, String expectedUrl) {
    String unreversed = TableUtil.unreverseUrl(reversedUrl);
    assertEquals(unreversed, expectedUrl);
  }
}
