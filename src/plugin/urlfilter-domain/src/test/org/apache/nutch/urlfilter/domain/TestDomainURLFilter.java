package org.apache.nutch.urlfilter.domain;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.util.NutchConfiguration;

public class TestDomainURLFilter
  extends TestCase {

  protected static final Log LOG = LogFactory.getLog(TestDomainURLFilter.class);

  private final static String SEPARATOR = System.getProperty("file.separator");
  private final static String SAMPLES = System.getProperty("test.data", ".");

  public TestDomainURLFilter(String testName) {
    super(testName);
  }

  public void testFilter()
    throws Exception {

    String domainFile = SAMPLES + SEPARATOR + "hosts.txt";
    Configuration conf = NutchConfiguration.create();
    DomainURLFilter domainFilter = new DomainURLFilter(domainFile);
    domainFilter.setConf(conf);
    assertNotNull(domainFilter.filter("http://lucene.apache.org"));
    assertNotNull(domainFilter.filter("http://hadoop.apache.org"));
    assertNotNull(domainFilter.filter("http://www.apache.org"));
    assertNull(domainFilter.filter("http://www.google.com"));
    assertNull(domainFilter.filter("http://mail.yahoo.com"));
    assertNotNull(domainFilter.filter("http://www.foobar.net"));
    assertNotNull(domainFilter.filter("http://www.foobas.net"));
    assertNotNull(domainFilter.filter("http://www.yahoo.com"));
    assertNotNull(domainFilter.filter("http://www.foobar.be"));
    assertNull(domainFilter.filter("http://www.adobe.com"));
  }

}
