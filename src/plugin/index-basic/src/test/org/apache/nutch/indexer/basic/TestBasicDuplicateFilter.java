package org.apache.nutch.indexer.basic;

import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.util.NutchConfiguration;

import static org.junit.Assert.*;

public class TestBasicDuplicateFilter {
  
  private static Configuration conf = NutchConfiguration.create();
  private static BasicDuplicateFilter filter = new BasicDuplicateFilter();
  private static List<CharSequence> duplicates;
  private static String[] urls;
  
  @BeforeClass
  public static void setup() {
    filter.setConf(conf);
    
    duplicates = new ArrayList<>();
    urls = new String[] {
      "http://localhost:8080/page/one",
      "http://localhost:8080/pages/page/page1.html",
      "http://localhost:8080/pages/page/page1.html?var=true",
      "http://localhost:8080/pages/1",
      "http://localhost:8080/pages/page/one",
    };
    for (String url : urls) {
      duplicates.add(url);
    }
  }
  
  @Test
  public void testShortestIsOriginal() throws Throwable {
    assertTrue(filter.isOriginal(urls[3], duplicates));
  }
  
  public void testLongerIsNotOriginal() throws Throwable {
    assertFalse(filter.isOriginal(urls[0], duplicates));
    assertFalse(filter.isOriginal(urls[1], duplicates));
    assertFalse(filter.isOriginal(urls[2], duplicates));
    assertFalse(filter.isOriginal(urls[4], duplicates));
  }
}
