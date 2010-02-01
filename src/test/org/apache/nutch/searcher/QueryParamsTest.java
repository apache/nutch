package org.apache.nutch.searcher;

import junit.framework.TestCase;

import org.apache.nutch.util.WritableTestUtils;

public class QueryParamsTest extends TestCase {

  public void testWritable() throws Exception {
    QueryParams context = new QueryParams(10, 2, "site", null, false);
    context.put("cat", "dog");
    WritableTestUtils.testWritable(context);
  }

}
