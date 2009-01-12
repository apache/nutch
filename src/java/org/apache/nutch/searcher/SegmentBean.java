package org.apache.nutch.searcher;

import java.io.IOException;

public interface SegmentBean extends HitContent, HitSummarizer {

  public String[] getSegmentNames() throws IOException;
}
