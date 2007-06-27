package org.apache.nutch.crawl;

import org.apache.hadoop.io.Writable;
import org.apache.nutch.util.GenericWritableConfigurable;

public class NutchWritable extends GenericWritableConfigurable {
  
  private static Class<? extends Writable>[] CLASSES = null;
  
  static {
    CLASSES = (Class<? extends Writable>[]) new Class[] {
      org.apache.hadoop.io.NullWritable.class, 
      org.apache.hadoop.io.LongWritable.class,
      org.apache.hadoop.io.BytesWritable.class,
      org.apache.hadoop.io.FloatWritable.class,
      org.apache.hadoop.io.IntWritable.class,
      org.apache.hadoop.io.Text.class,
      org.apache.hadoop.io.MD5Hash.class,
      org.apache.nutch.crawl.CrawlDatum.class,
      org.apache.nutch.crawl.Inlink.class,
      org.apache.nutch.crawl.Inlinks.class,
      org.apache.nutch.crawl.MapWritable.class,
      org.apache.nutch.fetcher.FetcherOutput.class,
      org.apache.nutch.metadata.Metadata.class,
      org.apache.nutch.parse.Outlink.class,
      org.apache.nutch.parse.ParseText.class,
      org.apache.nutch.parse.ParseData.class,
      org.apache.nutch.parse.ParseImpl.class,
      org.apache.nutch.parse.ParseStatus.class,
      org.apache.nutch.protocol.Content.class,
      org.apache.nutch.protocol.ProtocolStatus.class,
      org.apache.nutch.searcher.Hit.class,
      org.apache.nutch.searcher.HitDetails.class,
      org.apache.nutch.searcher.Hits.class
    };
  }

  public NutchWritable() { }
  
  public NutchWritable(Writable instance) {
    set(instance);
  }

  @Override
  protected Class<? extends Writable>[] getTypes() {
    return CLASSES;
  }

}
