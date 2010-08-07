package org.apache.nutch.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Writable;
import org.apache.nutch.storage.WebPage;
import org.gora.util.IOUtils;

public class WebPageWritable extends Configured
implements Writable {

  private WebPage webPage;

  public WebPageWritable() {
    this(null, new WebPage());
  }

  public WebPageWritable(Configuration conf, WebPage webPage) {
    super(conf);
    this.webPage = webPage;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    webPage = IOUtils.deserialize(getConf(), in, webPage, WebPage.class);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    IOUtils.serialize(getConf(), out, webPage, WebPage.class);
  }

  public WebPage getWebPage() {
    return webPage;
  }

}
