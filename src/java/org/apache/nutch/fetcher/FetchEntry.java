package org.apache.nutch.fetcher;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.nutch.storage.WebPage;
import org.gora.util.IOUtils;

public class FetchEntry extends Configured implements Writable {

  private String key;
  private WebPage page;

  public FetchEntry() {
    super(null);
  }

  public FetchEntry(Configuration conf, String key, WebPage page) {
    super(conf);
    this.key = key;
    this.page = page;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    key = Text.readString(in);
    page = IOUtils.deserialize(getConf(), in, null, WebPage.class);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Text.writeString(out, key);
    IOUtils.serialize(getConf(), out, page, WebPage.class);
  }

  public String getKey() {
    return key;
  }

  public WebPage getWebPage() {
    return page;
  }
}
