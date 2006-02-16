/*
 * Created on Nov 23, 2005
 * Author: Andrzej Bialecki &lt;ab@getopt.org&gt;
 *
 */
package org.apache.nutch.searcher;

import java.io.IOException;

import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.crawl.LinkDbReader;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.conf.Configuration;

import java.io.File;

public class LinkDbInlinks implements HitInlinks {
  
  private LinkDbReader linkdb = null;
  
  public LinkDbInlinks(FileSystem fs, File dir, Configuration conf) {
    linkdb = new LinkDbReader(fs, dir, conf);
  }

  public String[] getAnchors(HitDetails details) throws IOException {
    return linkdb.getAnchors(new UTF8(details.getValue("url")));
  }

  public Inlinks getInlinks(HitDetails details) throws IOException {
    return linkdb.getInlinks(new UTF8(details.getValue("url")));
  }

  public void close() throws IOException {
    if (linkdb != null) { linkdb.close(); }
  }

}
