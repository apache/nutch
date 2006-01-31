/*
 * Created on Nov 23, 2005
 * Author: Andrzej Bialecki &lt;ab@getopt.org&gt;
 *
 */
package org.apache.nutch.searcher;

import java.io.IOException;

import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.crawl.LinkDbReader;
import org.apache.nutch.fs.NutchFileSystem;
import org.apache.nutch.io.UTF8;
import org.apache.nutch.util.NutchConf;

import java.io.File;

public class LinkDbInlinks implements HitInlinks {
  
  private LinkDbReader linkdb = null;
  
  public LinkDbInlinks(NutchFileSystem fs, File dir, NutchConf nutchConf) {
    linkdb = new LinkDbReader(fs, dir, nutchConf);
  }

  public String[] getAnchors(HitDetails details) throws IOException {
    return linkdb.getAnchors(new UTF8(details.getValue("url")));
  }

  public Inlinks getInlinks(HitDetails details) throws IOException {
    return linkdb.getInlinks(new UTF8(details.getValue("url")));
  }
}
