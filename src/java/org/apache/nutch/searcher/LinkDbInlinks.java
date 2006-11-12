/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
/*
 * Created on Nov 23, 2005
 * Author: Andrzej Bialecki &lt;ab@getopt.org&gt;
 *
 */
package org.apache.nutch.searcher;

import java.io.IOException;

import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.crawl.LinkDbReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class LinkDbInlinks implements HitInlinks {
  private static final Log LOG = LogFactory.getLog(LinkDbInlinks.class);
  
  private LinkDbReader linkdb = null;
  
  public LinkDbInlinks(FileSystem fs, Path dir, Configuration conf) {
    try {
      linkdb = new LinkDbReader(conf, dir);
    } catch (Exception e) {
      LOG.warn("Could not create LinkDbReader: " + e);
    }
  }

  public String[] getAnchors(HitDetails details) throws IOException {
    return linkdb.getAnchors(new Text(details.getValue("url")));
  }

  public Inlinks getInlinks(HitDetails details) throws IOException {
    return linkdb.getInlinks(new Text(details.getValue("url")));
  }

  public void close() throws IOException {
    if (linkdb != null) { linkdb.close(); }
  }

}
