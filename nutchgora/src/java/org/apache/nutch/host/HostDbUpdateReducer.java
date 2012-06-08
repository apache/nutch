/*******************************************************************************
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
 ******************************************************************************/
package org.apache.nutch.host;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;

import org.apache.avro.util.Utf8;
import org.apache.gora.mapreduce.GoraReducer;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlStatus;
import org.apache.nutch.storage.Host;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.Histogram;
import org.apache.nutch.util.URLUtil;

/**
 * Combines all WebPages with the same host key to create a Host object, 
 * with some statistics.
 */
public class HostDbUpdateReducer extends GoraReducer<Text, WebPage, String, Host> {
  
  @Override
  protected void reduce(Text key, Iterable<WebPage> values, Context context)
    throws IOException, InterruptedException {
    
    int numPages = 0;
    int numFetched = 0;
    boolean buildLinkDb = true;
    
    Histogram<String> inlinkCount = new Histogram<String>();
    Histogram<String> outlinkCount = new Histogram<String>();
    
    for (WebPage page: values) {
      // count number of pages
      numPages++;     
      // count number of fetched pages
      if (page.getStatus() == CrawlStatus.STATUS_FETCHED) {
        numFetched++;
      }
      
      // build host link db
      // TODO: limit number of links
      if (buildLinkDb) {
        if (page.getInlinks() != null) {
          Set<Utf8> inlinks = page.getInlinks().keySet();
          for (Utf8 inlink: inlinks) {
            String host = URLUtil.getHost(inlink.toString());
            inlinkCount.add(host);
          }
        }
        if (page.getOutlinks() != null) {
          Set<Utf8> outlinks = page.getOutlinks().keySet();
          for (Utf8 outlink: outlinks) {
            String host = URLUtil.getHost(outlink.toString());
            outlinkCount.add(host);
          }
        }
      }
    }
    
    // output host data
    Host host = new Host();
    host.putToMetadata(new Utf8("p"),ByteBuffer.wrap(Integer.toString(numPages).getBytes()));
    if (numFetched > 0) {
      host.putToMetadata(new Utf8("f"),ByteBuffer.wrap(Integer.toString(numFetched).getBytes())); 
    }
    for (String inlink: inlinkCount.getKeys()) {
      host.putToInlinks(new Utf8(inlink), new Utf8(Integer.toString(inlinkCount.getCount(inlink))));
    }
    for (String outlink: outlinkCount.getKeys()) {
      host.putToOutlinks(new Utf8(outlink), new Utf8(Integer.toString(outlinkCount.getCount(outlink))));
    }
    
    context.write(key.toString(), host);
  }
}
