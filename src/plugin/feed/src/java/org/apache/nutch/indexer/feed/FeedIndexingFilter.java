/**
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

package org.apache.nutch.indexer.feed;

//JDK imports
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

//APACHE imports
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.IndexingFilter;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.metadata.Feed;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.parse.Parse;
import org.apache.solr.common.util.DateUtil;

/**
 * @author dogacan
 * @author mattmann
 * @since NUTCH-444
 * 
 * An {@link IndexingFilter} implementation to pull out the
 * relevant extracted {@link Metadata} fields from the RSS feeds
 * and into the index.
 *
 */
public class FeedIndexingFilter implements IndexingFilter {
  
  public static final String dateFormatStr = "yyyyMMddHHmm";
  
  private Configuration conf;
  
  private final static String PUBLISHED_DATE = "publishedDate";
  
  private final static String UPDATED_DATE = "updatedDate";
  
  /**
   * Extracts out the relevant fields:
   * 
   * <ul>
   *  <li>FEED_AUTHOR</li>
   *  <li>FEED_TAGS</li>
   *  <li>FEED_PUBLISHED</li>
   *  <li>FEED_UPDATED</li>
   *  <li>FEED</li>
   * </ul>
   * 
   * And sends them to the {@link Indexer} for indexing within the Nutch
   * index.
   *  
   */
  public NutchDocument filter(NutchDocument doc, Parse parse, Text url, CrawlDatum datum,
                         Inlinks inlinks) throws IndexingException {
    ParseData parseData = parse.getData();
    Metadata parseMeta = parseData.getParseMeta();
    
    String[] authors = parseMeta.getValues(Feed.FEED_AUTHOR);
    String[] tags = parseMeta.getValues(Feed.FEED_TAGS);
    String published = parseMeta.get(Feed.FEED_PUBLISHED);
    String updated = parseMeta.get(Feed.FEED_UPDATED);
    String feed = parseMeta.get(Feed.FEED);
    
    if (authors != null) {
      for (String author : authors) {
        doc.add(Feed.FEED_AUTHOR, author);
      }
    }
    
    if (tags != null) {
      for (String tag : tags) {
        doc.add(Feed.FEED_TAGS, tag);
      }
    }
    
    if (feed != null)
      doc.add(Feed.FEED, feed);
    
    if (published != null) {
      Date date = new Date(Long.parseLong(published));
      String dateString =  DateUtil.getThreadLocalDateFormat().format(date);
      doc.add(PUBLISHED_DATE, dateString);
    }
    
    if (updated != null) {
      Date date = new Date(Long.parseLong(updated));
      String dateString = DateUtil.getThreadLocalDateFormat().format(date);
      doc.add(UPDATED_DATE, dateString);
    }
        
    return doc;
  }

  /**
   * @return the {@link Configuration} object used to configure
   * this {@link IndexingFilter}.
   */
  public Configuration getConf() {
    return conf;
  }

  /**
   * Sets the {@link Configuration} object used to configure this
   * {@link IndexingFilter}.
   * 
   * @param conf The {@link Configuration} object used to configure
   * this {@link IndexingFilter}.
   */
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

}
