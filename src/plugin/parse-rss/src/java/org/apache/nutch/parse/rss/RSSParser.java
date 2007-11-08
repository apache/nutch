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

package org.apache.nutch.parse.rss;

// JDK imports
import java.io.ByteArrayInputStream;
import java.net.MalformedURLException;
import java.util.List;
import java.util.Vector;

// Commons Logging imports
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

// Hadoop imports
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;

// Nutch imports
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.parse.ParseResult;
import org.apache.nutch.parse.Parser;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseStatus;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseImpl;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.rss.structs.RSSItem;
import org.apache.nutch.parse.rss.structs.RSSChannel;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.Protocol;
import org.apache.nutch.protocol.ProtocolFactory;
import org.apache.nutch.util.LogUtil;
import org.apache.nutch.util.NutchConfiguration;

// RSS parsing imports
import org.apache.commons.feedparser.FeedParserListener;
import org.apache.commons.feedparser.FeedParser;
import org.apache.commons.feedparser.FeedParserFactory;


/**
 * 
 * @author mattmann
 * @version 1.0
 * 
 * <p>
 * RSS Parser class for nutch
 * </p>
 */
public class RSSParser implements Parser {
    public static final Log LOG = LogFactory.getLog("org.apache.nutch.parse.rss");
    private Configuration conf;

    /**
     * <p>
     * Implementation method, parses the RSS content, and then returns a
     * {@link ParseImpl}.
     * </p>
     * 
     * @param content
     *            The content to parse (hopefully an RSS content stream)
     * @return A {@link ParseImpl}which implements the {@link Parse}interface.
     */
    public ParseResult getParse(Content content) {

        List theRSSChannels = null;

        try {
            byte[] raw = content.getContent();

            // create a new FeedParser...
            FeedParser parser = FeedParserFactory.newFeedParser();

            // create a listener for handling our callbacks
            FeedParserListener listener = new FeedParserListenerImpl();

            // start parsing our feed and have the onItem methods called
            parser.parse(listener, new ByteArrayInputStream(raw), /* resource */
            null);

            theRSSChannels = ((FeedParserListenerImpl) listener).getChannels();

        } catch (Exception e) { // run time exception
            if (LOG.isWarnEnabled()) {
              e.printStackTrace(LogUtil.getWarnStream(LOG));
              LOG.warn("nutch:parse-rss:RSSParser Exception: " + e.getMessage());
            }
            return new ParseStatus(ParseStatus.FAILED,
                    "Can't be handled as rss document. " + e).getEmptyParseResult(content.getUrl(), getConf());
        }

        StringBuffer contentTitle = new StringBuffer(), indexText = new StringBuffer();
        List theOutlinks = new Vector();

        // for us, the contentTitle will be a concatenation of the titles of the
        // RSS Channels that we've parsed
        // and the index text will be a concatenation of the RSS Channel
        // descriptions, and descriptions of the RSS Items in the channel

        // also get the outlinks

        if (theRSSChannels != null) {
            for (int i = 0; i < theRSSChannels.size(); i++) {
                RSSChannel r = (RSSChannel) theRSSChannels.get(i);
                contentTitle.append(r.getTitle());
                contentTitle.append(" ");

                // concat the description to the index text
                indexText.append(r.getDescription());
                indexText.append(" ");

                if (r.getLink() != null) {
                    try {
                        // get the outlink
			if (r.getDescription()!= null ) {
			    theOutlinks.add(new Outlink(r.getLink(), r.getDescription()));
			} else {
			    theOutlinks.add(new Outlink(r.getLink(), ""));
			}
                    } catch (MalformedURLException e) {
                        if (LOG.isWarnEnabled()) {
                          LOG.warn("MalformedURL: " + r.getLink());
                          LOG.warn("Attempting to continue processing outlinks");
                          e.printStackTrace(LogUtil.getWarnStream(LOG));
                        }
                        continue;
                    }
                }

                // now get the descriptions of all the underlying RSS Items and
                // then index them too
                for (int j = 0; j < r.getItems().size(); j++) {
                    RSSItem theRSSItem = (RSSItem) r.getItems().get(j);
                    indexText.append(theRSSItem.getDescription());
                    indexText.append(" ");

                    String whichLink = null;

                    if (theRSSItem.getPermalink() != null)
                        whichLink = theRSSItem.getPermalink();
                    else
                        whichLink = theRSSItem.getLink();

                    if (whichLink != null) {
                        try {
			    if (theRSSItem.getDescription()!=null) {
				theOutlinks.add(new Outlink(whichLink, theRSSItem.getDescription()));
			    } else {
				theOutlinks.add(new Outlink(whichLink, ""));
			    }
                        } catch (MalformedURLException e) {
                            if (LOG.isWarnEnabled()) {
                              LOG.warn("MalformedURL: " + whichLink);
                              LOG.warn("Attempting to continue processing outlinks");
                              e.printStackTrace(LogUtil.getWarnStream(LOG));
                            }
                            continue;
                        }
                    }

                }

            }

            if (LOG.isTraceEnabled()) {
              LOG.trace("nutch:parse-rss:getParse:indexText=" + indexText);
              LOG.trace("nutch:parse-rss:getParse:contentTitle=" + contentTitle);
            }

        } else if (LOG.isTraceEnabled()) {
            LOG.trace("nutch:parse-rss:Error:getParse: No RSS Channels recorded!");
        }

        // format the outlinks
        Outlink[] outlinks = (Outlink[]) theOutlinks.toArray(new Outlink[theOutlinks.size()]);

        if (LOG.isTraceEnabled()) {
          LOG.trace("nutch:parse-rss:getParse:found " + outlinks.length + " outlinks");
        }
        // if (LOG.isInfoEnabled()) {
        //   LOG.info("Outlinks: "+outlinks);
        // }

        ParseData parseData = new ParseData(ParseStatus.STATUS_SUCCESS,
                contentTitle.toString(), outlinks, content.getMetadata());
        return ParseResult.createParseResult(content.getUrl(), new ParseImpl(indexText.toString(), parseData));
    }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  public Configuration getConf() {
    return this.conf;
  }
  
  public static void main(String[] args) throws Exception {
    //LOG.setLevel(Level.FINE);
    String url = args[0];
    Configuration conf = NutchConfiguration.create();
    RSSParser parser = new RSSParser();
    parser.setConf(conf);
    Protocol protocol = new ProtocolFactory(conf).getProtocol(url);
    Content content = protocol.getProtocolOutput(new Text(url), new CrawlDatum()).getContent();
    Parse parse = parser.getParse(content).get(content.getUrl());
    System.out.println("data: "+ parse.getData());
    System.out.println("text: "+parse.getText());
  }
  

}
