/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.apache.nutch.protocol.Content;
import org.apache.hadoop.util.LogFormatter;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.parse.Parser;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseStatus;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseImpl;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.ParseException;

import org.apache.nutch.parse.rss.structs.RSSItem;
import org.apache.nutch.parse.rss.structs.RSSChannel;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.IOException;

import java.net.MalformedURLException;

import java.util.Properties;
import java.util.logging.Logger;
import java.util.Date;
import java.util.List;
import java.util.Vector;

// add all the RSS parsing imports right here
import org.apache.commons.feedparser.FeedParserState;
import org.apache.commons.feedparser.FeedParserListener;
import org.apache.commons.feedparser.FeedParser;
import org.apache.commons.feedparser.FeedParserException;
import org.apache.commons.feedparser.FeedParserFactory;
import org.apache.commons.feedparser.DefaultFeedParserListener;
import org.apache.commons.feedparser.network.ResourceRequestFactory;
import org.apache.commons.feedparser.network.ResourceRequest;

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
    public static final Logger LOG = LogFormatter
            .getLogger("org.apache.nutch.parse.rss");
    private Configuration conf;

    /**
     * <p>
     * Default Constructor
     * </p>
     */
    public RSSParser() {

        // redirect org.apache.log4j.Logger to java's native logger, in order
        // to, at least, suppress annoying log4j warnings.
        // Note on 20040614 by Xing:
        // log4j is used by pdfbox. This snippet'd better be moved
        // to a common place shared by all parsers that use log4j.
        org.apache.log4j.Logger rootLogger = org.apache.log4j.Logger
                .getRootLogger();

        rootLogger.setLevel(org.apache.log4j.Level.INFO);

        org.apache.log4j.Appender appender = new org.apache.log4j.WriterAppender(
                new org.apache.log4j.SimpleLayout(),
                org.apache.hadoop.util.LogFormatter.getLogStream(this.LOG,
                        java.util.logging.Level.INFO));

        rootLogger.addAppender(appender);
    }

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
    public Parse getParse(Content content) {

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
            e.printStackTrace();
            LOG.fine("nutch:parse-rss:RSSParser Exception: " + e.getMessage());
            return new ParseStatus(ParseStatus.FAILED,
                    "Can't be handled as rss document. " + e).getEmptyParse(getConf());
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
			    theOutlinks.add(new Outlink(r.getLink(), r.getDescription(), getConf()));
			} else {
			    theOutlinks.add(new Outlink(r.getLink(), "", getConf()));
			}
                    } catch (MalformedURLException e) {
                        LOG.info("nutch:parse-rss:RSSParser Exception: MalformedURL: "
                                        + r.getLink()
                                        + ": Attempting to continue processing outlinks");
                        e.printStackTrace();
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
				theOutlinks.add(new Outlink(whichLink, theRSSItem.getDescription(), getConf()));
			    } else {
				theOutlinks.add(new Outlink(whichLink, "", getConf()));
			    }
                        } catch (MalformedURLException e) {
                            LOG.info("nutch:parse-rss:RSSParser Exception: MalformedURL: "
                                            + whichLink
                                            + ": Attempting to continue processing outlinks");
                            e.printStackTrace();
                            continue;
                        }
                    }

                }

            }

            LOG.fine("nutch:parse-rss:getParse:indexText=" + indexText);
            LOG.fine("nutch:parse-rss:getParse:contentTitle=" + contentTitle);

        } else {
            LOG.fine("nutch:parse-rss:Error:getParse: No RSS Channels recorded!");
        }

        // format the outlinks
        Outlink[] outlinks = (Outlink[]) theOutlinks.toArray(new Outlink[theOutlinks.size()]);

        LOG.fine("nutch:parse-rss:getParse:found " + outlinks.length + " outlinks");
        // LOG.info("Outlinks: "+outlinks);

        ParseData parseData = new ParseData(ParseStatus.STATUS_SUCCESS,
                contentTitle.toString(), outlinks, content.getMetadata());
        parseData.setConf(this.conf);
        return new ParseImpl(indexText.toString(), parseData);
    }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  public Configuration getConf() {
    return this.conf;
  }

}
