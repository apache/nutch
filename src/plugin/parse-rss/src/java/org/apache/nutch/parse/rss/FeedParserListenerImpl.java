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

import org.apache.commons.feedparser.DefaultFeedParserListener;
import org.apache.commons.feedparser.FeedParserState;
import org.apache.commons.feedparser.FeedParserException;

import java.util.List;
import java.util.Vector;

import org.apache.nutch.parse.rss.structs.RSSChannel;
import org.apache.nutch.parse.rss.structs.RSSItem;

/**
 * 
 * @author mattmann
 * @version 1.0
 * 
 * <p>
 * Feed parser listener class which builds up an RSS Channel model that can be
 * iterated through to retrieve the parsed information.
 * </p>
 */
public class FeedParserListenerImpl extends DefaultFeedParserListener {

    private List fRssChannels = null;

    private RSSChannel fCurrentChannel = null;

    /**
     * <p>
     * Default Constructor
     * </p>
     */
    public FeedParserListenerImpl() {
        fRssChannels = new Vector();
    }

    /**
     * <p>
     * Gets a {@link List}of {@link RSSChannel}s that the listener parsed from
     * the RSS document.
     * </p>
     * 
     * @return A {@link List}of {@link RSSChannel}s.
     */
    public List getChannels() {
        if (fRssChannels.size() > 0) {
            return fRssChannels;
        } else {
            //there was only one channel found
            //add it here, then return it
            fRssChannels.add(fCurrentChannel);
            return fRssChannels;
        }
    }

    /**
     * <p>
     * Callback method when the parser encounters an RSS Channel.
     * </p>
     * 
     * @param state
     *            The current state of the FeedParser.
     * @param title
     *            The title of the RSS Channel.
     * @param link
     *            A hyperlink to the RSS Channel.
     * @param description
     *            The description of the RSS Channel.
     */
    public void onChannel(FeedParserState state, String title, String link,
            String description) throws FeedParserException {

        //capture the old channel if it's not null
        if (fCurrentChannel != null) {
            fRssChannels.add(fCurrentChannel);
        }

        //System.out.println("Found a new channel: " + title);

        fCurrentChannel = new RSSChannel(title, link, description);

    }

    /**
     * <p>
     * Callback method when the parser encounters an RSS Item.
     * </p>
     * 
     * @param state
     *            The current state of the FeedParser.
     * @param title
     *            The title of the RSS Item.
     * @param link
     *            A hyperlink to the RSS Item.
     * @param description
     *            The description of the RSS Item.
     * @param permalink
     *            A permanent link to the RSS Item.
     */
    public void onItem(FeedParserState state, String title, String link,
            String description, String permalink) throws FeedParserException {

        //System.out.println("Found a new published article: " + permalink);
        if (fCurrentChannel != null) { //should never be null
            fCurrentChannel.getItems().add(
                    new RSSItem(title, link, description, permalink));
        }

    }

}
