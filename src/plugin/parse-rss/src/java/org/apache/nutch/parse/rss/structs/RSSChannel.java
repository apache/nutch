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
package org.apache.nutch.parse.rss.structs;

import java.util.List;
import java.util.Vector;

/**
 * 
 * <p>
 * Data class for holding RSS Channels to send to Nutch's indexer
 * </p>
 * 
 * @author mattmann
 * @version 1.0
 */
public class RSSChannel {

    //description of the channel
    private String fDescription = null;

    // link to the channel's page
    private String fLink = null;

    // title of the Channel
    private String fTitle = null;

    // set of items in the Channel
    private List fItems = null;

    /**
     * 
     * <p>
     * Default Constructor
     * </p>
     * 
     * @param desc
     *            The description of the channel.
     * @param link
     *            A link to the channel's url.
     * @param title
     *            The title of the channel.
     * @param items
     *            A list of {@link RSSItem}s for this RSS Channel.
     */
    public RSSChannel(String desc, String link, String title, List items) {
        fDescription = desc;
        fLink = link;
        fTitle = title;
        fItems = items;

    }

    /**
     * 
     * <p>
     * Constructor if you don't have the list of RSS Items ready yet.
     * </p>
     * 
     * @param desc
     *            The description of the channel.
     * @param link
     *            A link to the channel's url.
     * @param title
     *            The title of the channel.
     */
    public RSSChannel(String desc, String link, String title) {
        fDescription = desc;
        fLink = link;
        fTitle = title;
        fItems = new Vector();

    }

    /**
     * 
     * <p>
     * Get the list of items for this channel.
     * </p>
     * 
     * @return A list of {@link RSSItem}s.
     */
    public List getItems() {
        return fItems;
    }

    /**
     * 
     * <p>
     * Returns the channel title
     * </p>
     * 
     * @return The title of the channel.
     */

    public String getTitle() {
        return fTitle;
    }

    /**
     * 
     * <p>
     * Returns a link to the RSS Channel.
     * </p>
     * 
     * @return A {@link String}link to the RSS Channel.
     */
    public String getLink() {
        return fLink;
    }

    /**
     * 
     * <p>
     * Returns a {@link String}description of the RSS Channel.
     * </p>
     * 
     * @return The description of the RSS Channel.
     */
    public String getDescription() {
        return fDescription;
    }

    /**
     * 
     * <p>
     * Sets the list of RSS items for this channel.
     * </p>
     * 
     * @param items
     *            A List of {@link RSSItem}s for this RSSChannel.
     */
    public void setItems(List items) {
        fItems = items;
    }

    /**
     * 
     * <p>
     * Sets the Title for this RSS Channel.
     * </p>
     * 
     * @param title
     *            The title of this RSSChannel.
     */
    public void setTitle(String title) {
        fTitle = title;
    }

    /**
     * 
     * <p>
     * Sets the link to this RSSChannel
     * </p>
     * 
     * @param link
     *            A {@link String}representation of a link to this RSS Channel.
     */
    public void setLink(String link) {
        fLink = link;
    }

    /**
     * 
     * <p>
     * Sets the description of this RSSChannel
     * </p>
     * 
     * @param description
     *            A String description of this RSS Channel.
     */
    public void setDescription(String description) {
        fDescription = description;
    }
}
