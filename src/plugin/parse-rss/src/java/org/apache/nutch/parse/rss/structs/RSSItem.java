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

/**
 * 
 * <p>
 * Data class for holding RSS Items to send to Nutch's indexer
 * </p>
 * 
 * @author mattmann
 * @version 1.0
 */
public class RSSItem {

    //The title of this RSS Item
    private String fTitle = null;

    //The link that this RSS Item points to
    private String fLink = null;

    //The description of this RSS Item
    private String fDescription = null;

    //A permanent link that this RSS Item points to
    private String fPermalink = null;

    public RSSItem(String title, String link, String description,
            String permalink) {
        fTitle = title;
        fLink = link;
        fDescription = description;
        fPermalink = permalink;
    }

    /**
     * 
     * <P>
     * Get the title for this RSS Item
     * </p>
     * 
     * @return The title of this RSS Item
     */
    public String getTitle() {
        return fTitle;
    }

    /**
     * 
     * <p>
     * Gets the link that this RSS Item points to.
     * </p>
     * 
     * @return The link that this RSS Items points to.
     */
    public String getLink() {
        return fLink;
    }

    /**
     * 
     * <p>
     * Gets the Description of this RSS Item
     * </p>
     * 
     * @return The description of this RSS Item.
     */
    public String getDescription() {
        return fDescription;
    }

    /**
     * 
     * <p>
     * If this RSS Item points to a permanent link, then this method returns it.
     * </p>
     * 
     * @return The permanent link that this RSS Items points to.
     */
    public String getPermalink() {
        return fPermalink;
    }

    /**
     * 
     * <p>
     * Sets the title for this RSS Item.
     * </p>
     * 
     * @param title
     *            The title of this RSS Item
     */
    public void setTitle(String title) {
        fTitle = title;
    }

    /**
     * 
     * <p>
     * Sets the link that this RSS Item points to.
     * </p>
     * 
     * @param link
     *            The link that this RSS Item points to.
     */
    public void setLink(String link) {
        fTitle = link;
    }

    /**
     * 
     * <p>
     * Sets the description of this RSS Item.
     * </p>
     * 
     * @param description
     *            The description of this RSS Item.
     */
    public void setDescription(String description) {
        fDescription = description;
    }

    /**
     * 
     * <p>
     * Sets the permanent link that this RSS Item points to.
     * </p>
     * 
     * @param permalink
     *            The permanent link that this RSS Item points to
     */
    public void setPermalink(String permalink) {
        fPermalink = permalink;
    }

}
