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

package org.apache.nutch.parse.mp3;

import org.apache.nutch.parse.Outlink;
import org.apache.hadoop.conf.Configuration;

import java.net.MalformedURLException;
import java.util.ArrayList;
import org.apache.nutch.metadata.Metadata;

/**
 * This class allows meta data to be collected and manipulated
 * @author Andy Hedges 
 */
public class MetadataCollector {

  private Metadata metadata = new Metadata();
  private String title = null;
  private String artist = null;
  private String album = null;
  private ArrayList links = new ArrayList();
  private String text = "";
  private Configuration conf;

  public MetadataCollector(Configuration conf) {
      this.conf = conf;
  }
  
  public void notifyProperty(String name, String value) throws MalformedURLException {
    if (name.equals("TIT2-Text"))
      setTitle(value);
    if (name.equals("TALB-Text"))
      setAlbum(value);
    if (name.equals("TPE1-Text"))
      setArtist(value);

    if (name.indexOf("URL Link") > -1) {
      links.add(new Outlink(value, "", this.conf));
    } else if (name.indexOf("Text") > -1) {
      text += value + "\n";
    }

    metadata.set(name, value);
  }

  public Metadata getData() {
    return metadata;
  }

  public Outlink[] getOutlinks() {
    return (Outlink[]) links.toArray(new Outlink[links.size()]);
  }

  public String getTitle() {
    String text = "";
    if (title != null) {
      text = title;
    }
    if (album != null) {
      if (!text.equals("")) {
        text += " - " + album;
      } else {
        text = title;
      }
    }
    if (artist != null) {
      if (!text.equals("")) {
        text += " - " + artist;
      } else {
        text = artist;
      }
    }
    return text;
  }

  public void setTitle(String title) {
    this.title = title;
  }

  public void setArtist(String artist) {
    this.artist = artist;
  }

  public void setAlbum(String album) {
    this.album = album;
  }

  public String getText() {
    return text;
  }

}
