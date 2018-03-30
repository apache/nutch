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
package org.apache.nutch.parse.sitemap;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;

import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import crawlercommons.sitemaps.*;
import crawlercommons.sitemaps.SiteMapURL.ChangeFrequency;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.SitemapParse;
import org.apache.nutch.parse.ParseStatusCodes;
import org.apache.nutch.parse.SitemapParser;

import org.apache.nutch.storage.ParseStatus;
import org.apache.nutch.storage.WebPage;

public class NutchSitemapParser implements SitemapParser {

  private Configuration conf;

  private static Collection<WebPage.Field> FIELDS = new HashSet<WebPage.Field>();

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  static {
    FIELDS.add(WebPage.Field.BASE_URL);
    FIELDS.add(WebPage.Field.SITEMAPS);
    FIELDS.add(WebPage.Field.MARKERS);
    FIELDS.add(WebPage.Field.STM_PRIORITY);
    FIELDS.add(WebPage.Field.MODIFIED_TIME);
    FIELDS.add(WebPage.Field.FETCH_INTERVAL);
    FIELDS.add(WebPage.Field.METADATA);
  }

  public SitemapParse getParse(String url, WebPage page) {
    SitemapParse nutchSitemapParse = null;
    SiteMapParser parser = new SiteMapParser();
    Map<Outlink, Metadata> outlinkMap = new HashMap<Outlink, Metadata>();

    AbstractSiteMap siteMap = null;
    String contentType = page.getContentType().toString();
    try {
      siteMap = parser
          .parseSiteMap(contentType, page.getContent().array(),
              new URL(url));
    } catch (UnknownFormatException e) {
      LOG.error(StringUtils.stringifyException(e));
    } catch (IOException e) {
      LOG.error(StringUtils.stringifyException(e));
    }
    
    if (siteMap.isIndex()) {
      Collection<AbstractSiteMap> links = ((SiteMapIndex) siteMap)
          .getSitemaps();
      for (AbstractSiteMap link : links) {
        Metadata metadata = new Metadata();
        metadata.add("sitemap", "true");
        try {
          outlinkMap.put(new Outlink(link.getUrl().toString(), "sitemap.outlink"), metadata);
        } catch (MalformedURLException e) {
          LOG.error(StringUtils.stringifyException(e));
        }
      }

    } else {
      Collection<SiteMapURL> links = ((SiteMap) siteMap).getSiteMapUrls();

      for (SiteMapURL sitemapUrl : links) {
        Metadata metadata = new Metadata();
        ChangeFrequency changeFrequency = sitemapUrl.getChangeFrequency();
        if (changeFrequency != null) {
          metadata.add("changeFrequency", sitemapUrl.getChangeFrequency().name());
        }
        Date lastModified = sitemapUrl.getLastModified();
        if (lastModified != null) {
          metadata.add("lastModified", Long.toString(sitemapUrl.getLastModified().getTime()));
        }
        metadata.add("priority", Double.toString(sitemapUrl.getPriority()));
        try {
          outlinkMap.put(new Outlink(sitemapUrl.getUrl().toString(), "sitemap.outlink"), metadata);
        } catch (MalformedURLException e) {
          LOG.error(StringUtils.stringifyException(e));
        }
      }
    }
    ParseStatus status = ParseStatus.newBuilder().build();
    status.setMajorCode((int) ParseStatusCodes.SUCCESS);
    nutchSitemapParse = new SitemapParse(outlinkMap, status);
    return nutchSitemapParse;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  public Configuration getConf() {
    return conf;
  }

  public Collection<WebPage.Field> getFields() {
    return FIELDS;
  }
}
