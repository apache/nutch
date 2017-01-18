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
package org.apache.nutch.net.urlnormalizer.querystring;

import java.lang.invoke.MethodHandles;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.net.URLNormalizer;
import org.apache.nutch.plugin.Extension;
import org.apache.nutch.plugin.PluginRepository;
import org.apache.nutch.util.URLUtil;

/**
 * URL normalizer plugin for normalizing query strings but sorting query string
 * parameters. Not sorting query strings can lead to large amounts of duplicate
 * URL's such as ?a=x&b=y vs b=y&a=x.
 * 
 */
public class QuerystringURLNormalizer implements URLNormalizer {

  private Configuration conf;

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  public QuerystringURLNormalizer() {
  }

  public Configuration getConf() {
    return conf;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  public String normalize(String urlString, String scope)
      throws MalformedURLException {
    URL url = new URL(urlString);

    String queryString = url.getQuery();

    if (queryString == null) {
      return urlString;
    }

    List<String> queryStringParts = Arrays.asList(queryString.split("&"));
    Collections.sort(queryStringParts);

    StringBuilder sb = new StringBuilder();

    sb.append(url.getProtocol());
    sb.append("://");
    sb.append(url.getHost());
    if (url.getPort() > -1) {
      sb.append(":");
      sb.append(url.getPort());
    }
    sb.append(url.getPath());
    sb.append("?");
    sb.append(StringUtils.join(queryStringParts, "&"));
    if (url.getRef() != null) {
      sb.append("#");
      sb.append(url.getRef());
    }

    return sb.toString();
  }
}
