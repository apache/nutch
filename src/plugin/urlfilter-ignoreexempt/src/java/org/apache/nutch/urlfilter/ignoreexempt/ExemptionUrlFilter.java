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
package org.apache.nutch.urlfilter.ignoreexempt;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.http.Header;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.nutch.metadata.HttpHeaders;
import org.apache.nutch.net.URLExemptionFilter;
import org.apache.nutch.util.NutchConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.regex.Pattern;
import java.util.*;


/**
 * This implementation of {@link org.apache.nutch.net.URLExemptionFilter} checks uses regex and mime-type configuration
 * to check if URL is eligible for exemption from 'db.ignore.external'.
 * When this filter is enabled, urls will be checked against configured sequence of regex and mimetype rules.
 *<p>
 * The exemption rule file defaults to db-ignore-external-exemptions.txt in the classpath but can be
 * overridden using the property  <code>"db.ignore.external.exemptions.file" in ./conf/nutch-*.xml</code>
 *</p>
 *
 * The exemption rules are specified in plain text file where each line is a rule.
 * <br/><code>MimeType1,MimeType2,MimeTypeN=UrlRegex</code><br/>
 * The first occurance of '=' from left divides comma separated mime-types with the url regex.
 * When the url matches regex and content type is present in the specified list,
 * then url is exempted from 'db.ignore.external...'<br/>
 * <h3>Examples:</h3>
 * <ol>
 *   <li>
 *     <b>Exempt urls ending with .jpg or .png and have content type image/jpeg or image/png </b>
 *      <br/><code>image/jpeg,image/png=.*\.jpg$|.*\.JPG$|.*\.png$|.*\.PNG$</code><br/><br/>
 *   </li>
 *   <li>
 *  <b> Exempt all urls ending with gif, without looking for mimetypes.</b>
 *    <br/><code>=.*\.gif$</code><br/>
 *    <i>Note : Mimes are empty => accept any mimetype</i>
 *    <br/>
 *   </li>
 *   <li>
 *      <b>Exempt all urls having mimetype image/jpeg or image/png.</b><br/>
 *      <br/><code>image/jpeg,image/png=.*</code><br/>
 *       <i>Note : .* regex matches all urls</i>
 *       <br/>
 *   </li>
 * </ol>
 </pre>
 *
 * @author Thamme Gowda N
 * @since October 5, 2015
 * @version 1
 * @see URLExemptionFilter
 */
public class ExemptionUrlFilter implements URLExemptionFilter {

  public static final String DB_IGNORE_EXTERNAL_EXEMPTIONS_FILE = "db.ignore.external.exemptions.file";
  private static final Logger LOG = LoggerFactory.getLogger(ExemptionUrlFilter.class);
  private static ExemptionUrlFilter INSTANCE;

  private LinkedHashMap<Pattern, Set<String>> exemptions; //preserves  insertion order
  private Configuration conf;
  private HttpClientBuilder clientBuilder;
  private boolean enabled;

  public static ExemptionUrlFilter getInstance() {
    if(INSTANCE == null) {
      synchronized (ExemptionUrlFilter.class) {
        if (INSTANCE == null) {
          INSTANCE = new ExemptionUrlFilter();
          INSTANCE.setConf(NutchConfiguration.create());
        }
      }
    }
    return INSTANCE;
  }

  public boolean isEnabled() {
    return enabled;
  }

  public LinkedHashMap<Pattern, Set<String>> getExemptions() {
    return exemptions;
  }

  private String getContentType(String urlString){
    //FIXME : Do this in nutch way, use Fetcher Queues and Protocols
    CloseableHttpClient client = null;
    CloseableHttpResponse response = null;
    try {
      client = clientBuilder.build();
      HttpHead httpHead = new HttpHead(urlString);
      response = client.execute(httpHead);
      Header cTypeHeader = response.getFirstHeader(HttpHeaders.CONTENT_TYPE);
      if (cTypeHeader != null) {
        ContentType contentType = ContentType.parse(cTypeHeader.getValue());
        LOG.debug("{} MimeType={}", urlString, contentType.getMimeType());
        return contentType.getMimeType();
      }
    } catch (Exception e) {
      LOG.debug("{} while trying to HTTP HEAD on {}", e.getMessage(), urlString);
    } finally {
      IOUtils.closeQuietly(response);
      IOUtils.closeQuietly(client);
    }
    // couldn't get mime type
    return null;
  }

  @Override
  public boolean filter(String fromUrl, String toUrl) {
    //this implementation doesnt do anything with fromUrl
    if (exemptions != null) {
      String mimeType = null;
      for (Pattern pattern : exemptions.keySet()) {
        //condition 1: regex should match
        if (pattern.matcher(toUrl).matches()) {
          Set<String> mimes = exemptions.get(pattern);
          //condition 2a) Dont care mimes
          if (mimes.isEmpty()) {
            //when mimes are empty, it means don't care
            return true; //exempted
          }
          if (mimeType == null) {
            //get it lazily, only once in this loop
            mimeType = getContentType(toUrl);
          }
          //condition 2b) mime type also matches
          if (mimes.contains(mimeType)) {
            // exempted
            return true;
          }
        }
      }
    }
    //not exempted
    return false;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    LOG.info("Ignore exemptions enabled");
    RequestConfig requestConfig = RequestConfig.custom()
        .setConnectTimeout(conf.getInt("http.timeout", 10 * 1000))
        .build();
    this.clientBuilder = HttpClientBuilder.create()
        .setDefaultRequestConfig(requestConfig)
        .setUserAgent(conf.get("http.agent.name"));

    String fileName = this.conf.get(DB_IGNORE_EXTERNAL_EXEMPTIONS_FILE);
    InputStream stream = this.conf.getConfResourceAsInputStream(fileName);
    if (stream == null) {
      throw new RuntimeException("Couldn't find config file :" + fileName);
    }
    try {
      this.exemptions = new LinkedHashMap<Pattern, Set<String>>();
      List<String> lines = IOUtils.readLines(stream);
      for (String line : lines) {
        line = line.trim();
        if (line.startsWith("#") || line.isEmpty()) {
          continue; //Skip : comment line or empty line
        }
        int firstIndex = line.indexOf('=');
        if (firstIndex == -1) {
          // No Split! Invalid
          LOG.error("{} : Invalid Config  :: {}", fileName, line);
          continue;
        }
        String mimeString = line.substring(0, firstIndex).trim();
        String regex = line.substring(firstIndex + 1, line.length()).trim();
        if (regex.isEmpty()) {
          LOG.error("{} : Invalid Config  :: {}", fileName, line);
          continue;
        }
        //NOTE:empty mime string means don't care => */*
        HashSet<String> mimes = new HashSet<String>();
        if (!mimes.isEmpty()) {
          Arrays.asList(mimeString.split(","));
        }
        Pattern compiled = Pattern.compile(regex);
        LOG.info("URL rule :: {} <=> {}", regex, mimes);
        exemptions.put(compiled, mimes);
      }
      LOG.info("Read {} rules from {}", exemptions.size(), fileName);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public Configuration getConf() {
    return this.conf;
  }

  public static void main(String[] args) {
    if (args.length != 1) {
      System.out.println("Error: Invalid Args");
      System.out.println("Usage:" + ExemptionUrlFilter.class.getName() + "<url>");
      return;
    }
    String url = args[0];
    System.out.println(ExemptionUrlFilter.getInstance().filter(null, url));
  }
}
