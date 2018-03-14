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
package org.apache.nutch.indexwriter.solr;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;

import java.net.MalformedURLException;

public class SolrUtils {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  /**
   *
   *
   * @param job
   * @return SolrClient
   */
  public static ArrayList<SolrClient> getSolrClients(Configuration conf) throws MalformedURLException {
    String[] urls = conf.getStrings(SolrConstants.SERVER_URL);
    String[] zkHostString = conf.getStrings(SolrConstants.ZOOKEEPER_HOSTS);
    ArrayList<SolrClient> solrClients = new ArrayList<SolrClient>();
    
    if (zkHostString != null && zkHostString.length > 0) {
      for (int i = 0; i < zkHostString.length; i++) {
        CloudSolrClient sc = getCloudSolrClient(zkHostString[i]);
        sc.setDefaultCollection(conf.get(SolrConstants.COLLECTION));
        solrClients.add(sc);
      }
    } else {
      for (int i = 0; i < urls.length; i++) {
        SolrClient sc = new HttpSolrClient(urls[i]);
        solrClients.add(sc);
      }
    }

    return solrClients;
  }

  public static CloudSolrClient getCloudSolrClient(String url) throws MalformedURLException {
    CloudSolrClient sc = new CloudSolrClient(url.replace('|', ','));
    sc.setParallelUpdates(true);
    sc.connect();
    return sc;
  }

  public static SolrClient getHttpSolrClient(String url) throws MalformedURLException {
    SolrClient sc =new HttpSolrClient(url);
    return sc;
  }
  
  public static String stripNonCharCodepoints(String input) {
    StringBuilder retval = new StringBuilder();
    char ch;

    for (int i = 0; i < input.length(); i++) {
      ch = input.charAt(i);

      // Strip all non-characters
      // http://unicode.org/cldr/utility/list-unicodeset.jsp?a=[:Noncharacter_Code_Point=True:]
      // and non-printable control characters except tabulator, new line and
      // carriage return
      if (ch % 0x10000 != 0xffff && // 0xffff - 0x10ffff range step 0x10000
          ch % 0x10000 != 0xfffe && // 0xfffe - 0x10fffe range
          (ch <= 0xfdd0 || ch >= 0xfdef) && // 0xfdd0 - 0xfdef
          (ch > 0x1F || ch == 0x9 || ch == 0xa || ch == 0xd)) {

        retval.append(ch);
      }
    }

    return retval.toString();
  }

}
