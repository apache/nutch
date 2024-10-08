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
package org.apache.nutch.protocol.interactiveselenium;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.protocol.ProtocolException;
import org.apache.nutch.protocol.http.api.HttpBase;
import org.apache.nutch.util.NutchConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Http extends HttpBase {

  protected static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  public Http() {
    super(LOG);
  }

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
  }

  public static void main(String[] args) throws Exception {
    Http http = new Http();
    http.setConf(NutchConfiguration.create());
    main(http, args);
  }

  @Override
  protected Response getResponse(URL url, CrawlDatum datum, boolean redirect)
      throws ProtocolException, IOException {
    return new HttpResponse(this, url, datum);
  }

}
