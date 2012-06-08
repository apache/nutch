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
package org.apache.nutch.protocol.http;

// JDK imports
import java.io.IOException;
import java.net.URL;
import java.util.Collection;
import java.util.HashSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.protocol.ProtocolException;
import org.apache.nutch.protocol.http.api.HttpBase;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.NutchConfiguration;

public class Http extends HttpBase {

  public static final Logger LOG = LoggerFactory.getLogger(Http.class);

  private static final Collection<WebPage.Field> FIELDS = new HashSet<WebPage.Field>();

  static {
    FIELDS.add(WebPage.Field.MODIFIED_TIME);
    FIELDS.add(WebPage.Field.HEADERS);
  }

  public Http() {
    super(LOG);
  }

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
//    Level logLevel = Level.WARNING;
//    if (conf.getBoolean("http.verbose", false)) {
//      logLevel = Level.FINE;
//    }
//    LOG.setLevel(logLevel);
  }

  public static void main(String[] args) throws Exception {
    Http http = new Http();
    http.setConf(NutchConfiguration.create());
    main(http, args);
  }

  @Override
  protected Response getResponse(URL url, WebPage page, boolean redirect)
    throws ProtocolException, IOException {
    return new HttpResponse(this, url, page);
  }

  public Collection<WebPage.Field> getFields() {
    return FIELDS;
  }

}
