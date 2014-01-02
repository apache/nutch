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
package org.apache.nutch.protocol.httpclient;

// JDK imports
import java.util.ArrayList;
import java.util.Collection;

// Slf4j Logging imports
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Hadoop imports
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configurable;

// Nutch imports
import org.apache.nutch.metadata.Metadata;


/**
 * Provides the Http protocol implementation
 * with the ability to authenticate when prompted.  The goal is to provide 
 * multiple authentication types but for now just the {@link HttpBasicAuthentication} authentication 
 * type is provided.
 *
 * @see HttpBasicAuthentication
 * @see Http
 * @see HttpResponse
 *
 * @author Matt Tencati
 */
public class HttpAuthenticationFactory implements Configurable {

  /**
   * The HTTP Authentication (WWW-Authenticate) header which is returned
   * by a webserver requiring authentication.
   */
  public static final String WWW_AUTHENTICATE = "WWW-Authenticate";

  public static final Logger LOG = LoggerFactory.getLogger(HttpAuthenticationFactory.class);

  private Configuration conf = null;

  public HttpAuthenticationFactory(Configuration conf) {
    setConf(conf);
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }
  public Configuration getConf() {
    return conf;
  }

  public HttpAuthentication findAuthentication(Metadata header) {

    if (header == null) return null;

    try {
      Collection<String> challenge = new ArrayList<String>();
      challenge.add(header.get(WWW_AUTHENTICATE));

      for(String challengeString: challenge) {
        if (challengeString.equals("NTLM"))
          challengeString="Basic realm=techweb";

        if (LOG.isTraceEnabled())
          LOG.trace("Checking challengeString=" + challengeString);

        HttpAuthentication auth = HttpBasicAuthentication.getAuthentication(challengeString, conf);
        if (auth != null) return auth;

        //TODO Add additional Authentication lookups here
      }
    } catch (Exception e) {
      LOG.error("Error: ", e);
    }
    return null;
  }
}
