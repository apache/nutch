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

package org.apache.nutch.tools;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.util.URLUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

/**
 * This class provides methods to map crawled data on JSON using Jackson Streaming APIs. 
 *
 */
public class CommonCrawlFormatJackson extends AbstractCommonCrawlFormat {

  private static final Logger LOG = LoggerFactory.getLogger(CommonCrawlFormatJackson.class.getName());

  public CommonCrawlFormatJackson(String url, byte[] content,
      Metadata metadata, Configuration conf) {
    super(url, content, metadata, conf);
  }

  @Override
  protected String getJsonDataAll() throws IOException {
    JsonFactory factory = new JsonFactory();

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    JsonGenerator generator = null;

    try {
      generator = factory.createGenerator(out);
      generator.useDefaultPrettyPrinter(); // INDENTED OUTPUT

      generator.writeStartObject();

      // url
      generator.writeFieldName("url");
      generator.writeString(url);

      // timestamp
      generator.writeFieldName("timestamp");
      generator.writeString(metadata.get(Metadata.LAST_MODIFIED));


      //request
      generator.writeFieldName("request");
      generator.writeStartObject();
      generator.writeFieldName("method");
      generator.writeString("GET"); 
      generator.writeFieldName("client");
      generator.writeStartObject();
      generator.writeFieldName("hostname");
      generator.writeString(getHostName());
      generator.writeFieldName("address");
      generator.writeString(getHostAddress());
      generator.writeFieldName("software");
      generator.writeString(conf.get("http.agent.version", ""));
      generator.writeFieldName("robots");
      generator.writeString("classic");
      generator.writeFieldName("contact");
      generator.writeStartObject();
      generator.writeFieldName("name");
      generator.writeString(conf.get("http.agent.name", ""));
      generator.writeFieldName("email");
      generator.writeString(conf.get("http.agent.email", ""));
      generator.writeEndObject();
      generator.writeFieldName("headers");
      generator.writeStartObject();
      generator.writeFieldName("Accept");
      generator.writeString(conf.get("accept", ""));
      generator.writeFieldName("Accept-Encoding");
      generator.writeString(""); // TODO
      generator.writeFieldName("Accept-Language");
      generator.writeString(conf.get("http.accept.language", ""));
      generator.writeFieldName("User-Agent");
      generator.writeString(conf.get("http.robots.agents", ""));
      generator.writeEndObject();
      generator.writeFieldName("body");
      generator.writeNull();
      generator.writeEndObject();

      //response
      generator.writeFieldName("response");
      generator.writeStartObject();
      generator.writeFieldName("status");
      generator.writeString(ifNullString(metadata.get("status")));
      generator.writeFieldName("server");

      generator.writeStartObject();
      generator.writeFieldName("hostname");
      generator.writeString(URLUtil.getHost(url)); 
      generator.writeFieldName("address");
      generator.writeString(ifNullString(metadata.get("_ip_")));
      generator.writeEndObject();

      generator.writeFieldName("headers");
      generator.writeStartObject();
      for (String name : metadata.names()) {
        generator.writeFieldName(name);
        generator.writeString(ifNullString(metadata.get(name)));
      }
      generator.writeEndObject();

      generator.writeFieldName("body");
      generator.writeString(new String(content));
      generator.writeEndObject();

      generator.writeFieldName("key"); 
      generator.writeString(url);

      generator.writeFieldName("imported"); // TODO
      generator.writeString("");

      generator.writeEndObject();

      generator.flush();

      return out.toString();

    } catch (IOException ioe) {
      LOG.warn("Error in processing file " + url + ": " + ioe.getMessage());
      throw new IOException("Error in generating JSON using Jackson:" + ioe.getMessage()); 
    }
  }

  @Override
  protected String getJsonDataSet() throws IOException {
    JsonFactory factory = new JsonFactory();

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    JsonGenerator generator = null;

    try {
      generator = factory.createGenerator(out);
      generator.useDefaultPrettyPrinter(); // INDENTED OUTPUT

      generator.writeStartObject();

      // url
      generator.writeFieldName("url");
      generator.writeString(url);

      // timestamp
      generator.writeFieldName("timestamp");
      generator.writeString(metadata.get(Metadata.LAST_MODIFIED)); 

      //request
      generator.writeFieldName("request");
      generator.writeStartObject();
      generator.writeFieldName("method");
      generator.writeString("GET");
      generator.writeFieldName("client");
      generator.writeStartObject();
      generator.writeFieldName("hostname");
      generator.writeString(getHostName());
      generator.writeFieldName("address");
      generator.writeString(getHostAddress());
      generator.writeFieldName("software");
      generator.writeString(conf.get("http.agent.version", ""));
      generator.writeFieldName("robots");
      generator.writeString("CLASSIC"); 
      generator.writeFieldName("contact");
      generator.writeStartObject();
      generator.writeFieldName("name");
      generator.writeString(conf.get("http.agent.name", ""));
      generator.writeFieldName("email");
      generator.writeString(conf.get("http.agent.email", ""));
      generator.writeEndObject();
      generator.writeFieldName("headers");
      generator.writeStartObject();
      generator.writeFieldName("Accept");
      generator.writeString(conf.get("accept", ""));
      generator.writeFieldName("Accept-Encoding");
      generator.writeString(""); // TODO
      generator.writeFieldName("Accept-Language");
      generator.writeString(conf.get("http.accept.language", ""));
      generator.writeFieldName("User-Agent");
      generator.writeString(conf.get("http.robots.agents", ""));
      generator.writeEndObject();
      generator.writeFieldName("body");
      generator.writeNull();
      generator.writeEndObject();

      //response
      generator.writeFieldName("response");
      generator.writeStartObject();
      generator.writeFieldName("status");
      generator.writeString(ifNullString(metadata.get("status")));
      generator.writeFieldName("server");

      generator.writeStartObject();
      generator.writeFieldName("hostname");
      generator.writeString(URLUtil.getHost(url)); 
      generator.writeFieldName("address");
      generator.writeString(ifNullString(metadata.get("_ip_")));
      generator.writeEndObject();

      generator.writeFieldName("headers");
      generator.writeStartObject();
      generator.writeFieldName("Content-Encoding");
      generator.writeString(ifNullString(metadata.get("Content-Encoding")));
      generator.writeFieldName("Content-Type");
      generator.writeString(ifNullString(metadata.get("Content-Type")));
      generator.writeFieldName("Date");
      generator.writeString(ifNullString(metadata.get("Date")));
      generator.writeFieldName("Server");
      generator.writeString(ifNullString(metadata.get("Server")));
      generator.writeEndObject();

      generator.writeFieldName("body");
      generator.writeString(new String(content));
      generator.writeEndObject();

      generator.writeFieldName("key");
      generator.writeString(url);

      generator.writeFieldName("imported"); // TODO
      generator.writeString("");

      generator.writeEndObject();

      generator.flush();

      return out.toString();

    } catch (IOException ioe) {
      LOG.warn("Error in processing file " + url + ": " + ioe.getMessage());
      throw new IOException("Error in generating JSON using Jackson:" + ioe.getMessage()); 
    }
  }
}
