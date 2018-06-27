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
package org.apache.nutch.protocol.okhttp;

import java.lang.invoke.MethodHandles;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.SocketAddress;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;

import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.protocol.ProtocolException;
import org.apache.nutch.protocol.http.api.HttpBase;
import org.apache.nutch.util.NutchConfiguration;

import okhttp3.Connection;
import okhttp3.Headers;
import okhttp3.Interceptor;
import okhttp3.OkHttpClient;
import okhttp3.Request;

public class OkHttp extends HttpBase {

  protected static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  private final List<String[]> customRequestHeaders = new LinkedList<>();

  private OkHttpClient client;

  public OkHttp() {
    super(LOG);
  }

  public void setConf(Configuration conf) {
    super.setConf(conf);

    // protocols in order of preference
    List<okhttp3.Protocol> protocols = new ArrayList<>();
    if (useHttp2) {
      protocols.add(okhttp3.Protocol.HTTP_2);
    }
    protocols.add(okhttp3.Protocol.HTTP_1_1);

    okhttp3.OkHttpClient.Builder builder = new OkHttpClient.Builder()
        .protocols(protocols) //
        .retryOnConnectionFailure(true) //
        .followRedirects(false) //
        .connectTimeout(timeout, TimeUnit.MILLISECONDS)
        .writeTimeout(timeout, TimeUnit.MILLISECONDS)
        .readTimeout(timeout, TimeUnit.MILLISECONDS);

    if (!accept.isEmpty()) {
      getCustomRequestHeaders().add(new String[] { "Accept", accept });
    }

    if (!acceptLanguage.isEmpty()) {
      getCustomRequestHeaders()
          .add(new String[] { "Accept-Language", acceptLanguage });
    }

    if (!acceptCharset.isEmpty()) {
      getCustomRequestHeaders()
          .add(new String[] { "Accept-Charset", acceptCharset });
    }

    if (useProxy) {
      ProxySelector selector = new ProxySelector() {
        @SuppressWarnings("serial")
        private final List<Proxy> noProxy = new ArrayList<Proxy>() {
          {
            add(Proxy.NO_PROXY);
          }
        };
        @SuppressWarnings("serial")
        private final List<Proxy> proxy = new ArrayList<Proxy>() {
          {
            add(new Proxy(proxyType,
                new InetSocketAddress(proxyHost, proxyPort)));
          }
        };
        @Override
        public List<Proxy> select(URI uri) {
          if (useProxy(uri)) {
            return proxy;
          }
          return noProxy;
        }
        @Override
        public void connectFailed(URI uri, SocketAddress sa, IOException ioe) {
          LOG.error("Connection to proxy failed for {}: {}", uri, ioe);
        }
      };
      builder.proxySelector(selector);
    }

    if (storeIPAddress || storeHttpHeaders || storeHttpRequest) {
        builder.addNetworkInterceptor(new HTTPHeadersInterceptor());
    }

    client = builder.build();
  }

  class HTTPHeadersInterceptor implements Interceptor {

    @Override
    public okhttp3.Response intercept(Interceptor.Chain chain)
        throws IOException {

      Connection connection = chain.connection();
      String ipAddress = null;
      if (storeIPAddress) {
        InetAddress address = connection.socket().getInetAddress();
        ipAddress = address.getHostAddress();
      }

      Request request = chain.request();
      okhttp3.Response response = chain.proceed(request);
      String httpProtocol = response.protocol().toString()
          .toUpperCase(Locale.ROOT);
      if (useHttp2 && "H2".equals(httpProtocol)) {
        // back-warc compatible protocol name
        httpProtocol = "HTTP/2";
      }

      StringBuilder resquestverbatim = null;
      StringBuilder responseverbatim = null;

      if (storeHttpRequest) {
        resquestverbatim = new StringBuilder();

        resquestverbatim.append(request.method()).append(' ');
        resquestverbatim.append(request.url().encodedPath());
        String query = request.url().encodedQuery();
        if (query != null) {
          resquestverbatim.append('?').append(query);
        }
        resquestverbatim.append(' ').append(httpProtocol).append("\r\n");

        Headers headers = request.headers();

        for (int i = 0, size = headers.size(); i < size; i++) {
          String key = headers.name(i);
          String value = headers.value(i);
          resquestverbatim.append(key).append(": ").append(value)
              .append("\r\n");
        }

        resquestverbatim.append("\r\n");
      }

      if (storeHttpHeaders) {
        responseverbatim = new StringBuilder();

        responseverbatim.append(httpProtocol).append(' ')
            .append(response.code());
        if (!response.message().isEmpty()) {
          responseverbatim.append(' ').append(response.message());
        }
        responseverbatim.append("\r\n");

        Headers headers = response.headers();

        for (int i = 0, size = headers.size(); i < size; i++) {
          String key = headers.name(i);
          String value = headers.value(i);
          responseverbatim.append(key).append(": ").append(value)
              .append("\r\n");
        }

        responseverbatim.append("\r\n");
      }

      okhttp3.Response.Builder builder = response.newBuilder();

      if (ipAddress != null) {
        builder = builder.header(Response.IP_ADDRESS, ipAddress);
      }

      if (resquestverbatim != null) {
        byte[] encodedBytesRequest = Base64.getEncoder()
            .encode(resquestverbatim.toString().getBytes());
        builder = builder.header(Response.REQUEST,
            new String(encodedBytesRequest));
      }

      if (responseverbatim != null) {
        byte[] encodedBytesResponse = Base64.getEncoder()
            .encode(responseverbatim.toString().getBytes());
        builder = builder.header(Response.RESPONSE_HEADERS,
            new String(encodedBytesResponse));
      }

      // returns a modified version of the response
      return builder.build();
    }
  }

  protected List<String[]> getCustomRequestHeaders() {
    return customRequestHeaders;
  }

  protected OkHttpClient getClient() {
    return client;
  }

  protected Response getResponse(URL url, CrawlDatum datum, boolean redirect)
      throws ProtocolException, IOException {
    return new OkHttpResponse(this, url, datum);
  }

  public static void main(String[] args) throws Exception {
    OkHttp okhttp = new OkHttp();
    okhttp.setConf(NutchConfiguration.create());
    main(okhttp, args);
  }

}
