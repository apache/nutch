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
package org.apache.nutch.protocol.http.api;

import java.lang.invoke.MethodHandles;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.net.Proxy;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.Protocol;
import org.apache.nutch.protocol.ProtocolException;
import org.apache.nutch.protocol.ProtocolOutput;
import org.apache.nutch.protocol.ProtocolStatus;
import org.apache.nutch.util.GZIPUtils;
import org.apache.nutch.util.MimeUtil;
import org.apache.nutch.util.DeflateUtils;
import org.apache.nutch.util.URLUtil;
import org.apache.hadoop.util.StringUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import crawlercommons.robots.BaseRobotRules;

public abstract class HttpBase implements Protocol {

  public static final Text RESPONSE_TIME = new Text("_rs_");

  public static final Text COOKIE = new Text("Cookie");
  
  public static final int BUFFER_SIZE = 8 * 1024;

  private static final byte[] EMPTY_CONTENT = new byte[0];

  private HttpRobotRulesParser robots = null;

  private ArrayList<String> userAgentNames = null;
  
  /** Mapping hostnames to cookies */
  private Map<String, String> hostCookies = null;

  /** The proxy hostname. */
  protected String proxyHost = null;

  /** The proxy port. */
  protected int proxyPort = 8080;
  
  /** The proxy port. */
  protected Proxy.Type proxyType = Proxy.Type.HTTP;

  /** The proxy exception list. */
  protected HashMap<String,String> proxyException = new HashMap<>();

  /** Indicates if a proxy is used */
  protected boolean useProxy = false;

  /** The network timeout in millisecond */
  protected int timeout = 10000;

  /** The length limit for downloaded content, in bytes. */
  protected int maxContent = 1024 * 1024;

  /** The time limit to download the entire content, in seconds. */
  protected int maxDuration = 300;

  /** Whether to save partial fetches as truncated content. */
  protected boolean partialAsTruncated = false;

  /** The Nutch 'User-Agent' request header */
  protected String userAgent = getAgentString("NutchCVS", null, "Nutch",
      "http://nutch.apache.org/bot.html", "agent@nutch.apache.org");

  /** The "Accept-Language" request header value. */
  protected String acceptLanguage = "en-us,en-gb,en;q=0.7,*;q=0.3";

  /** The "Accept-Charset" request header value. */
  protected String acceptCharset = "utf-8,iso-8859-1;q=0.7,*;q=0.7";

  /** The "Accept" request header value. */
  protected String accept = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8";

  /** The default logger */
  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  /** The specified logger */
  private Logger logger = LOG;

  /** The nutch configuration */
  private Configuration conf = null;

  /**
   * MimeUtil for MIME type detection. Note (see NUTCH-2578): MimeUtil object is
   * used concurrently by parallel fetcher threads, methods to detect MIME type
   * must be thread-safe.
   */
  private MimeUtil mimeTypes = null;

  /** Do we use HTTP/1.1? */
  protected boolean useHttp11 = false;

  /** Whether to use HTTP/2 */
  protected boolean useHttp2 = false;

  /**
   * Record response time in CrawlDatum's meta data, see property
   * http.store.responsetime.
   */
  protected boolean responseTime = true;

  /**
   * Record the IP address of the responding server, see property
   * <code>store.ip.address</code>.
   */
  protected boolean storeIPAddress = false;

  /**
   * Record the HTTP request in the metadata, see property
   * <code>store.http.request</code>.
   */
  protected boolean storeHttpRequest = false;

  /**
   * Record the HTTP response header in the metadata, see property
   * <code>store.http.headers</code>.
   */
  protected boolean storeHttpHeaders = false;

  /** Skip page if Crawl-Delay longer than this value. */
  protected long maxCrawlDelay = -1L;

  /** Whether to check TLS/SSL certificates */
  protected boolean tlsCheckCertificate = false;

  /** Which TLS/SSL protocols to support */
  protected Set<String> tlsPreferredProtocols;

  /** Which TLS/SSL cipher suites to support */
  protected Set<String> tlsPreferredCipherSuites;
  
  /** Configuration directive for If-Modified-Since HTTP header */
  protected boolean enableIfModifiedsinceHeader = true;
  
  /** Controls whether or not to set Cookie HTTP header based on CrawlDatum metadata */
  protected boolean enableCookieHeader = true;

  /** Creates a new instance of HttpBase */
  public HttpBase() {
    this(null);
  }

  /** Creates a new instance of HttpBase */
  public HttpBase(Logger logger) {
    if (logger != null) {
      this.logger = logger;
    }
    robots = new HttpRobotRulesParser();
  }

  // Inherited Javadoc
  public void setConf(Configuration conf) {
    this.conf = conf;
    this.proxyHost = conf.get("http.proxy.host");
    this.proxyPort = conf.getInt("http.proxy.port", 8080);
    this.proxyType = Proxy.Type.valueOf(conf.get("http.proxy.type", "HTTP"));
    this.proxyException = arrayToMap(conf.getStrings("http.proxy.exception.list"));
    this.useProxy = (proxyHost != null && proxyHost.length() > 0);
    this.timeout = conf.getInt("http.timeout", 10000);
    this.maxContent = conf.getInt("http.content.limit", 1024 * 1024);
    this.maxDuration = conf.getInt("http.time.limit", -1);
    this.partialAsTruncated = conf
        .getBoolean("http.partial.truncated", false);
    this.userAgent = getAgentString(conf.get("http.agent.name"),
        conf.get("http.agent.version"), conf.get("http.agent.description"),
        conf.get("http.agent.url"), conf.get("http.agent.email"));
    this.acceptLanguage = conf.get("http.accept.language", acceptLanguage)
        .trim();
    this.acceptCharset = conf.get("http.accept.charset", acceptCharset).trim();
    this.accept = conf.get("http.accept", accept).trim();
    this.mimeTypes = new MimeUtil(conf);
    // backward-compatible default setting
    this.useHttp11 = conf.getBoolean("http.useHttp11", true);
    this.useHttp2 = conf.getBoolean("http.useHttp2", false);
    this.tlsCheckCertificate = conf.getBoolean("http.tls.certificates.check",
        false);
    this.responseTime = conf.getBoolean("http.store.responsetime", true);
    this.storeIPAddress = conf.getBoolean("store.ip.address", false);
    this.storeHttpRequest = conf.getBoolean("store.http.request", false);
    this.storeHttpHeaders = conf.getBoolean("store.http.headers", false);
    this.enableIfModifiedsinceHeader = conf.getBoolean("http.enable.if.modified.since.header", true);
    this.enableCookieHeader = conf.getBoolean("http.enable.cookie.header", true);
    this.robots.setConf(conf);

    // NUTCH-1941: read list of alternating agent names
    if (conf.getBoolean("http.agent.rotate", false)) {
      String agentsFile = conf.get("http.agent.rotate.file", "agents.txt");
      BufferedReader br = null;
      try {
        Reader reader = conf.getConfResourceAsReader(agentsFile);
        br = new BufferedReader(reader);
        userAgentNames = new ArrayList<String>();
        String word = "";
        while ((word = br.readLine()) != null) {
          if (!word.trim().isEmpty())
            userAgentNames.add(word.trim());
        }

        if (userAgentNames.size() == 0) {
          logger.warn("Empty list of user agents in http.agent.rotate.file {}",
              agentsFile);
          userAgentNames = null;
        }

      } catch (Exception e) {
        logger.warn("Failed to read http.agent.rotate.file {}: {}", agentsFile,
            StringUtils.stringifyException(e));
        userAgentNames = null;
      } finally {
        if (br != null) {
          try {
            br.close();
          } catch (IOException e) {
            // ignore
          }
        }
      }
      if (userAgentNames == null) {
        logger
            .warn("Falling back to fixed user agent set via property http.agent.name");
      }
    }
    
    // If cookies are enabled, try to load a per-host cookie file
    if (enableCookieHeader) {
      String cookieFile = conf.get("http.agent.host.cookie.file", "cookies.txt");
      BufferedReader br = null;
      try {
        Reader reader = conf.getConfResourceAsReader(cookieFile);
        br = new BufferedReader(reader);
        hostCookies = new HashMap<String,String>();
        String word = "";
        while ((word = br.readLine()) != null) {
          if (!word.trim().isEmpty()) {
            if (word.indexOf("#") == -1) { // skip comment
              String[] parts = word.split("\t");
              if (parts.length == 2) {
                hostCookies.put(parts[0], parts[1]);
              } else {
                LOG.warn("Unable to parse cookie file correctly at: " + word);
              }
            }
          }
        }
      } catch (Exception e) {
        logger.warn("Failed to read http.agent.host.cookie.file {}: {}", cookieFile,
            StringUtils.stringifyException(e));
        hostCookies = null;
      } finally {
        if (br != null) {
          try {
            br.close();
          } catch (IOException e) {
            // ignore
          }
        }
      }
    }

    String[] protocols = conf.getStrings("http.tls.supported.protocols",
        "TLSv1.2", "TLSv1.1", "TLSv1", "SSLv3");
    String[] ciphers = conf.getStrings("http.tls.supported.cipher.suites",
        "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384",
        "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384",
        "TLS_RSA_WITH_AES_256_CBC_SHA256",
        "TLS_ECDH_ECDSA_WITH_AES_256_CBC_SHA384",
        "TLS_ECDH_RSA_WITH_AES_256_CBC_SHA384",
        "TLS_DHE_RSA_WITH_AES_256_CBC_SHA256",
        "TLS_DHE_DSS_WITH_AES_256_CBC_SHA256",
        "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA",
        "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA",
        "TLS_ECDH_ECDSA_WITH_AES_256_CBC_SHA",
        "TLS_ECDH_RSA_WITH_AES_256_CBC_SHA",
        "TLS_DHE_RSA_WITH_AES_256_CBC_SHA", "TLS_DHE_DSS_WITH_AES_256_CBC_SHA",
        "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256",
        "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256",
        "TLS_RSA_WITH_AES_128_CBC_SHA256",
        "TLS_ECDH_ECDSA_WITH_AES_128_CBC_SHA256",
        "TLS_ECDH_RSA_WITH_AES_128_CBC_SHA256",
        "TLS_DHE_RSA_WITH_AES_128_CBC_SHA256",
        "TLS_DHE_DSS_WITH_AES_128_CBC_SHA256",
        "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA",
        "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_128_CBC_SHA",
        "TLS_ECDH_ECDSA_WITH_AES_128_CBC_SHA",
        "TLS_ECDH_RSA_WITH_AES_128_CBC_SHA",
        "TLS_DHE_RSA_WITH_AES_128_CBC_SHA", "TLS_DHE_DSS_WITH_AES_128_CBC_SHA",
        "TLS_ECDHE_ECDSA_WITH_RC4_128_SHA", "TLS_ECDHE_RSA_WITH_RC4_128_SHA",
        "SSL_RSA_WITH_RC4_128_SHA", "TLS_ECDH_ECDSA_WITH_RC4_128_SHA",
        "TLS_ECDH_RSA_WITH_RC4_128_SHA",
        "TLS_ECDHE_ECDSA_WITH_3DES_EDE_CBC_SHA",
        "TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA", "SSL_RSA_WITH_3DES_EDE_CBC_SHA",
        "TLS_ECDH_ECDSA_WITH_3DES_EDE_CBC_SHA",
        "TLS_ECDH_RSA_WITH_3DES_EDE_CBC_SHA",
        "SSL_DHE_RSA_WITH_3DES_EDE_CBC_SHA",
        "SSL_DHE_DSS_WITH_3DES_EDE_CBC_SHA", "SSL_RSA_WITH_RC4_128_MD5",
        "TLS_EMPTY_RENEGOTIATION_INFO_SCSV", "TLS_RSA_WITH_NULL_SHA256",
        "TLS_ECDHE_ECDSA_WITH_NULL_SHA", "TLS_ECDHE_RSA_WITH_NULL_SHA",
        "SSL_RSA_WITH_NULL_SHA", "TLS_ECDH_ECDSA_WITH_NULL_SHA",
        "TLS_ECDH_RSA_WITH_NULL_SHA", "SSL_RSA_WITH_NULL_MD5",
        "SSL_RSA_WITH_DES_CBC_SHA", "SSL_DHE_RSA_WITH_DES_CBC_SHA",
        "SSL_DHE_DSS_WITH_DES_CBC_SHA", "TLS_KRB5_WITH_RC4_128_SHA",
        "TLS_KRB5_WITH_RC4_128_MD5", "TLS_KRB5_WITH_3DES_EDE_CBC_SHA",
        "TLS_KRB5_WITH_3DES_EDE_CBC_MD5", "TLS_KRB5_WITH_DES_CBC_SHA",
        "TLS_KRB5_WITH_DES_CBC_MD5");

    tlsPreferredProtocols = new HashSet<String>(Arrays.asList(protocols));
    tlsPreferredCipherSuites = new HashSet<String>(Arrays.asList(ciphers));

    logConf();
  }

  // Inherited Javadoc
  public Configuration getConf() {
    return this.conf;
  }

  public ProtocolOutput getProtocolOutput(Text url, CrawlDatum datum) {

    String urlString = url.toString();
    try {
      URL u = new URL(urlString);

      long startTime = System.currentTimeMillis();
      Response response = getResponse(u, datum, false); // make a request

      if (this.responseTime) {
        int elapsedTime = (int) (System.currentTimeMillis() - startTime);
        datum.getMetaData().put(RESPONSE_TIME, new IntWritable(elapsedTime));
      }

      int code = response.getCode();
      datum.getMetaData().put(Nutch.PROTOCOL_STATUS_CODE_KEY,
        new Text(Integer.toString(code)));

      byte[] content = response.getContent();
      Content c = new Content(u.toString(), u.toString(),
          (content == null ? EMPTY_CONTENT : content),
          response.getHeader("Content-Type"), response.getHeaders(), mimeTypes);

      if (code == 200) { // got a good response
        return new ProtocolOutput(c); // return it

      } else if (code >= 300 && code < 400) { // handle redirect
        String location = response.getHeader("Location");
        // some broken servers, such as MS IIS, use lowercase header name...
        if (location == null)
          location = response.getHeader("location");
        if (location == null)
          location = "";
        u = new URL(u, location);
        int protocolStatusCode;
        switch (code) {
        case 300: // multiple choices, preferred value in Location
          protocolStatusCode = ProtocolStatus.MOVED;
          break;
        case 301: // moved permanently
        case 305: // use proxy (Location is URL of proxy)
          protocolStatusCode = ProtocolStatus.MOVED;
          break;
        case 302: // found (temporarily moved)
        case 303: // see other (redirect after POST)
        case 307: // temporary redirect
          protocolStatusCode = ProtocolStatus.TEMP_MOVED;
          break;
        case 304: // not modified
          protocolStatusCode = ProtocolStatus.NOTMODIFIED;
          break;
        default:
          protocolStatusCode = ProtocolStatus.MOVED;
        }
        // handle this in the higher layer.
        return new ProtocolOutput(c, new ProtocolStatus(protocolStatusCode, u));
      } else if (code == 400) { // bad request, mark as GONE
        if (logger.isTraceEnabled()) {
          logger.trace("400 Bad request: " + u);
        }
        return new ProtocolOutput(c, new ProtocolStatus(ProtocolStatus.GONE, u));
      } else if (code == 401) { // requires authorization, but no valid auth
                                // provided.
        if (logger.isTraceEnabled()) {
          logger.trace("401 Authentication Required");
        }
        return new ProtocolOutput(c, new ProtocolStatus(
            ProtocolStatus.ACCESS_DENIED, "Authentication required: "
                + urlString));
      } else if (code == 404) {
        return new ProtocolOutput(c, new ProtocolStatus(
            ProtocolStatus.NOTFOUND, u));
      } else if (code == 410) { // permanently GONE
        return new ProtocolOutput(c, new ProtocolStatus(ProtocolStatus.GONE,
            "Http: " + code + " url=" + u));
      } else {
        return new ProtocolOutput(c, new ProtocolStatus(
            ProtocolStatus.EXCEPTION, "Http code=" + code + ", url=" + u));
      }
    } catch (Throwable e) {
      logger.error("Failed to get protocol output", e);
      return new ProtocolOutput(null, new ProtocolStatus(e));
    }
  }

  /*
   * -------------------------- * </implementation:Protocol> *
   * --------------------------
   */

  public String getProxyHost() {
    return proxyHost;
  }

  public int getProxyPort() {
    return proxyPort;
  }

  public boolean useProxy(URL url) {
    return useProxy(url.getHost());
  }

  public boolean useProxy(URI uri) {
    return useProxy(uri.getHost());
  }

  public boolean useProxy(String host) {
    if (useProxy && proxyException.containsKey(host)) {
      return false;
    }
    return useProxy;
  }

  public int getTimeout() {
    return timeout;
  }
  
  public boolean isIfModifiedSinceEnabled() {
    return enableIfModifiedsinceHeader;
  }
  
  public boolean isCookieEnabled() {
    return enableCookieHeader;
  }

  public boolean isStoreIPAddress() {
    return storeIPAddress;
  }

  public boolean isStoreHttpRequest() {
    return storeHttpRequest;
  }

  public boolean isStoreHttpHeaders() {
    return storeHttpHeaders;
  }

  public int getMaxContent() {
    return maxContent;
  }

  /**
   * The time limit to download the entire content, in seconds. See the property
   * <code>http.time.limit</code>.
   */
  public int getMaxDuration() {
    return maxDuration;
  }

  /**
   * Whether to save partial fetches as truncated content, cf. the property
   * <code>http.partial.truncated</code>.
   */
  public boolean isStorePartialAsTruncated() {
    return partialAsTruncated;
  }

  public String getUserAgent() {
    if (userAgentNames != null) {
      return userAgentNames
          .get(ThreadLocalRandom.current().nextInt(userAgentNames.size()));
    }
    return userAgent;
  }
  
  /**
   * If per-host cookies are configured, this method will look it up
   * for the given url.
   *
   * @param url the url to look-up a cookie for
   * @return the cookie or null
   */
  public String getCookie(URL url) {
    if (hostCookies != null) {
      return hostCookies.get(url.getHost());
    }
    
    return null;
  }

  /**
   * Value of "Accept-Language" request header sent by Nutch.
   * 
   * @return The value of the header "Accept-Language" header.
   */
  public String getAcceptLanguage() {
    return acceptLanguage;
  }

  public String getAcceptCharset() {
    return acceptCharset;
  }

  public String getAccept() {
    return accept;
  }

  public boolean getUseHttp11() {
    return useHttp11;
  }

  public boolean isTlsCheckCertificates() {
    return tlsCheckCertificate;
  }

  public Set<String> getTlsPreferredCipherSuites() {
    return tlsPreferredCipherSuites;
  }

  public Set<String> getTlsPreferredProtocols() {
    return tlsPreferredProtocols;
  }

  private static String getAgentString(String agentName, String agentVersion,
      String agentDesc, String agentURL, String agentEmail) {

    if ((agentName == null) || (agentName.trim().length() == 0)) {
      // TODO : NUTCH-258
      if (LOG.isErrorEnabled()) {
        LOG.error("No User-Agent string set (http.agent.name)!");
      }
    }

    StringBuffer buf = new StringBuffer();

    buf.append(agentName);
    if (agentVersion != null && !agentVersion.trim().isEmpty()) {
      buf.append("/");
      buf.append(agentVersion);
    }
    if (((agentDesc != null) && (agentDesc.length() != 0))
        || ((agentEmail != null) && (agentEmail.length() != 0))
        || ((agentURL != null) && (agentURL.length() != 0))) {
      buf.append(" (");

      if ((agentDesc != null) && (agentDesc.length() != 0)) {
        buf.append(agentDesc);
        if ((agentURL != null) || (agentEmail != null))
          buf.append("; ");
      }

      if ((agentURL != null) && (agentURL.length() != 0)) {
        buf.append(agentURL);
        if (agentEmail != null)
          buf.append("; ");
      }

      if ((agentEmail != null) && (agentEmail.length() != 0))
        buf.append(agentEmail);

      buf.append(")");
    }
    return buf.toString();
  }

  protected void logConf() {
    if (logger.isInfoEnabled()) {
      logger.info("http.proxy.host = " + proxyHost);
      logger.info("http.proxy.port = " + proxyPort);
      logger.info("http.proxy.exception.list = " + useProxy);
      logger.info("http.timeout = " + timeout);
      logger.info("http.content.limit = " + maxContent);
      logger.info("http.agent = " + userAgent);
      logger.info("http.accept.language = " + acceptLanguage);
      logger.info("http.accept = " + accept);
      logger.info("http.enable.cookie.header = " + isCookieEnabled());
    }
  }

  public byte[] processGzipEncoded(byte[] compressed, URL url)
      throws IOException {

    if (LOG.isTraceEnabled()) {
      LOG.trace("uncompressing....");
    }

    // content can be empty (i.e. redirection) in which case
    // there is nothing to unzip
    if (compressed.length == 0)
      return compressed;

    byte[] content;
    if (getMaxContent() >= 0) {
      content = GZIPUtils.unzipBestEffort(compressed, getMaxContent());
    } else {
      content = GZIPUtils.unzipBestEffort(compressed);
    }

    if (content == null)
      throw new IOException("unzipBestEffort returned null");

    if (LOG.isTraceEnabled()) {
      LOG.trace("fetched " + compressed.length
          + " bytes of compressed content (expanded to " + content.length
          + " bytes) from " + url);
    }
    return content;
  }

  public byte[] processDeflateEncoded(byte[] compressed, URL url)
      throws IOException {

    // content can be empty (i.e. redirection) in which case
    // there is nothing to deflate
    if (compressed.length == 0)
      return compressed;

    if (LOG.isTraceEnabled()) {
      LOG.trace("inflating....");
    }

    byte[] content;
    if (getMaxContent() >= 0) {
      content = DeflateUtils.inflateBestEffort(compressed, getMaxContent());
    } else {
      content = DeflateUtils.inflateBestEffort(compressed);
    }

    if (content == null)
      throw new IOException("inflateBestEffort returned null");

    if (LOG.isTraceEnabled()) {
      LOG.trace("fetched " + compressed.length
          + " bytes of compressed content (expanded to " + content.length
          + " bytes) from " + url);
    }
    return content;
  }

  protected static void main(HttpBase http, String[] args) throws Exception {
    String url = null;

    String usage = "Usage: Http [-verbose] [-timeout N] url";

    if (args.length == 0) {
      System.err.println(usage);
      System.exit(-1);
    }

    for (int i = 0; i < args.length; i++) { // parse command line
      if (args[i].equals("-timeout")) { // found -timeout option
        http.timeout = Integer.parseInt(args[++i]) * 1000;
      } else if (args[i].equals("-verbose")) { // found -verbose option
      } else if (i != args.length - 1) {
        System.err.println(usage);
        System.exit(-1);
      } else
        // root is required parameter
        url = args[i];
    }

    ProtocolOutput out = http
        .getProtocolOutput(new Text(url), new CrawlDatum());
    Content content = out.getContent();

    System.out.println("Status: " + out.getStatus());
    if (content != null) {
      System.out.println("Content Type: " + content.getContentType());
      System.out.println("Content Length: "
          + content.getMetadata().get(Response.CONTENT_LENGTH));
      System.out.println("Content:");
      String text = new String(content.getContent());
      System.out.println(text);
    }
  }

  protected abstract Response getResponse(URL url, CrawlDatum datum,
      boolean followRedirects) throws ProtocolException, IOException;

  @Override
  public BaseRobotRules getRobotRules(Text url, CrawlDatum datum,
      List<Content> robotsTxtContent) {
    return robots.getRobotRulesSet(this, url, robotsTxtContent);
  }
  
  /**
   * Transforming a String[] into a HashMap for faster searching
   * @param input String[]
   * @return a new HashMap
   */
  private HashMap<String, String> arrayToMap(String[] input) {
    if (input == null || input.length == 0) {
      return new HashMap<String, String>();
    }
    HashMap<String, String> hm = new HashMap<>();
    for (int i = 0; i < input.length; i++) {
      if (!"".equals(input[i].trim())) {
        hm.put(input[i], input[i]);
      }
    }
    return hm;
  }
}
