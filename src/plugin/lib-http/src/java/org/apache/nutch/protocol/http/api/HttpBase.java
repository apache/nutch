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
package org.apache.nutch.protocol.http.api;

// JDK imports
import java.io.IOException;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.LinkedList;

// Commons Logging imports
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

// Nutch imports
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.Protocol;
import org.apache.nutch.protocol.ProtocolException;
import org.apache.nutch.protocol.ProtocolOutput;
import org.apache.nutch.protocol.ProtocolStatus;
import org.apache.nutch.protocol.RobotRules;
import org.apache.nutch.util.GZIPUtils;
import org.apache.nutch.util.LogUtil;

// Hadoop imports
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

/**
 * @author J&eacute;r&ocirc;me Charron
 */
public abstract class HttpBase implements Protocol {
  
  
  public static final int BUFFER_SIZE = 8 * 1024;
  
  private static final byte[] EMPTY_CONTENT = new byte[0];

  private RobotRulesParser robots = null;
 
  /** The proxy hostname. */ 
  protected String proxyHost = null;

  /** The proxy port. */
  protected int proxyPort = 8080; 

  /** Indicates if a proxy is used */
  protected boolean useProxy = false;

  /** The network timeout in millisecond */
  protected int timeout = 10000;

  /** The length limit for downloaded content, in bytes. */
  protected int maxContent = 64 * 1024; 

  /** The number of times a thread will delay when trying to fetch a page. */
  protected int maxDelays = 3;

  /**
   * The maximum number of threads that should be allowed
   * to access a host at one time.
   */
  protected int maxThreadsPerHost = 1; 

  /**
   * The number of seconds the fetcher will delay between
   * successive requests to the same server.
   */
  protected long serverDelay = 1000;

  /** The Nutch 'User-Agent' request header */
  protected String userAgent = getAgentString(
                        "NutchCVS", null, "Nutch",
                        "http://lucene.apache.org/nutch/bot.html",
                        "nutch-agent@lucene.apache.org");

    
  /**
   * Maps from host to a Long naming the time it should be unblocked.
   * The Long is zero while the host is in use, then set to now+wait when
   * a request finishes.  This way only one thread at a time accesses a
   * host.
   */
  private static HashMap BLOCKED_ADDR_TO_TIME = new HashMap();
  
  /**
   * Maps a host to the number of threads accessing that host.
   */
  private static HashMap THREADS_PER_HOST_COUNT = new HashMap();
  
  /**
   * Queue of blocked hosts.  This contains all of the non-zero entries
   * from BLOCKED_ADDR_TO_TIME, ordered by increasing time.
   */
  private static LinkedList BLOCKED_ADDR_QUEUE = new LinkedList();
  
  /** The default logger */
  private final static Log LOGGER = LogFactory.getLog(HttpBase.class);

  /** The specified logger */
  private Log logger = LOGGER;
 
  /** The nutch configuration */
  private Configuration conf = null;
  
  /** Do we block by IP addresses or by hostnames? */
  private boolean byIP = true;
 
  /** Do we use HTTP/1.1? */
  protected boolean useHttp11 = false;
  
  /** Skip page if Crawl-Delay longer than this value. */
  protected long maxCrawlDelay = -1L;

  /** Plugin should handle host blocking internally. */
  protected boolean checkBlocking = true;
  
  /** Plugin should handle robot rules checking internally. */
  protected boolean checkRobots = true;

  /** Creates a new instance of HttpBase */
  public HttpBase() {
    this(null);
  }
  
  /** Creates a new instance of HttpBase */
  public HttpBase(Log logger) {
    if (logger != null) {
      this.logger = logger;
    }
    robots = new RobotRulesParser();
  }
  
   // Inherited Javadoc
    public void setConf(Configuration conf) {
        this.conf = conf;
        this.proxyHost = conf.get("http.proxy.host");
        this.proxyPort = conf.getInt("http.proxy.port", 8080);
        this.useProxy = (proxyHost != null && proxyHost.length() > 0);
        this.timeout = conf.getInt("http.timeout", 10000);
        this.maxContent = conf.getInt("http.content.limit", 64 * 1024);
        this.maxDelays = conf.getInt("http.max.delays", 3);
        this.maxThreadsPerHost = conf.getInt("fetcher.threads.per.host", 1);
        this.userAgent = getAgentString(conf.get("http.agent.name"), conf.get("http.agent.version"), conf
                .get("http.agent.description"), conf.get("http.agent.url"), conf.get("http.agent.email"));
        this.serverDelay = (long) (conf.getFloat("fetcher.server.delay", 1.0f) * 1000);
        this.maxCrawlDelay = (long)(conf.getInt("fetcher.max.crawl.delay", -1) * 1000);
        // backward-compatible default setting
        this.byIP = conf.getBoolean("fetcher.threads.per.host.by.ip", true);
        this.useHttp11 = conf.getBoolean("http.useHttp11", false);
        this.robots.setConf(conf);
        this.checkBlocking = conf.getBoolean(Protocol.CHECK_BLOCKING, true);
        this.checkRobots = conf.getBoolean(Protocol.CHECK_ROBOTS, true);
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
      
      if (checkRobots) {
        try {
          if (!robots.isAllowed(this, u)) {
            return new ProtocolOutput(null, new ProtocolStatus(ProtocolStatus.ROBOTS_DENIED, url));
          }
        } catch (Throwable e) {
          // XXX Maybe bogus: assume this is allowed.
          if (logger.isTraceEnabled()) {
            logger.trace("Exception checking robot rules for " + url + ": " + e);
          }
        }
      }
      
      long crawlDelay = robots.getCrawlDelay(this, u);
      long delay = crawlDelay > 0 ? crawlDelay : serverDelay;
      if (checkBlocking && maxCrawlDelay >= 0 && delay > maxCrawlDelay) {
        // skip this page, otherwise the thread would block for too long.
        LOGGER.info("Skipping: " + u + " exceeds fetcher.max.crawl.delay, max="
                + (maxCrawlDelay / 1000) + ", Crawl-Delay=" + (delay / 1000));
        return new ProtocolOutput(null, ProtocolStatus.STATUS_WOULDBLOCK);
      }
      String host = null;
      if (checkBlocking) {
        try {
          host = blockAddr(u, delay);
        } catch (BlockedException be) {
          return new ProtocolOutput(null, ProtocolStatus.STATUS_BLOCKED);
        }
      }
      Response response;
      try {
        response = getResponse(u, datum, false); // make a request
      } finally {
        if (checkBlocking) unblockAddr(host, delay);
      }
      
      int code = response.getCode();
      byte[] content = response.getContent();
      Content c = new Content(u.toString(), u.toString(),
                              (content == null ? EMPTY_CONTENT : content),
                              response.getHeader("Content-Type"),
                              response.getHeaders(), this.conf);
      
      if (code == 200) { // got a good response
        return new ProtocolOutput(c); // return it
        
      } else if (code == 410) { // page is gone
        return new ProtocolOutput(c, new ProtocolStatus(ProtocolStatus.GONE, "Http: " + code + " url=" + url));
        
      } else if (code >= 300 && code < 400) { // handle redirect
        String location = response.getHeader("Location");
        // some broken servers, such as MS IIS, use lowercase header name...
        if (location == null) location = response.getHeader("location");
        if (location == null) location = "";
        u = new URL(u, location);
        int protocolStatusCode;
        switch (code) {
          case 300:   // multiple choices, preferred value in Location
            protocolStatusCode = ProtocolStatus.MOVED;
            break;
          case 301:   // moved permanently
          case 305:   // use proxy (Location is URL of proxy)
            protocolStatusCode = ProtocolStatus.MOVED;
            break;
          case 302:   // found (temporarily moved)
          case 303:   // see other (redirect after POST)
          case 307:   // temporary redirect
            protocolStatusCode = ProtocolStatus.TEMP_MOVED;
            break;
          case 304:   // not modified
            protocolStatusCode = ProtocolStatus.NOTMODIFIED;
            break;
          default:
            protocolStatusCode = ProtocolStatus.MOVED;
        }
        // handle this in the higher layer.
        return new ProtocolOutput(c, new ProtocolStatus(protocolStatusCode, u));
      } else if (code == 400) { // bad request, mark as GONE
        if (logger.isTraceEnabled()) { logger.trace("400 Bad request: " + u); }
        return new ProtocolOutput(c, new ProtocolStatus(ProtocolStatus.GONE, u));
      } else if (code == 401) { // requires authorization, but no valid auth provided.
        if (logger.isTraceEnabled()) { logger.trace("401 Authentication Required"); }
        return new ProtocolOutput(c, new ProtocolStatus(ProtocolStatus.ACCESS_DENIED, "Authentication required: "
                + urlString));
      } else if (code == 404) {
        return new ProtocolOutput(c, new ProtocolStatus(ProtocolStatus.NOTFOUND, u));
      } else if (code == 410) { // permanently GONE
        return new ProtocolOutput(c, new ProtocolStatus(ProtocolStatus.GONE, u));
      } else {
        return new ProtocolOutput(c, new ProtocolStatus(ProtocolStatus.EXCEPTION, "Http code=" + code + ", url="
                + u));
      }
    } catch (Throwable e) {
      e.printStackTrace(LogUtil.getErrorStream(logger));
      return new ProtocolOutput(null, new ProtocolStatus(e));
    }
  }
  
  /* -------------------------- *
   * </implementation:Protocol> *
   * -------------------------- */


  public String getProxyHost() {
    return proxyHost;
  }

  public int getProxyPort() {
    return proxyPort;
  }

  public boolean useProxy() {
    return useProxy;
  }

  public int getTimeout() {
    return timeout;
  }

  public int getMaxContent() {
    return maxContent;
  }

  public int getMaxDelays() {
    return maxDelays;
  }

  public int getMaxThreadsPerHost() {
    return maxThreadsPerHost;
  }

  public long getServerDelay() {
    return serverDelay;
  }

  public String getUserAgent() {
    return userAgent;
  }
  
  public boolean getUseHttp11() {
    return useHttp11;
  }
  
  private String blockAddr(URL url, long crawlDelay) throws ProtocolException {
    
    String host;
    if (byIP) {
      try {
        InetAddress addr = InetAddress.getByName(url.getHost());
        host = addr.getHostAddress();
      } catch (UnknownHostException e) {
        // unable to resolve it, so don't fall back to host name
        throw new HttpException(e);
      }
    } else {
      host = url.getHost();
      if (host == null)
        throw new HttpException("Unknown host for url: " + url);
      host = host.toLowerCase();
    }
    
    int delays = 0;
    while (true) {
      cleanExpiredServerBlocks();                 // free held addresses
      
      Long time;
      synchronized (BLOCKED_ADDR_TO_TIME) {
        time = (Long) BLOCKED_ADDR_TO_TIME.get(host);
        if (time == null) {                       // address is free
          
          // get # of threads already accessing this addr
          Integer counter = (Integer)THREADS_PER_HOST_COUNT.get(host);
          int count = (counter == null) ? 0 : counter.intValue();
          
          count++;                              // increment & store
          THREADS_PER_HOST_COUNT.put(host, new Integer(count));
          
          if (count >= maxThreadsPerHost) {
            BLOCKED_ADDR_TO_TIME.put(host, new Long(0)); // block it
          }
          return host;
        }
      }
      
      if (delays == maxDelays)
        throw new BlockedException("Exceeded http.max.delays: retry later.");
      
      long done = time.longValue();
      long now = System.currentTimeMillis();
      long sleep = 0;
      if (done == 0) {                            // address is still in use
        sleep = crawlDelay;                      // wait at least delay
        
      } else if (now < done) {                    // address is on hold
        sleep = done - now;                       // wait until its free
      }
      
      try {
        Thread.sleep(sleep);
      } catch (InterruptedException e) {}
      delays++;
    }
  }
  
  private void unblockAddr(String host, long crawlDelay) {
    synchronized (BLOCKED_ADDR_TO_TIME) {
      int addrCount = ((Integer)THREADS_PER_HOST_COUNT.get(host)).intValue();
      if (addrCount == 1) {
        THREADS_PER_HOST_COUNT.remove(host);
        BLOCKED_ADDR_QUEUE.addFirst(host);
        BLOCKED_ADDR_TO_TIME.put
                (host, new Long(System.currentTimeMillis() + crawlDelay));
      } else {
        THREADS_PER_HOST_COUNT.put(host, new Integer(addrCount - 1));
      }
    }
  }
  
  private static void cleanExpiredServerBlocks() {
    synchronized (BLOCKED_ADDR_TO_TIME) {
      for (int i = BLOCKED_ADDR_QUEUE.size() - 1; i >= 0; i--) {
        String host = (String) BLOCKED_ADDR_QUEUE.get(i);
        long time = ((Long) BLOCKED_ADDR_TO_TIME.get(host)).longValue();
        if (time <= System.currentTimeMillis()) {
          BLOCKED_ADDR_TO_TIME.remove(host);
          BLOCKED_ADDR_QUEUE.remove(i);
        }
      }
    }
  }
  
  private static String getAgentString(String agentName,
                                       String agentVersion,
                                       String agentDesc,
                                       String agentURL,
                                       String agentEmail) {
    
    if ( (agentName == null) || (agentName.trim().length() == 0) ) {
      // TODO : NUTCH-258
      if (LOGGER.isFatalEnabled()) {
        LOGGER.fatal("No User-Agent string set (http.agent.name)!");
      }
    }
    
    StringBuffer buf= new StringBuffer();
    
    buf.append(agentName);
    if (agentVersion != null) {
      buf.append("/");
      buf.append(agentVersion);
    }
    if ( ((agentDesc != null) && (agentDesc.length() != 0))
    || ((agentEmail != null) && (agentEmail.length() != 0))
    || ((agentURL != null) && (agentURL.length() != 0)) ) {
      buf.append(" (");
      
      if ((agentDesc != null) && (agentDesc.length() != 0)) {
        buf.append(agentDesc);
        if ( (agentURL != null) || (agentEmail != null) )
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
      logger.info("http.timeout = " + timeout);
      logger.info("http.content.limit = " + maxContent);
      logger.info("http.agent = " + userAgent);
      logger.info(Protocol.CHECK_BLOCKING + " = " + checkBlocking);
      logger.info(Protocol.CHECK_ROBOTS + " = " + checkRobots);
      if (checkBlocking) {
        logger.info("fetcher.server.delay = " + serverDelay);
        logger.info("http.max.delays = " + maxDelays);
      }
    }
  }
  
  public byte[] processGzipEncoded(byte[] compressed, URL url) throws IOException {

    if (LOGGER.isTraceEnabled()) { LOGGER.trace("uncompressing...."); }

    byte[] content;
    if (getMaxContent() >= 0) {
        content = GZIPUtils.unzipBestEffort(compressed, getMaxContent());
    } else {
        content = GZIPUtils.unzipBestEffort(compressed);
    } 

    if (content == null)
      throw new IOException("unzipBestEffort returned null");

    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("fetched " + compressed.length
                 + " bytes of compressed content (expanded to "
                 + content.length + " bytes) from " + url);
    }
    return content;
  }
  
  protected static void main(HttpBase http, String[] args) throws Exception {
    boolean verbose = false;
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
        verbose = true;
      } else if (i != args.length - 1) {
        System.err.println(usage);
        System.exit(-1);
      } else // root is required parameter
        url = args[i];
    }
    
//    if (verbose) {
//      LOGGER.setLevel(Level.FINE);
//    }
    
    ProtocolOutput out = http.getProtocolOutput(new Text(url), new CrawlDatum());
    Content content = out.getContent();
    
    System.out.println("Status: " + out.getStatus());
    if (content != null) {
      System.out.println("Content Type: " + content.getContentType());
      System.out.println("Content Length: " +
                         content.getMetadata().get(Response.CONTENT_LENGTH));
      System.out.println("Content:");
      String text = new String(content.getContent());
      System.out.println(text);
    }
    
  }
  
  
  protected abstract Response getResponse(URL url,
                                          CrawlDatum datum,
                                          boolean followRedirects)
    throws ProtocolException, IOException;

  public RobotRules getRobotRules(Text url, CrawlDatum datum) {
    return robots.getRobotRulesSet(this, url);
  }

}
