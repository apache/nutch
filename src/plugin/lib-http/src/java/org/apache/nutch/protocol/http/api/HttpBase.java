/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

// Nutch imports
import java.io.IOException;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.io.UTF8;
import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.Protocol;
import org.apache.nutch.protocol.ProtocolException;
import org.apache.nutch.protocol.ProtocolOutput;
import org.apache.nutch.protocol.ProtocolStatus;
import org.apache.nutch.util.NutchConf;


/**
 * @author J&eacute;r&ocirc;me Charron
 */
public abstract class HttpBase implements Protocol {
  
  
  public static final int BUFFER_SIZE = 8 * 1024;
  
  public static String PROXY_HOST =
          NutchConf.get().get("http.proxy.host");
  
  public static int PROXY_PORT =
          NutchConf.get().getInt("http.proxy.port", 8080);
  
  public static boolean PROXY =
          (PROXY_HOST != null && PROXY_HOST.length() > 0);
  
  public static int TIMEOUT =
          NutchConf.get().getInt("http.timeout", 10000);
  
  public static int MAX_CONTENT =
          NutchConf.get().getInt("http.content.limit", 64 * 1024);
  
  public static int MAX_DELAYS =
          NutchConf.get().getInt("http.max.delays", 3);
  
  public static int MAX_THREADS_PER_HOST =
          NutchConf.get().getInt("fetcher.threads.per.host", 1);
  
  public static String AGENT_STRING =
          getAgentString();
  
  public static long SERVER_DELAY =
          (long) (NutchConf.get().getFloat("fetcher.server.delay", 1.0f) * 1000);
  

  private static final byte[] EMPTY_CONTENT = new byte[0];
    
  /**
   * Maps from InetAddress to a Long naming the time it should be unblocked.
   * The Long is zero while the address is in use, then set to now+wait when
   * a request finishes.  This way only one thread at a time accesses an
   * address.
   */
  private static HashMap BLOCKED_ADDR_TO_TIME = new HashMap();
  
  /**
   * Maps an address to the number of threads accessing that address.
   */
  private static HashMap THREADS_PER_HOST_COUNT = new HashMap();
  
  /**
   * Queue of blocked InetAddress.  This contains all of the non-zero entries
   * from BLOCKED_ADDR_TO_TIME, ordered by increasing time.
   */
  private static LinkedList BLOCKED_ADDR_QUEUE = new LinkedList();
  
  /** The default logger */
  private final static Logger LOGGER = Logger.getLogger(HttpBase.class.getName());

  /** The specified logger */
  private Logger logger = LOGGER;
  

  /** Creates a new instance of HttpBase */
  public HttpBase() {
    this(null);
  }
  
  /** Creates a new instance of HttpBase */
  public HttpBase(Logger logger) {
    if (logger != null) {
      this.logger = logger;
    }
    logger.info("http.proxy.host = " + PROXY_HOST);
    logger.info("http.proxy.port = " + PROXY_PORT);
    logger.info("http.timeout = " + TIMEOUT);
    logger.info("http.content.limit = " + MAX_CONTENT);
    logger.info("http.agent = " + AGENT_STRING);
    logger.info("fetcher.server.delay = " + SERVER_DELAY);
    logger.info("http.max.delays = " + MAX_DELAYS);
  }
  
  public ProtocolOutput getProtocolOutput(UTF8 url, CrawlDatum datum) {
    
    String urlString = url.toString();
    try {
      URL u = new URL(urlString);
      
      try {
        if (!RobotRulesParser.isAllowed(this, u)) {
          return new ProtocolOutput(null, new ProtocolStatus(ProtocolStatus.ROBOTS_DENIED, url));
        }
      } catch (Throwable e) {
        // XXX Maybe bogus: assume this is allowed.
        logger.fine("Exception checking robot rules for " + url + ": " + e);
      }
      
      InetAddress addr = blockAddr(u);
      Response response;
      try {
        response = getResponse(u, datum, false); // make a request
      } finally {
        unblockAddr(addr);
      }
      
      int code = response.getCode();
      byte[] content = response.getContent();
      Content c = new Content(u.toString(), u.toString(),
                              (content == null ? EMPTY_CONTENT : content),
                              response.getHeader("Content-Type"),
                              response.getHeaders());
      
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
        logger.fine("400 Bad request: " + u);
        return new ProtocolOutput(c, new ProtocolStatus(ProtocolStatus.GONE, u));
      } else if (code == 401) { // requires authorization, but no valid auth provided.
        logger.fine("401 Authentication Required");
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
      e.printStackTrace();
      return new ProtocolOutput(null, new ProtocolStatus(e));
    }
  }
  
  
  private static InetAddress blockAddr(URL url) throws ProtocolException {
    
    InetAddress addr;
    try {
      addr = InetAddress.getByName(url.getHost());
    } catch (UnknownHostException e) {
      throw new HttpException(e);
    }
    
    int delays = 0;
    while (true) {
      cleanExpiredServerBlocks();                 // free held addresses
      
      Long time;
      synchronized (BLOCKED_ADDR_TO_TIME) {
        time = (Long) BLOCKED_ADDR_TO_TIME.get(addr);
        if (time == null) {                       // address is free
          
          // get # of threads already accessing this addr
          Integer counter = (Integer)THREADS_PER_HOST_COUNT.get(addr);
          int count = (counter == null) ? 0 : counter.intValue();
          
          count++;                              // increment & store
          THREADS_PER_HOST_COUNT.put(addr, new Integer(count));
          
          if (count >= MAX_THREADS_PER_HOST) {
            BLOCKED_ADDR_TO_TIME.put(addr, new Long(0)); // block it
          }
          return addr;
        }
      }
      
      if (delays == MAX_DELAYS)
        throw new HttpException("Exceeded http.max.delays: retry later.");
      
      long done = time.longValue();
      long now = System.currentTimeMillis();
      long sleep = 0;
      if (done == 0) {                            // address is still in use
        sleep = SERVER_DELAY;                     // wait at least delay
        
      } else if (now < done) {                    // address is on hold
        sleep = done - now;                       // wait until its free
      }
      
      try {
        Thread.sleep(sleep);
      } catch (InterruptedException e) {}
      delays++;
    }
  }
  
  private static void unblockAddr(InetAddress addr) {
    synchronized (BLOCKED_ADDR_TO_TIME) {
      int addrCount = ((Integer)THREADS_PER_HOST_COUNT.get(addr)).intValue();
      if (addrCount == 1) {
        THREADS_PER_HOST_COUNT.remove(addr);
        BLOCKED_ADDR_QUEUE.addFirst(addr);
        BLOCKED_ADDR_TO_TIME.put
                (addr, new Long(System.currentTimeMillis()+SERVER_DELAY));
      } else {
        THREADS_PER_HOST_COUNT.put(addr, new Integer(addrCount - 1));
      }
    }
  }
  
  private static void cleanExpiredServerBlocks() {
    synchronized (BLOCKED_ADDR_TO_TIME) {
      while (!BLOCKED_ADDR_QUEUE.isEmpty()) {
        InetAddress addr = (InetAddress) BLOCKED_ADDR_QUEUE.getLast();
        long time = ((Long) BLOCKED_ADDR_TO_TIME.get(addr)).longValue();
        if (time <= System.currentTimeMillis()) {
          BLOCKED_ADDR_TO_TIME.remove(addr);
          BLOCKED_ADDR_QUEUE.removeLast();
        } else {
          break;
        }
      }
    }
  }
  
  private static String getAgentString() {
    
    String agentName = NutchConf.get().get("http.agent.name");
    String agentVersion = NutchConf.get().get("http.agent.version");
    String agentDesc = NutchConf.get().get("http.agent.description");
    String agentURL = NutchConf.get().get("http.agent.url");
    String agentEmail = NutchConf.get().get("http.agent.email");
    
    if ( (agentName == null) || (agentName.trim().length() == 0) )
      LOGGER.severe("No User-Agent string set (http.agent.name)!");
    
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
        TIMEOUT = Integer.parseInt(args[++i]) * 1000;
      } else if (args[i].equals("-verbose")) { // found -verbose option
        verbose = true;
      } else if (i != args.length - 1) {
        System.err.println(usage);
        System.exit(-1);
      } else // root is required parameter
        url = args[i];
    }
    
    if (verbose) {
      LOGGER.setLevel(Level.FINE);
    }
    
    ProtocolOutput out = http.getProtocolOutput(new UTF8(url), new CrawlDatum());
    Content content = out.getContent();
    
    System.out.println("Status: " + out.getStatus());
    if (content != null) {
      System.out.println("Content Type: " + content.getContentType());
      System.out.println("Content Length: " + content.get("Content-Length"));
      System.out.println("Content:");
      String text = new String(content.getContent());
      System.out.println(text);
    }
    
  }
  
  
  protected abstract Response getResponse(URL url,
                                          CrawlDatum datum,
                                          boolean followRedirects)
    throws ProtocolException, IOException;
  
}
