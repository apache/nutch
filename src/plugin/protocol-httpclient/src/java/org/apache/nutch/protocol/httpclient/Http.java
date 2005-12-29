/* Copyright (c) 2004 The Nutch Organization.  All rights reserved.   */
/* Use subject to the conditions in http://www.nutch.org/LICENSE.txt. */

package org.apache.nutch.protocol.httpclient;

import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.httpclient.Credentials;
import org.apache.commons.httpclient.HostConfiguration;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.NTCredentials;
import org.apache.commons.httpclient.auth.AuthScope;
import org.apache.commons.httpclient.params.HttpConnectionManagerParams;
import org.apache.commons.httpclient.protocol.Protocol;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.io.UTF8;
import org.apache.nutch.protocol.*;
import org.apache.nutch.util.LogFormatter;
import org.apache.nutch.util.NutchConf;

/** An implementation of the Http protocol. */
public class Http implements org.apache.nutch.protocol.Protocol {

  public static final Logger LOG = LogFormatter.getLogger("org.apache.nutch.net.Http");

  static {
    if (NutchConf.get().getBoolean("http.verbose", false)) {
      LOG.setLevel(Level.FINE);
    } else {                                      // shush about redirects
      Logger.getLogger("org.apache.commons.httpclient.HttpMethodDirector")
        .setLevel(Level.WARNING);
    }
  }

  static final int BUFFER_SIZE = 8 * 1024;
  private static MultiThreadedHttpConnectionManager connectionManager = new MultiThreadedHttpConnectionManager();
  private static HttpClient client;

  static synchronized HttpClient getClient() {
    if (client != null) return client;
    configureClient();
    return client;
  }

  static String PROXY_HOST = NutchConf.get().get("http.proxy.host");
  static int PROXY_PORT = NutchConf.get().getInt("http.proxy.port", 8080);
  static boolean PROXY = (PROXY_HOST != null && PROXY_HOST.length() > 0);
  static int TIMEOUT = NutchConf.get().getInt("http.timeout", 10000);
  static int MAX_CONTENT = NutchConf.get().getInt("http.content.limit", 64 * 1024);
  static int MAX_DELAYS = NutchConf.get().getInt("http.max.delays", 3);
  static int MAX_THREADS_PER_HOST = NutchConf.get().getInt("fetcher.threads.per.host", 1);
  static int MAX_THREADS_TOTAL = NutchConf.get().getInt("fetcher.threads.fetch", 10);
  static String AGENT_STRING = getAgentString();
  static long SERVER_DELAY = (long) (NutchConf.get().getFloat("fetcher.server.delay", 1.0f) * 1000);
  static String NTLM_USERNAME = NutchConf.get().get("http.auth.ntlm.username", "");
  static String NTLM_PASSWORD = NutchConf.get().get("http.auth.ntlm.password", "");
  static String NTLM_DOMAIN = NutchConf.get().get("http.auth.ntlm.domain", "");
  static String NTLM_HOST = NutchConf.get().get("http.auth.ntlm.host", "");

  static {
    LOG.info("http.proxy.host = " + PROXY_HOST);
    LOG.info("http.proxy.port = " + PROXY_PORT);

    LOG.info("http.timeout = " + TIMEOUT);
    LOG.info("http.content.limit = " + MAX_CONTENT);
    LOG.info("http.agent = " + AGENT_STRING);

    LOG.info("http.auth.ntlm.username = " + NTLM_USERNAME);

    LOG.info("fetcher.server.delay = " + SERVER_DELAY);
    LOG.info("http.max.delays = " + MAX_DELAYS);
  }

  /**
   * Maps from InetAddress to a Long naming the time it should be unblocked. The
   * Long is zero while the address is in use, then set to now+wait when a
   * request finishes. This way only one thread at a time accesses an address.
   */
  private static HashMap BLOCKED_ADDR_TO_TIME = new HashMap();

  /** Maps an address to the number of threads accessing that address. */
  private static HashMap THREADS_PER_HOST_COUNT = new HashMap();

  /**
   * Queue of blocked InetAddress. This contains all of the non-zero entries
   * from BLOCKED_ADDR_TO_TIME, ordered by increasing time.
   */
  private static LinkedList BLOCKED_ADDR_QUEUE = new LinkedList();

  private static InetAddress blockAddr(URL url) throws ProtocolException {
    InetAddress addr;
    try {
      addr = InetAddress.getByName(url.getHost());
    } catch (UnknownHostException e) {
      throw new HttpException(e);
    }

    int delays = 0;
    while (true) {
      cleanExpiredServerBlocks(); // free held addresses

      Long time;
      synchronized (BLOCKED_ADDR_TO_TIME) {
        time = (Long) BLOCKED_ADDR_TO_TIME.get(addr);
        if (time == null) { // address is free

          // get # of threads already accessing this addr
          Integer counter = (Integer) THREADS_PER_HOST_COUNT.get(addr);
          int count = (counter == null) ? 0 : counter.intValue();

          count++; // increment & store
          THREADS_PER_HOST_COUNT.put(addr, new Integer(count));

          if (count >= MAX_THREADS_PER_HOST) {
            BLOCKED_ADDR_TO_TIME.put(addr, new Long(0)); // block it
          }
          return addr;
        }
      }

      if (delays == MAX_DELAYS) throw new HttpException("Exceeded http.max.delays: retry later.");

      long done = time.longValue();
      long now = System.currentTimeMillis();
      long sleep = 0;
      if (done == 0) { // address is still in use
        sleep = SERVER_DELAY; // wait at least delay

      } else if (now < done) { // address is on hold
        sleep = done - now; // wait until its free
      }

      try {
        Thread.sleep(sleep);
      } catch (InterruptedException e) {}
      delays++;
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

  private static void unblockAddr(InetAddress addr) {
    synchronized (BLOCKED_ADDR_TO_TIME) {
      int addrCount = ((Integer) THREADS_PER_HOST_COUNT.get(addr)).intValue();
      if (addrCount == 1) {
        THREADS_PER_HOST_COUNT.remove(addr);
        BLOCKED_ADDR_QUEUE.addFirst(addr);
        BLOCKED_ADDR_TO_TIME.put(addr, new Long(System.currentTimeMillis() + SERVER_DELAY));
      } else {
        THREADS_PER_HOST_COUNT.put(addr, new Integer(addrCount - 1));
      }
    }
  }

  public ProtocolOutput getProtocolOutput(UTF8 url, CrawlDatum datum) {
    String urlString = url.toString();
    try {
      URL u = new URL(urlString);

        try {
          if (!RobotRulesParser.isAllowed(u))
                  return new ProtocolOutput(null, new ProtocolStatus(ProtocolStatus.ROBOTS_DENIED, url));
        } catch (Throwable e) {
          // XXX Maybe bogus: assume this is allowed.
          LOG.fine("Exception checking robot rules for " + url + ": " + e);
        }

        InetAddress addr = blockAddr(u);
        HttpResponse response;
        try {
          response = new HttpResponse(u, datum); // make a request
        } finally {
          unblockAddr(addr);
        }

        int code = response.getCode();
        Content c = response.toContent();

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
          LOG.fine("400 Bad request: " + u);
          return new ProtocolOutput(c, new ProtocolStatus(ProtocolStatus.GONE, u));
        } else if (code == 401) { // requires authorization, but no valid auth provided.
          LOG.fine("401 Authentication Required");
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

  private static String getAgentString() {
    NutchConf conf = NutchConf.get();
    String agentName = conf.get("http.agent.name");
    String agentVersion = conf.get("http.agent.version");
    String agentDesc = conf.get("http.agent.description");
    String agentURL = conf.get("http.agent.url");
    String agentEmail = conf.get("http.agent.email");

    if ((agentName == null) || (agentName.trim().length() == 0))
            LOG.severe("No User-Agent string set (http.agent.name)!");

    StringBuffer buf = new StringBuffer();

    buf.append(agentName);
    if (agentVersion != null) {
      buf.append("/");
      buf.append(agentVersion);
    }
    if (((agentDesc != null) && (agentDesc.length() != 0)) || ((agentEmail != null) && (agentEmail.length() != 0))
            || ((agentURL != null) && (agentURL.length() != 0))) {
      buf.append(" (");

      if ((agentDesc != null) && (agentDesc.length() != 0)) {
        buf.append(agentDesc);
        if ((agentURL != null) || (agentEmail != null)) buf.append("; ");
      }

      if ((agentURL != null) && (agentURL.length() != 0)) {
        buf.append(agentURL);
        if (agentEmail != null) buf.append("; ");
      }

      if ((agentEmail != null) && (agentEmail.length() != 0)) buf.append(agentEmail);

      buf.append(")");
    }
    return buf.toString();
  }

  /** For debugging. */
  public static void main(String[] args) throws Exception {
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

    Http http = new Http();

    if (verbose) {
      LOG.setLevel(Level.FINE);
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

  private static void configureClient() {

    // get a client isntance -- we just need one.

    client = new HttpClient(connectionManager);

    // Set up an HTTPS socket factory that accepts self-signed certs.
    Protocol dummyhttps = new Protocol("https", new DummySSLProtocolSocketFactory(), 443);
    Protocol.registerProtocol("https", dummyhttps);
    
    HttpConnectionManagerParams params = connectionManager.getParams();
    params.setConnectionTimeout(TIMEOUT);
    params.setSoTimeout(TIMEOUT);
    params.setSendBufferSize(BUFFER_SIZE);
    params.setReceiveBufferSize(BUFFER_SIZE);
    params.setMaxTotalConnections(MAX_THREADS_TOTAL);
    if (MAX_THREADS_TOTAL > MAX_THREADS_PER_HOST) {
      params.setDefaultMaxConnectionsPerHost(MAX_THREADS_PER_HOST);
    } else {
      params.setDefaultMaxConnectionsPerHost(MAX_THREADS_TOTAL);
    }

    HostConfiguration hostConf = client.getHostConfiguration();
    if (PROXY) {
      hostConf.setProxy(PROXY_HOST, PROXY_PORT);
    }
    if (NTLM_USERNAME.length() > 0) {
      Credentials ntCreds = new NTCredentials(NTLM_USERNAME, NTLM_PASSWORD, NTLM_HOST, NTLM_DOMAIN);
      client.getState().setCredentials(new AuthScope(NTLM_HOST, AuthScope.ANY_PORT), ntCreds);

      LOG.info("Added NTLM credentials for " + NTLM_USERNAME);
    }
    LOG.info("Configured Client");
  }
}