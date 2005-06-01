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

package org.apache.nutch.protocol.http;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.nutch.util.LogFormatter;
import org.apache.nutch.util.NutchConf;

import org.apache.nutch.db.Page;
import org.apache.nutch.pagedb.FetchListEntry;
import org.apache.nutch.protocol.*;

/** An implementation of the Http protocol. */
public class Http implements Protocol {

  public static final Logger LOG =
    LogFormatter.getLogger("org.apache.nutch.net.Http");

  static {
    if (NutchConf.get().getBoolean("http.verbose", false))
      LOG.setLevel(Level.FINE);
  }

  static final int BUFFER_SIZE = 8 * 1024;

  private static final int MAX_REDIRECTS =
    NutchConf.get().getInt("http.redirect.max", 3);

  static String PROXY_HOST = NutchConf.get().get("http.proxy.host");
  static int PROXY_PORT = NutchConf.get().getInt("http.proxy.port",8080);
  static boolean PROXY = (PROXY_HOST != null && PROXY_HOST.length() > 0);
  
  static int TIMEOUT = NutchConf.get().getInt("http.timeout", 10000);
  static int MAX_CONTENT= NutchConf.get().getInt("http.content.limit",64*1024);

  static int MAX_DELAYS= NutchConf.get().getInt("http.max.delays",3);
  static int MAX_THREADS_PER_HOST =
    NutchConf.get().getInt("fetcher.threads.per.host", 1);

  static String AGENT_STRING = getAgentString();

  static long SERVER_DELAY =
    (long)(NutchConf.get().getFloat("fetcher.server.delay", 1.0f) * 1000);

  static {
    LOG.info("http.proxy.host = " + PROXY_HOST);
    LOG.info("http.proxy.port = " + PROXY_PORT);

    LOG.info("http.timeout = " + TIMEOUT);
    LOG.info("http.content.limit = " + MAX_CONTENT);
    LOG.info("http.agent = " + AGENT_STRING);

    LOG.info("fetcher.server.delay = " + SERVER_DELAY);
    LOG.info("http.max.delays = " + MAX_DELAYS);
  }

  /** Maps from InetAddress to a Long naming the time it should be unblocked.
   * The Long is zero while the address is in use, then set to now+wait when
   * a request finishes.  This way only one thread at a time accesses an
   * address. */
  private static HashMap BLOCKED_ADDR_TO_TIME = new HashMap();
    
  /** Maps an address to the number of threads accessing that address. */
  private static HashMap THREADS_PER_HOST_COUNT = new HashMap();

  /** Queue of blocked InetAddress.  This contains all of the non-zero entries
   * from BLOCKED_ADDR_TO_TIME, ordered by increasing time. */
  private static LinkedList BLOCKED_ADDR_QUEUE = new LinkedList();

  private RobotRulesParser robotRules = new RobotRulesParser();

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
        throw new RetryLater(url, "Exceeded http.max.delays: retry later.");

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
  
  private static void cleanExpiredServerBlocks() {
    synchronized (BLOCKED_ADDR_TO_TIME) {
      while (!BLOCKED_ADDR_QUEUE.isEmpty()) {
        InetAddress addr = (InetAddress)BLOCKED_ADDR_QUEUE.getLast();
        long time = ((Long)BLOCKED_ADDR_TO_TIME.get(addr)).longValue();
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
      int addrCount = ((Integer)THREADS_PER_HOST_COUNT.get(addr)).intValue();
      if (addrCount == 1) {
        THREADS_PER_HOST_COUNT.remove(addr);
        BLOCKED_ADDR_QUEUE.addFirst(addr);
        BLOCKED_ADDR_TO_TIME.put
          (addr, new Long(System.currentTimeMillis()+SERVER_DELAY));
      }
      else {
        THREADS_PER_HOST_COUNT.put(addr, new Integer(addrCount - 1));
      }
    }
  }

  public ProtocolOutput getProtocolOutput(String urlString) {
    ProtocolOutput output = null;
    try {
      return getProtocolOutput(new FetchListEntry(true,
            new Page(urlString, 1.0f), new String[0]));
    } catch (MalformedURLException mue) {
      return new ProtocolOutput(null, new ProtocolStatus(mue));
    }
  }
  
  public ProtocolOutput getProtocolOutput(FetchListEntry fle) {
    String urlString = fle.getUrl().toString();
    try {
      URL url = new URL(urlString);

      int redirects = 0;
      while (true) {
        
        if (!RobotRulesParser.isAllowed(url))
          throw new ResourceGone(url, "Blocked by robots.txt");
        
        InetAddress addr = blockAddr(url);
        HttpResponse response;
        try {
          response = new HttpResponse(urlString, url); // make a request
        } finally {
          unblockAddr(addr);
        }
        
        int code = response.getCode();
        
        if (code == 200) {                        // got a good response
          return new ProtocolOutput(response.toContent());            // return it
          
        } else if (code == 410) {                 // page is gone
          throw new ResourceGone(url, "Http: " + code);

        } else if (code >= 300 && code < 400) {   // handle redirect
          if (redirects == MAX_REDIRECTS)
            throw new HttpException("Too many redirects: " + urlString);
          url = new URL(url, response.getHeader("Location"));
          redirects++;                
          LOG.fine("redirect to " + url); 
          
        } else {                                  // convert to exception
          throw new HttpError(code);
        }
      }
    } catch (Exception e) {
      return new ProtocolOutput(null, new ProtocolStatus(e));
    } 
  }

  private static String getAgentString() {
    String agentName = NutchConf.get().get("http.agent.name");
    String agentVersion = NutchConf.get().get("http.agent.version");
    String agentDesc = NutchConf.get().get("http.agent.description");
    String agentURL = NutchConf.get().get("http.agent.url");
    String agentEmail = NutchConf.get().get("http.agent.email");

    if ( (agentName == null) || (agentName.trim().length() == 0) )
      LOG.severe("No User-Agent string set (http.agent.name)!");

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

  /** For debugging. */
  public static void main(String[] args) throws Exception {
    boolean verbose = false;
    String url = null;

    String usage = "Usage: Http [-verbose] [-timeout N] url";

    if (args.length == 0) {
      System.err.println(usage);
      System.exit(-1);
    }
      

    for (int i = 0; i < args.length; i++) {       // parse command line
      if (args[i].equals("-timeout")) {           // found -timeout option
        TIMEOUT = Integer.parseInt(args[++i]) * 1000;
      } else if (args[i].equals("-verbose")) {    // found -verbose option
        verbose = true;
      } else if (i != args.length-1) {
        System.err.println(usage);
        System.exit(-1);
      } else                                      // root is required parameter
        url = args[i];
    }

    Http http = new Http();

    if (verbose) {
      LOG.setLevel(Level.FINE);
    }

    Content content = http.getProtocolOutput(url).getContent();

    System.out.println("Content Type: " + content.getContentType());
    System.out.println("Content Length: " + content.get("Content-Length"));
    System.out.println("Content:");
    String text = new String(content.getContent());
    System.out.println(text);

  }

}
