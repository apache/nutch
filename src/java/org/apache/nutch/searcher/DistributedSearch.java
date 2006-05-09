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

package org.apache.nutch.searcher;

import java.net.InetSocketAddress;
import java.io.*;
import java.util.*;
import java.util.logging.Logger;
import java.lang.reflect.Method;

import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseText;
import org.apache.nutch.crawl.Inlinks;

import org.apache.hadoop.util.LogFormatter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

import org.apache.nutch.util.NutchConfiguration;

/** Implements the search API over IPC connnections. */
public class DistributedSearch {
  public static final Logger LOG =
    LogFormatter.getLogger("org.apache.nutch.searcher.DistributedSearch");

  private DistributedSearch() {}                  // no public ctor

  /** The distributed search protocol. */
  public interface Protocol
    extends Searcher, HitDetailer, HitSummarizer, HitContent, HitInlinks {

    /** The name of the segments searched by this node. */
    String[] getSegmentNames();
  }

  /** The search server. */
  public static class Server  {

    private Server() {}

    /** Runs a search server. */
    public static void main(String[] args) throws Exception {
      String usage = "DistributedSearch$Server <port> <index dir>";

      if (args.length == 0 || args.length > 2) {
        System.err.println(usage);
        System.exit(-1);
      }

      int port = Integer.parseInt(args[0]);
      Path directory = new Path(args[1]);

      Configuration conf = NutchConfiguration.create();
      NutchBean bean = new NutchBean(conf, directory);

      org.apache.hadoop.ipc.Server server = RPC.getServer(bean, port, 10, true, conf);
      server.start();
      server.join();
    }

  }

  /** The search client. */
  public static class Client extends Thread
    implements Searcher, HitDetailer, HitSummarizer, HitContent, HitInlinks,
               Runnable {

    private InetSocketAddress[] defaultAddresses;
    private InetSocketAddress[] liveAddresses;
    private HashMap segmentToAddress = new HashMap();
    
    private boolean running = true;
    private Configuration conf;

    /** Construct a client talking to servers listed in the named file.
     * Each line in the file lists a server hostname and port, separated by
     * whitespace. 
     */
    public Client(Path file, Configuration conf) throws IOException {
      this(readConfig(file, conf), conf);
    }

    private static InetSocketAddress[] readConfig(Path path, Configuration conf)
      throws IOException {
      FileSystem fs = FileSystem.get(conf);
      BufferedReader reader =
        new BufferedReader(new InputStreamReader(fs.open(path)));
      try {
        ArrayList addrs = new ArrayList();
        String line;
        while ((line = reader.readLine()) != null) {
          StringTokenizer tokens = new StringTokenizer(line);
          if (tokens.hasMoreTokens()) {
            String host = tokens.nextToken();
            if (tokens.hasMoreTokens()) {
              String port = tokens.nextToken();
              addrs.add(new InetSocketAddress(host, Integer.parseInt(port)));
              LOG.info("Client adding server "  + host + ":" + port);
            }
          }
        }
        return (InetSocketAddress[])
          addrs.toArray(new InetSocketAddress[addrs.size()]);
      } finally {
        reader.close();
      }
    }

    /** Construct a client talking to the named servers. */
    public Client(InetSocketAddress[] addresses, Configuration conf) throws IOException {
      this.conf = conf;
      this.defaultAddresses = addresses;
      updateSegments();
      setDaemon(true);
      start();
    }
    
    private static final Method GET_SEGMENTS;
    private static final Method SEARCH;
    private static final Method DETAILS;
    private static final Method SUMMARY;
    static {
      try {
        GET_SEGMENTS = Protocol.class.getMethod
          ("getSegmentNames", new Class[] {});
        SEARCH = Protocol.class.getMethod
          ("search", new Class[] { Query.class, Integer.TYPE, String.class,
                                   String.class, Boolean.TYPE});
        DETAILS = Protocol.class.getMethod
          ("getDetails", new Class[] { Hit.class});
        SUMMARY = Protocol.class.getMethod
          ("getSummary", new Class[] { HitDetails.class, Query.class});
      } catch (NoSuchMethodException e) {
        throw new RuntimeException(e);
      }
    }


    /** Updates segment names.
     * 
     * @throws IOException
     */
    public void updateSegments() throws IOException {
      
      int liveServers=0;
      int liveSegments=0;
      Vector liveAddresses=new Vector();
      
      // build segmentToAddress map
      Object[][] params = new Object[defaultAddresses.length][0];
      String[][] results =
        (String[][])RPC.call(GET_SEGMENTS, params, defaultAddresses, this.conf);

      for (int i = 0; i < results.length; i++) {  // process results of call
        InetSocketAddress addr = defaultAddresses[i];
        String[] segments = results[i];
        if (segments == null) {
          LOG.warning("Client: no segments from: " + addr);
          continue;
        }
        for (int j = 0; j < segments.length; j++) {
          LOG.finest("Client: segment "+segments[j]+" at "+addr);
          segmentToAddress.put(segments[j], addr);
        }
        liveAddresses.add(addr);
        liveServers++;
        liveSegments+=segments.length;
      }

      this.liveAddresses = (InetSocketAddress[]) // update liveAddresses
        liveAddresses.toArray(new InetSocketAddress[liveAddresses.size()]);

      LOG.info("STATS: "+liveServers+" servers, "+liveSegments+" segments.");
    }

    /** Return the names of segments searched. */
    public String[] getSegmentNames() {
      return (String[])
        segmentToAddress.keySet().toArray(new String[segmentToAddress.size()]);
    }

    public Hits search(final Query query, final int numHits,
                       final String dedupField, final String sortField,
                       final boolean reverse) throws IOException {
      long totalHits = 0;
      Hits[] segmentHits = new Hits[liveAddresses.length];

      Object[][] params = new Object[liveAddresses.length][5];
      for (int i = 0; i < params.length; i++) {
        params[i][0] = query;
        params[i][1] = new Integer(numHits);
        params[i][2] = dedupField;
        params[i][3] = sortField;
        params[i][4] = Boolean.valueOf(reverse);
      }
      Hits[] results = (Hits[])RPC.call(SEARCH, params, liveAddresses, this.conf);

      TreeSet queue;                              // cull top hits from results

      if (sortField == null || reverse) {
        queue = new TreeSet(new Comparator() {
            public int compare(Object o1, Object o2) {
              return ((Comparable)o2).compareTo(o1); // reverse natural order
            }
          });
      } else {
        queue = new TreeSet();
      }
      
      Comparable maxValue = null;
      for (int i = 0; i < results.length; i++) {
        Hits hits = results[i];
        if (hits == null) continue;
        totalHits += hits.getTotal();
        for (int j = 0; j < hits.getLength(); j++) {
          Hit h = hits.getHit(j);
          if (maxValue == null ||
              ((reverse || sortField == null)
               ? h.getSortValue().compareTo(maxValue) >= 0
               : h.getSortValue().compareTo(maxValue) <= 0)) {
            queue.add(new Hit(i, h.getIndexDocNo(),
                              h.getSortValue(), h.getDedupValue()));
            if (queue.size() > numHits) {         // if hit queue overfull
              queue.remove(queue.last());         // remove lowest in hit queue
              maxValue = ((Hit)queue.last()).getSortValue(); // reset maxValue
            }
          }
        }
      }
      return new Hits(totalHits, (Hit[])queue.toArray(new Hit[queue.size()]));
    }
    
    private Protocol getRemote(Hit hit) {
      return (Protocol)
        RPC.getProxy(Protocol.class, liveAddresses[hit.getIndexNo()], conf);
    }

    private Protocol getRemote(HitDetails hit) {
      InetSocketAddress address =
        (InetSocketAddress)segmentToAddress.get(hit.getValue("segment"));
      return (Protocol)RPC.getProxy(Protocol.class, address, conf);
    }

    public String getExplanation(Query query, Hit hit) throws IOException {
      return getRemote(hit).getExplanation(query, hit);
    }
    
    public HitDetails getDetails(Hit hit) throws IOException {
      return getRemote(hit).getDetails(hit);
    }
    
    public HitDetails[] getDetails(Hit[] hits) throws IOException {
      InetSocketAddress[] addrs = new InetSocketAddress[hits.length];
      Object[][] params = new Object[hits.length][1];
      for (int i = 0; i < hits.length; i++) {
        addrs[i] = liveAddresses[hits[i].getIndexNo()];
        params[i][0] = hits[i];
      }
      return (HitDetails[])RPC.call(DETAILS, params, addrs, conf);
    }


    public Summary getSummary(HitDetails hit, Query query) throws IOException {
      return getRemote(hit).getSummary(hit, query);
    }

    public Summary[] getSummary(HitDetails[] hits, Query query)
      throws IOException {
      InetSocketAddress[] addrs = new InetSocketAddress[hits.length];
      Object[][] params = new Object[hits.length][2];
      for (int i = 0; i < hits.length; i++) {
        HitDetails hit = hits[i];
        addrs[i] =
          (InetSocketAddress)segmentToAddress.get(hit.getValue("segment"));
        params[i][0] = hit;
        params[i][1] = query;
      }
      return (Summary[])RPC.call(SUMMARY, params, addrs, conf);
    }
    
    public byte[] getContent(HitDetails hit) throws IOException {
      return getRemote(hit).getContent(hit);
    }
    
    public ParseData getParseData(HitDetails hit) throws IOException {
      return getRemote(hit).getParseData(hit);
    }
      
    public ParseText getParseText(HitDetails hit) throws IOException {
      return getRemote(hit).getParseText(hit);
    }
      
    public String[] getAnchors(HitDetails hit) throws IOException {
      return getRemote(hit).getAnchors(hit);
    }

    public Inlinks getInlinks(HitDetails hit) throws IOException {
      return getRemote(hit).getInlinks(hit);
    }

    public long getFetchDate(HitDetails hit) throws IOException {
      return getRemote(hit).getFetchDate(hit);
    }
      
    public static void main(String[] args) throws Exception {
      String usage = "DistributedSearch$Client query <host> <port> ...";

      if (args.length == 0) {
        System.err.println(usage);
        System.exit(-1);
      }

      Query query = Query.parse(args[0], NutchConfiguration.create());
      
      InetSocketAddress[] addresses = new InetSocketAddress[(args.length-1)/2];
      for (int i = 0; i < (args.length-1)/2; i++) {
        addresses[i] =
          new InetSocketAddress(args[i*2+1], Integer.parseInt(args[i*2+2]));
      }

      Client client = new Client(addresses, NutchConfiguration.create());
      //client.setTimeout(Integer.MAX_VALUE);

      Hits hits = client.search(query, 10, null, null, false);
      System.out.println("Total hits: " + hits.getTotal());
      for (int i = 0; i < hits.getLength(); i++) {
        System.out.println(" "+i+" "+ client.getDetails(hits.getHit(i)));
      }

    }

    public void run() {
      while (running){
        try{
          Thread.sleep(10000);
        } catch (InterruptedException ie){
          LOG.info("Thread sleep interrupted.");
        }
        try{
          LOG.info("Querying segments from search servers...");
          updateSegments();
        } catch (IOException ioe) {
          LOG.warning("No search servers available!");
          liveAddresses=new InetSocketAddress[0];
        }
      }
    }
    
    /**
     * Stops the watchdog thread.
     */
    public void close() {
      running = false;
      interrupt();
    }
  }
}
