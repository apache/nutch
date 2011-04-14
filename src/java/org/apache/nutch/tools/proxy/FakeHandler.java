/*******************************************************************************
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
 ******************************************************************************/
package org.apache.nutch.tools.proxy;
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

import java.io.IOException;
import java.io.OutputStream;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletResponse;

import org.mortbay.jetty.HttpURI;
import org.mortbay.jetty.Request;

public class FakeHandler extends AbstractTestbedHandler {
  /** Create links to hosts generated from a pool of numHosts/numPages random names. */
  public static enum Mode {UNIQUE, RANDOM};
    
  int numInternalLinks;
  int numExternalLinks;
  Mode hostMode;
  Mode pageMode;
  AtomicLong hostSeq = new AtomicLong(0);
  AtomicLong pageSeq = new AtomicLong(0);
  int numHosts;
  int numPages;
  
  Random r = new Random(1234567890L); // predictable
  Random pageR;

  private static final String testA = 
    "<html><body><h1>Internet Weather Forecast Accuracy</h1>\n" + 
    "<p>Weather forecasting is a secure and popular online presence, which is understandable. The weather affects most everyone's life, and the Internet can provide information on just about any location at any hour of the day or night. But how accurate is this information? How much can we trust it? Perhaps it is just my skeptical nature (or maybe the seeming unpredictability of nature), but I've never put much weight into weather forecasts - especially those made more than three days in advance. That skepticism progressed to a new high in the Summer of 2004, but I have only now done the research necessary to test the accuracy of online weather forecasts. First the story, then the data.</p>" +
    "<h2>An Internet Weather Forecast Gone Terribly Awry</h2>" +
    "<p>It was the Summer of 2004 and my wife and I were gearing up for a trip with another couple to Schlitterbahn in New Braunfels - one of the (if not the) best waterparks ever created. As a matter of course when embarking on a 2.5-hour drive to spend the day in a swimsuit, and given the tendency of the area for natural disasters, we checked the weather. The temperatures looked ideal and, most importantly, the chance of rain was a nice round goose egg.</p>";
  private static final String testB =
    "<p>A couple of hours into our Schlitterbahn experience, we got on a bus to leave the 'old section' for the 'new section.' Along the way, clouds gathered and multiple claps of thunder sounded. 'So much for the 0% chance of rain,' I commented. By the time we got to our destination, lightning sightings had led to the slides and pools being evacuated and soon the rain began coming down in torrents - accompanied by voluminous lightning flashes. After at least a half an hour the downpour had subsided, but the lightning showed no sign of letting up, so we began heading back to our vehicles. A hundred yards into the parking lot, we passing a tree that had apparently been split in two during the storm (whether by lightning or wind, I'm not sure). Not but a few yards later, there was a distinct thud and the husband of the couple accompanying us cried out as a near racquetball sized hunk of ice rebounded off of his head and onto the concrete. Soon, similarly sized hail was falling all around us as everyone scampered for cover. Some cowered under overturned trashcans while others were more fortunate and made it indoors.</p>" +
    "<p>The hail, rain and lightning eventually subsided, but the most alarming news was waiting on cell phone voicemail. A friend who lived in the area had called frantically, knowing we were at the park, as the local news was reporting multiple people had been by struck by lightning at Schlitterbahn during the storm.</p>" +
    "<p>'So much for the 0% chance of rain,' I repeated.</p></body></html>";

  /**
   * Create fake pages.
   * @param hostMode if UNIQUE then each external outlink will use a unique host name. If
   * RANDOM then each outlink will use a host name allocated from pool of numHosts.
   * @param pageMode if UNIQUE then each internal outlinks will use a unique page name.
   * if RANDOM then each outlink will use a page name allocated from pool of numPages.
   * @param numInternalLinks
   * @param numExternalLinks
   * @param numHosts
   * @param numPages
   */
  public FakeHandler(Mode hostMode, Mode pageMode,
      int numInternalLinks, int numExternalLinks,
      int numHosts, int numPages) {
    this.numExternalLinks = numExternalLinks;
    this.numInternalLinks = numInternalLinks;
    this.numHosts = numHosts;
    this.numPages = numPages;
    this.hostMode = hostMode;
    this.pageMode = pageMode;
  }
  
  @Override
  public void handle(Request req, HttpServletResponse res, String target, 
          int dispatch) throws IOException, ServletException {
    HttpURI u = req.getUri();
    String uri = u.toString();
    addMyHeader(res, "URI", uri);
    // don't pass it down the chain
    req.setHandled(true);
    res.addHeader("X-Handled-By", getClass().getSimpleName());
    if (uri.endsWith("/robots.txt")) {
      return;
    }
    res.setContentType("text/html");
    try {
      OutputStream os = res.getOutputStream();
      byte[] bytes = testA.getBytes("UTF-8");
      os.write(bytes);
      // record URI
      String p = "<p>URI: " + uri + "</p>\r\n";
      os.write(p.getBytes());
      // fake some links
      String basePath;
      String baseDomain;
      if (u.getPath().length() > 5) {
        basePath = u.getPath().substring(0, u.getPath().length() - 5);
      } else {
        basePath = u.getPath();
      }
      // internal links
      if (pageMode.equals(Mode.RANDOM)) { // initialize random per host
        pageR = new Random(u.getHost().hashCode());
      }
      for (int i = 0; i < numInternalLinks; i++) {
        String link = "<p><a href='";
        if (pageMode.equals(Mode.RANDOM)) {
          link += pageR.nextInt (numPages) + ".html'>";
        } else {
          if (!basePath.endsWith("/")) {
            link += "/";
          }
          link += pageSeq.getAndIncrement() + ".html'>";
        }
        link += "outlink " + i + "</a></p>\r\n";
        os.write(link.getBytes());
      }
      baseDomain = u.getHost();
      // chop off the TLD
      int pos = baseDomain.lastIndexOf('.');
      String tld = baseDomain.substring(pos);
      baseDomain = baseDomain.substring(0, pos);
      String link;
      // external links
      for (int i = 0; i < numExternalLinks; i++) {
        String host;
        if (hostMode.equals(Mode.RANDOM)) {
          host = "www.rnd-" + r.nextInt(numHosts) + ".com";
          link = "http://" + host + "/";
        } else {
          host = baseDomain + "-" + hostSeq.getAndIncrement() + ".com";
          link = "http://" + host + "/";
        }
        link = "<p><a href='" + link + "'>fake host " + host + "</a></p>\r\n";
        os.write(link.getBytes());
      }
      // fake a link to the root URL
      link = "<p><a href='" + u.getScheme() + "://" + u.getHost();
      if (u.getPort() != 80 && u.getPort() != -1) link += ":" + u.getPort();
      link += "/'>site " + u.getHost() + "</a></p>\r\n";
      os.write(link.getBytes());
      os.write(testB.getBytes());
      res.flushBuffer();
    } catch (IOException ioe) {
    }    
  }

}
