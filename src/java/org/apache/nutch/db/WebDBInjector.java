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

package org.apache.nutch.db;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.logging.*;
import java.net.MalformedURLException;
import java.util.regex.*;

import javax.xml.parsers.*;
import org.xml.sax.*;
import org.xml.sax.helpers.*;
import org.apache.xerces.util.XMLChar;

import org.apache.nutch.io.*;
import org.apache.nutch.fs.*;
import org.apache.nutch.net.*;
import org.apache.nutch.util.*;
import org.apache.nutch.pagedb.*;
import org.apache.nutch.linkdb.*;
import org.apache.nutch.util.NutchConf;

/*********************************************
 * This class takes a flat file of URLs and adds
 * them as entries into a pagedb.  Useful for 
 * bootstrapping the system.
 *
 * @author Mike Cafarella
 * @author Doug Cutting
 *********************************************/
public class WebDBInjector {
    private static final String DMOZ_PAGENAME = "http://www.dmoz.org/";

    private static final byte DEFAULT_INTERVAL =
      (byte)NutchConf.get().getInt("db.default.fetch.interval", 30);

    private static final float NEW_INJECTED_PAGE_SCORE =
      NutchConf.get().getFloat("db.score.injected", 2.0f);

    public static final Logger LOG = LogFormatter.getLogger("org.apache.nutch.db.WebDBInjector");

    /**
     * This filter fixes characters that might offend our parser.
     * This lets us be tolerant of errors that might appear in the input XML.
     */
    private static class XMLCharFilter extends FilterReader {
      private boolean lastBad = false;

      public XMLCharFilter(Reader reader) {
        super(reader);
      }

      public int read() throws IOException {
        int c = in.read();
        int value = c;
        if (c != -1 && !(XMLChar.isValid(c)))     // fix invalid characters
          value = 'X';
        else if (lastBad && c == '<') {           // fix mis-matched brackets
          in.mark(1);
          if (in.read() != '/')
            value = 'X';
          in.reset();
        }
        lastBad = (c == 65533);

        return value;
      }

      public int read(char[] cbuf, int off, int len)
        throws IOException {
        int n = in.read(cbuf, off, len);
        if (n != -1) {
          for (int i = 0; i < n; i++) {
            char c = cbuf[off+i];
            char value = c;
            if (!(XMLChar.isValid(c)))            // fix invalid characters
              value = 'X';
            else if (lastBad && c == '<') {       // fix mis-matched brackets
              if (i != n-1 && cbuf[off+i+1] != '/')
                value = 'X';
            }
            lastBad = (c == 65533);
            cbuf[off+i] = value;
          }
        }
        return n;
      }
    }


    /**
     * The RDFProcessor receives tag messages during a parse
     * of RDF XML data.  We build whatever structures we need
     * from these messages.
     */
    class RDFProcessor extends DefaultHandler {
        String curURL = null, curSection = null;
        boolean titlePending = false, descPending = false, insideAdultSection = false;
        Pattern topicPattern = null; 
        StringBuffer title = new StringBuffer(), desc = new StringBuffer();
        XMLReader reader;
        int subsetDenom;
        int hashSkew;
        boolean includeAdult, includeDmozDesc;
        MD5Hash srcDmozID;
        long srcDmozDomainID;
        Locator location;

        /**
         * Pass in an XMLReader, plus a flag as to whether we 
         * should include adult material.
         */
        public RDFProcessor(XMLReader reader, int subsetDenom, boolean includeAdult, boolean includeDmozDesc, int skew, Pattern topicPattern) throws IOException {
            this.reader = reader;
            this.subsetDenom = subsetDenom;
            this.includeAdult = includeAdult;
            this.includeDmozDesc = includeDmozDesc;
            this.topicPattern = topicPattern;

            // We create a Page entry for the "Dmoz" page, from
            // which all descriptive links originate.  The name
            // of this page is always the same, stored in 
            // DMOZ_PAGENAME.  The MD5 is generated over the current
            // timestamp.  Until this page is deleted, the descriptive
            // links will always be kept.
            //
            // If the DMOZ page is updated with new content, you 
            // *could* update these links, if you really wanted to.
            // Just run inject again!  This will replace the old
            // Dmoz Page, because we always keep the same name.
            // That obsolete Page will be deleted, and all its 
            // outlinks (the descriptive ones) garbage-collected.
            // 
            // Then we just proceed to add the new descriptive 
            // links, with the brand-new page's src MD5.
            //
            this.srcDmozID = MD5Hash.digest(DMOZ_PAGENAME + "_" + nextFetch);
            Page dmozPage = new Page(DMOZ_PAGENAME, srcDmozID);
            dmozPage.setNextFetchTime(Long.MAX_VALUE);
            dbWriter.addPageIfNotPresent(dmozPage);

            this.srcDmozDomainID = MD5Hash.digest(new URL(DMOZ_PAGENAME).getHost()).halfDigest();

            this.hashSkew = skew != 0 ? skew : new Random().nextInt();
        }

        //
        // Interface ContentHandler
        //

        /**
         * Start of an XML elt
         */
        public void startElement(String namespaceURI, String localName, String qName, Attributes atts) throws SAXException {
            if ("Topic".equals(qName)) {
                curSection = atts.getValue("r:id");
            } else if ("ExternalPage".equals(qName)) {
                // Porn filter
                if ((! includeAdult) && curSection.startsWith("Top/Adult")) {
                    return;
                }
          
                if (topicPattern != null && !topicPattern.matcher(curSection).matches()) {
                   return;
                }

                // Subset denominator filter.  
                // Only emit with a chance of 1/denominator.
                String url = atts.getValue("about");
                int hashValue = MD5Hash.digest(url).hashCode();
                hashValue = Math.abs(hashValue ^ hashSkew);
                if ((hashValue % subsetDenom) != 0) {
                    return;
                }

                // We actually claim the URL!
                curURL = url;
            } else if (curURL != null && "d:Title".equals(qName)) {
                titlePending = true;
            } else if (curURL != null && "d:Description".equals(qName)) {
                descPending = true;
            }
        }

        /**
         * The contents of an XML elt
         */
        public void characters(char ch[], int start, int length) {
            if (titlePending) {
                title.append(ch, start, length);
            } else if (descPending) {
                desc.append(ch, start, length);
            }
        }

        /**
         * Termination of XML elt
         */
        public void endElement(String namespaceURI, String localName, String qName) throws SAXException {
            if (curURL != null) {
                if ("ExternalPage".equals(qName)) {
                    //
                    // Inc the number of pages, insert the page, and 
                    // possibly print status.
                    //
                    try {
                      // First, manufacture the Page entry for the
                      // given DMOZ listing.
                      if (addPage(curURL)) {

                        // Second, add a link from the DMOZ page TO the
                        // just-added target Page.  The anchor text should 
                        // be the merged Title and Desc that we get from 
                        // the DMOZ listing.  For testing reasons, the 
                        // caller may choose to disallow this.
                        if (includeDmozDesc) {
                          String fullDesc = title + " " + desc;
                          Link descLink = new Link(srcDmozID, srcDmozDomainID, curURL, fullDesc);
                          dbWriter.addLink(descLink);
                        }
                        pages++;
                      }

                    } catch (MalformedURLException e) {
                        LOG.fine("skipping " + curURL + ":" + e);
                    } catch (IOException ie) {
                        LOG.severe("problem adding url " + curURL + ": " + ie);
                    }
                    printStatusBar(2000, 50000);

                    //
                    // Clear out the link text.  This is what
                    // you would use for adding to the linkdb.
                    //
                    if (title.length() > 0) {
                        title.delete(0, title.length());
                    }
                    if (desc.length() > 0) {
                        desc.delete(0, desc.length());
                    }

                    // Null out the URL.
                    curURL = null;
                } else if ("d:Title".equals(qName)) {
                    titlePending = false;
                } else if ("d:Description".equals(qName)) {
                    descPending = false;
                }
            }
        }

        /**
         * When parsing begins
         */
        public void startDocument() {
            LOG.info("Begin parse");
        }

        /**
         * When parsing ends
         */
        public void endDocument() {
            LOG.info("Completed parse.  Added " + pages + " pages.");
        }

        /**
         * From time to time the Parser will set the "current location"
         * by calling this function.  It's useful for emitting locations
         * for error messages.
         */
        public void setDocumentLocator(Locator locator) {
            location = locator;
        }


        //
        // Interface ErrorHandler
        //

        /**
         * Emit the exception message
         */
        public void error(SAXParseException spe) {
            LOG.severe("Error: " + spe.toString() + ": " + spe.getMessage());
            spe.printStackTrace(System.out);
        }

        /**
         * Emit the exception message, with line numbers
         */
        public void fatalError(SAXParseException spe) {
            LOG.severe("Fatal error: " + spe.toString() + ": " + spe.getMessage());
            LOG.severe("Last known line is " + location.getLineNumber() + ", column " + location.getColumnNumber());
            spe.printStackTrace(System.out);
        }
        
        /**
         * Emit exception warning message
         */
        public void warning(SAXParseException spe) {
            LOG.warning("Warning: " + spe.toString() + ": " + spe.getMessage());
            spe.printStackTrace(System.out);
        }
    }

    private IWebDBWriter dbWriter;

    /**
     * WebDBInjector takes a reference to a WebDBWriter that it should add to.
     */
    public WebDBInjector(IWebDBWriter dbWriter) {
        this.dbWriter = dbWriter;
    }

    /**
     * Close dbWriter and save changes
     */
    public void close() throws IOException {
        dbWriter.close();
    }

    /**
     * Utility to present small status bar
     */
    public void printStatusBar(int small, int big){
        if ((pages % small ) == 0) {
            System.out.print(".");
        }
        if ((pages % big ) == 0) {
            printStatus();
        }
    }

    long startTime = System.currentTimeMillis();
    long pages = 0;
    long nextFetch = System.currentTimeMillis();

    /**
     * Utility to present performance stats
     */
    public void printStatus(){
        long elapsed = (System.currentTimeMillis() - this.startTime); 
        if ( this.pages == 0) {
        } else {
            LOG.info("\t" + this.pages + "\t" + 
                     (int)((1000 *  pages)/elapsed) + " pages/second\t" );
        }
    }

    /**
     * Iterate through all the items in this flat text file and
     * add them to the db.
     */
    public void injectURLFile(File urlList) throws IOException {
        nextFetch = urlList.lastModified();
        BufferedReader reader = new BufferedReader(new FileReader(urlList));
        try {
            String curStr = null; 
            LOG.info("Starting URL processing");
            while ((curStr = reader.readLine()) != null) {
                String url = curStr.trim();
                if (addPage(url))
                  this.pages++;
                printStatusBar(2000,50000);
            }
            LOG.info("Added " + pages + " pages");
        } catch (Exception e) {
          LOG.severe("error while injecting:" + e);
          e.printStackTrace();
        } finally {
          reader.close();
        }
    }

    /**
     * Iterate through all the items in this structured DMOZ file.
     * Add each URL to the web db.
     */
    public void injectDmozFile(File dmozFile, int subsetDenom, boolean includeAdult, boolean includeDmozDesc, int skew, Pattern topicPattern) throws IOException, SAXException, ParserConfigurationException {
        nextFetch = dmozFile.lastModified();

        SAXParserFactory parserFactory = SAXParserFactory.newInstance();
        SAXParser parser = parserFactory.newSAXParser();
        XMLReader reader = parser.getXMLReader();

        // Create our own processor to receive SAX events
        RDFProcessor rp =
          new RDFProcessor(reader, subsetDenom, includeAdult, includeDmozDesc, skew, topicPattern);
        reader.setContentHandler(rp);
        reader.setErrorHandler(rp);
        LOG.info("skew = " + rp.hashSkew);

        //
        // Open filtered text stream.  The UTF8Filter makes sure that
        // only appropriate XML-approved UTF8 characters are received.
        // Any non-conforming characters are silently skipped.
        //
        XMLCharFilter in = new XMLCharFilter(new BufferedReader(new InputStreamReader(new BufferedInputStream(new FileInputStream(dmozFile)), "UTF-8")));
        try {
            InputSource is = new InputSource(in);
            reader.parse(is);
        } catch (Exception e) {
            LOG.severe(e.toString());
            e.printStackTrace(System.out);
            System.exit(0);
        } finally {
            in.close();
        }
    }

    /**
     * Add one page to WebDB. Changes are not saved to the db until
     * the <code>close()</code> is invoked. URLs are checked with the
     * URLFilter, and only those that pass are added.
     * @param url URL to be added
     * @return true on success, false otherwise (e.g. filtered out or
     * bad URL syntax).
     */
    public boolean addPage(String url) throws IOException {
      try {
        url = URLFilters.filter(url);
      } catch (URLFilterException e) {
        throw new IOException(e.getMessage());
      }
      if (url != null) {
        try {
          Page page = new Page(url, NEW_INJECTED_PAGE_SCORE, nextFetch);
          dbWriter.addPageIfNotPresent(page);
          return true;
        } catch (MalformedURLException e) {
          LOG.warning("bad url: "+url);
        }
      }
      return false;
    }

    private static void addTopicsFromFile(String topicFile, Vector topics) throws IOException {
      BufferedReader in = null;
      try {
        in = new BufferedReader(new InputStreamReader(new FileInputStream(topicFile), "UTF-8"));
        String line = null;
        while ((line = in.readLine()) != null) {
          topics.addElement(new String(line));
        }
      } 
      catch (Exception e) {
        LOG.severe(e.toString());
        e.printStackTrace(System.out);
        System.exit(0);
      } finally {
       in.close();
      }
    }
    

    /**
     * Command-line access.  User may add URLs via a flat text file
     * or the structured DMOZ file.  By default, we ignore Adult
     * material (as categorized by DMOZ).
     */
    public static void main(String argv[]) throws Exception {
      if (argv.length < 3) {
        System.out.println("Usage: WebDBInjector (-local | -ndfs <namenode:port>) <db_dir> (-urlfile <url_file> | -dmozfile <dmoz_file>) [-subset <subsetDenominator>] [-includeAdultMaterial] [-skew skew] [-noDmozDesc] [-topicFile <topic list file>] [-topic <topic> [-topic <topic> [...]]]");
        return;
      }

      //
      // Parse the command line, figure out what kind of
      // URL file we need to load
      //
      int subsetDenom = 1;
      int skew = 0;
      String command = null, loadfile = null;
      boolean includeAdult = false, includeDmozDesc = true;
      Pattern topicPattern = null; 
      Vector topics = new Vector(); 

      int i = 0;
      NutchFileSystem nfs = NutchFileSystem.parseArgs(argv, i);
      try {
          File root = new File(argv[i++]);

          for (; i < argv.length; i++) {
              if ("-urlfile".equals(argv[i]) || 
                  "-dmozfile".equals(argv[i])) {
                  command = argv[i];
                  loadfile = argv[i+1];
                  i++;
              } else if ("-includeAdultMaterial".equals(argv[i])) {
                  includeAdult = true;
              } else if ("-noDmozDesc".equals(argv[i])) {
                  includeDmozDesc = false;
              } else if ("-subset".equals(argv[i])) {
                  subsetDenom = Integer.parseInt(argv[i+1]);
                  i++;
              } else if ("-topic".equals(argv[i])) {
                  topics.addElement(argv[i+1]); 
                  i++;
              } else if ("-topicFile".equals(argv[i])) {
                  addTopicsFromFile(argv[i+1], topics);
                  i++;
              } else if ("-skew".equals(argv[i])) {
                  skew = Integer.parseInt(argv[i+1]);
                  i++;
              }
          }

          //
          // Create the webdbWriter, the injector, and then inject the
          // right kind of URL file.
          //
          IWebDBWriter writer = new WebDBWriter(nfs, root);
          WebDBInjector injector = new WebDBInjector(writer);
          try {
              if ("-urlfile".equals(command)) {
                  if (!topics.isEmpty()) {
                      System.out.println("You can't select URLs based on a topic when using a URL-file");
                  }
                  injector.injectURLFile(new File(loadfile));
              } else if ("-dmozfile".equals(command)) {
                  if (!topics.isEmpty()) {
                      String regExp = new String("^("); 
                      int j = 0;
                      for ( ; j < topics.size() - 1; ++j) {
                          regExp = regExp.concat((String) topics.get(j));
                          regExp = regExp.concat("|");
                      }
                      regExp = regExp.concat((String) topics.get(j));
                      regExp = regExp.concat(").*"); 
                      LOG.info("Topic selection pattern = " + regExp);
                      topicPattern = Pattern.compile(regExp); 
                  }
                  injector.injectDmozFile(new File(loadfile), subsetDenom, includeAdult, includeDmozDesc, skew, topicPattern);
              } else {
                  System.out.println("No command indicated.");
                  return;
              }
          } finally {
              injector.close();
          }
      } finally {
          nfs.close();
      }
    }
}
