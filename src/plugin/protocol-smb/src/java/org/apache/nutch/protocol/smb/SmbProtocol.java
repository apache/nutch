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
package org.apache.nutch.protocol.smb;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.net.protocols.HttpDateFormat;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.Protocol;
import org.apache.nutch.protocol.ProtocolOutput;
import org.apache.nutch.protocol.ProtocolStatus;
import org.apache.nutch.protocol.RobotRulesParser;
import org.apache.nutch.protocol.smb.URLAuthentication.Authentication;
import org.xml.sax.InputSource;
import com.hierynomus.msdtyp.AccessMask;
import com.hierynomus.mserref.NtStatus;
import com.hierynomus.msfscc.FileAttributes;
import com.hierynomus.msfscc.fileinformation.FileAllInformation;
import com.hierynomus.msfscc.fileinformation.FileIdBothDirectoryInformation;
import com.hierynomus.mssmb2.SMBApiException;
import com.hierynomus.mssmb2.SMB2CreateDisposition;
import com.hierynomus.mssmb2.SMB2CreateOptions;
import com.hierynomus.mssmb2.SMB2ShareAccess;
import com.hierynomus.smbj.auth.AuthenticationContext;
import com.hierynomus.smbj.connection.Connection;
import com.hierynomus.smbj.session.Session;
import com.hierynomus.smbj.share.DiskShare;
import com.hierynomus.smbj.share.File;
import com.hierynomus.smbj.SMBClient;

import crawlercommons.robots.BaseRobotRules;
import crawlercommons.robots.SimpleRobotRules;
import crawlercommons.robots.SimpleRobotRulesParser;
import java.util.Map;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SmbProtocol implements Protocol, AutoCloseable {
  protected static final Logger LOG = LoggerFactory.getLogger(SmbProtocol.class);

  private Configuration conf;
  private URLAuthentication urlAuthentication;

  private int contentLimit;
  private Set<String> ignoreFiles;
  private Collection<String> agentNames;
  
  private long scannedFolderCount;
  private long scannedFileCount;
  private long truncatedFileCount;
  
  private Map<String, BaseRobotRules> robotsCache = new TreeMap<>();

  public SmbProtocol() {
    // Place here only files that SMB needs to ignore. Other files such as
    // version control (.git, .svn) can be ignored via the regex url filter.
    this.ignoreFiles = new HashSet<>();
    ignoreFiles.add(".");
    ignoreFiles.add("..");
  }

  @Override
  public Configuration getConf() {
    LOG.debug("getConf()");
    return this.conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;

    agentNames = conf.getTrimmedStringCollection("smb.agent.name");
    if (agentNames == null || agentNames.isEmpty()) {
      throw new IllegalArgumentException("Config parameter 'smb.agent.name' not set or empty.");
    }

    // load authentication data
    String filename = conf.get("smb.url-authentication.file", "url-authentication.xml");
    InputStream ssInputStream = conf.getConfResourceAsInputStream(filename);
    InputSource inputSource = new InputSource(ssInputStream);
    urlAuthentication = URLAuthentication.loadAuthentication(inputSource);

    contentLimit = conf.getInt("smb.content.limit", Integer.MAX_VALUE-100);
    LOG.info("Understood smb.content.limit={}", contentLimit);
  }

  /**
   * list directory.
   * 
   * @return some HTML string
   */
  private String getDirectoryContent(DiskShare share, String shareName, String path) throws UnsupportedEncodingException {
      StringBuffer sb = new StringBuffer();
      sb.append("<html><head>");
      sb.append("<title>Index of ").append("/").append(shareName).append(path).append("</title>");
      sb.append("</head><body>");
      sb.append("<h1>Index of ").append("/").append(shareName).append(path).append("</h1>");
      sb.append("<pre>");
      for (FileIdBothDirectoryInformation f : share.list(path)) {
        if (ignoreFiles.contains(f.getFileName())) {
          LOG.debug("File skipped: " + f.getFileName());
          continue;
        }
        boolean isDir = share.folderExists(path + "/" + f.getFileName());

        sb.append("<a href=\"").append(java.net.URLEncoder.encode(f.getFileName(), StandardCharsets.UTF_8.name()));
        if (isDir) {
          sb.append("/");
        }
        sb.append("\">").append(f.getFileName());
        if (isDir) {
          sb.append("/");
        }
        sb.append("\t").append(f.getLastWriteTime()).append("</a>\n");
      }
      sb.append("</pre>");
      sb.append("</body></html>");

    return sb.toString();
  }

  private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();

  private Connection getSMBConnection(URL url) throws UnsupportedEncodingException, IOException {
    String hostname = url.getHost();
    int port = url.getPort();
    String shareAndPath = url.getPath();

    if (port == -1) {
      port = 445;
    }
    String[] components = shareAndPath.split("/", 3);
    String shareName = components[1];
    shareName = java.net.URLDecoder.decode(shareName, StandardCharsets.UTF_8.name());
    String path = components.length>2 ? "/" + components[2]: "/";
    path = java.net.URLDecoder.decode(path, StandardCharsets.UTF_8.name());

    LOG.trace("hostname={}", hostname);
    LOG.trace("port={}", port);
    LOG.trace("shareAndPath={}", shareAndPath);
    LOG.trace("share={}", shareName);
    LOG.trace("path={}", path);

    // todo: we construct and destruct the connection for each and every URL. Can connection pools improve?
    SMBClient client = new SMBClient();
    Connection connection = client.connect(hostname, port);
    return connection;
  }
  
  private URL getRobotsUrl(URL url) throws URISyntaxException, MalformedURLException {
    String shareAndPath = url.getPath();
    String[] components = shareAndPath.split("/", 3);
    String shareName = components[1];
    return new URI(url.getProtocol(), url.getUserInfo(), url.getHost(), url.getPort(), "/" + shareName + "/robots.txt", null, null).toURL();
  }

  private DiskShare getDiskShare(URL url, Connection connection) throws UnsupportedEncodingException, IOException {
    if (urlAuthentication == null) {
      throw new IllegalStateException("urlAuthentication must not be null");
    }

    String shareAndPath = url.getPath();
    String[] components = shareAndPath.split("/", 3);
    String shareName = components[1];
    shareName = java.net.URLDecoder.decode(shareName, StandardCharsets.UTF_8.name());
    String path = components.length>2 ? "/" + components[2]: "/";
    path = java.net.URLDecoder.decode(path, StandardCharsets.UTF_8.name());

    LOG.trace("shareAndPath={}", shareAndPath);
    LOG.trace("share={}", shareName);
    LOG.trace("path={}", path);

    Authentication auth = urlAuthentication.getAuthenticationFor(url.toString());
    Session session = null;
    if (auth == null) {
      LOG.trace("Anonymously connecting to {}", url);
      session = connection.authenticate(
        AuthenticationContext.anonymous()
      );
    } else {
      LOG.trace("Authenticating with {}", auth);
      session = connection.authenticate(
        new AuthenticationContext(auth.getUser(), auth.getPassword(), auth.getDomain())
      );
    }
    // Connect to Share
    DiskShare share = (DiskShare) session.connectShare(shareName);
    return share;
  }

  /**
   * Splits an absolute path into share and path.
   * The share is the top level directory, everything else will become the path.
   * Since the whole structure can be transported via URLs, URL-decoding is also
   * applied.
   * 
   * @param url the url to parse
   * @return an array consisting of [share, path]
   */
  private String[] getSmbShareAndPath(URL url) throws UnsupportedEncodingException {
    String shareAndPath = url.getPath();
    String[] components = shareAndPath.split("/", 3);
    String shareName = components[1];
    shareName = java.net.URLDecoder.decode(shareName, StandardCharsets.UTF_8.name());
    String path = components.length>2 ? "/" + components[2]: "/";
    path = java.net.URLDecoder.decode(path, StandardCharsets.UTF_8.name());

    return new String[]{shareName, path};
  }

  private Content getFileContent(String urlstr, String base, DiskShare share, String path, Metadata metadata) throws IOException {
    FileAllInformation fileInfo = share.getFileInformation(path);
    File file = share.openFile(path, EnumSet.of(AccessMask.GENERIC_READ), null, SMB2ShareAccess.ALL, SMB2CreateDisposition.FILE_OPEN, null);

    InputStream fileIn = file.getInputStream();
    byte[] bytes = null;
    long fileSize = fileInfo.getStandardInformation().getEndOfFile();
    long fetchSize = fileSize;
    metadata.add("fileSize", String.valueOf(fileSize));

    // todo: we run into issues if the file is bigger than 2 GB. I made the limit configurable
    // but e.g. zip can no longer be evaluated if too big.
    if (fetchSize > contentLimit) {
      LOG.info("trunkating {}", urlstr);
      fetchSize = contentLimit;

      // todo: this metadata seems to be not available for the indexer. However it might be useful to know the content
      // discovery is incomplete
      metadata.add("truncated", String.valueOf(fetchSize));
      truncatedFileCount++;
    }

    bytes = IOUtils.toByteArray(fileIn, fetchSize); // read inputstream into byte array

    LOG.trace("retrieved {} bytes", bytes.length);

    if (LOG.isTraceEnabled()) {
      StringBuilder sb = new StringBuilder();
      for (int i=0; i<Math.min(16, fetchSize);i++) {
        int b = bytes[i] & 0xFF;
        sb.append(" ").append(HEX_ARRAY[b>>>4]).append(HEX_ARRAY[b & 0xF]);
      }
      LOG.trace("retrieved {} bytes starting with {}", bytes.length, sb.toString());
    }
    LOG.trace("metadata={}", metadata);

    return new Content(urlstr, base, bytes, "application/octet-stream", metadata, getConf());
  }

  private String getBase(Text urlstr) {
          // construct a suitable base
          String base = urlstr.toString();
          if (base.endsWith("/")) {
            base = base + ".";
          }
          if (!base.endsWith("/.")) {
            base = base + "/.";
          }

          LOG.trace("base={}", base);
          return base;
  }

  /**
   * Get the {@link ProtocolOutput} for a given url and crawldatum.
   * 
   * @param url canonical url
   * @param datum associated {@link org.apache.nutch.crawl.CrawlDatum}
   * @return the {@link ProtocolOutput}
   * @see https://github.com/apache/nutch/blob/master/src/java/org/apache/nutch/crawl/CrawlDatum.java
   */
  @Override
  public ProtocolOutput getProtocolOutput(Text urlstr, CrawlDatum datum) {
    LOG.debug("getProtocolOutput({}, {})", urlstr, datum);

    try {
      URL url = new URI(urlstr.toString()).toURL();
      String[] components = getSmbShareAndPath(url);
      String shareName = components[0];
      String path = components[1];

      try (Connection connection = getSMBConnection(url)) {
        String base = base = getBase(urlstr);

        try (DiskShare share = getDiskShare(url, connection)) {

          // now get the content
          if (share.folderExists(path)) {
            String htmlContent = getDirectoryContent(share, shareName, path);
            LOG.trace("directory={}", htmlContent);
            scannedFolderCount++;

            return new ProtocolOutput(
              new Content(urlstr.toString(), base, htmlContent.getBytes(), "text/html", new Metadata(), getConf()), 
                ProtocolStatus.STATUS_SUCCESS
              );
          } else if (share.fileExists(path)) {
            // todo: how can we store this, and maybe more metadata?
            Metadata metadata = new Metadata();
            metadata.set(Metadata.CONTENT_TYPE, "application/octet-stream");

            Content content = getFileContent(urlstr.toString(), url.toURI().resolve("..").toString(), share, path, metadata);
            scannedFileCount++;

            // create content and return result
            return new ProtocolOutput(
              content, 
              ProtocolStatus.STATUS_SUCCESS
            );

          } else {
            // communicate error
            String message = "File not found: " + urlstr;
            LOG.info(message);
            return new ProtocolOutput(
              new Content(urlstr.toString(), base, message.getBytes(), "text/plain", new Metadata(), getConf()),
              ProtocolStatus.STATUS_NOTFOUND
            );
          }
        } catch (SMBApiException e) {
          if (e.getStatus() == NtStatus.STATUS_BAD_NETWORK_NAME) {

            // this URL makes to sense to be scanned. Make sure this URL gets evicted from the CrawlDB.
            LOG.error("Bad network name: {}", urlstr);
            return new ProtocolOutput(
              new Content(urlstr.toString(), base, e.getMessage().getBytes(), "text/plain", new Metadata(), getConf()),
              ProtocolStatus.STATUS_NOTFOUND
            );
          } else {
            throw e;
          }
        }
      }

    } catch(Exception e) {
      LOG.error("Could not get protocol output for {}", urlstr, e);
      return new ProtocolOutput(null, new ProtocolStatus(e));
    }
  }

  /**
   * Retrieve robot rules applicable for this URL.
   *
   * @param url
   *          URL to check
   * @param datum
   *          page datum
   * @param robotsTxtContent
   *          container to store responses when fetching the robots.txt file for
   *          debugging or archival purposes. Instead of a robots.txt file, it
   *          may include redirects or an error page (404, etc.). Response
   *          {@link Content} is appended to the passed list. If null is passed
   *          nothing is stored.
   * @return robot rules (specific for this URL or default), never null
   */
  @Override
  public BaseRobotRules getRobotRules(Text urlstr, CrawlDatum datum, List<Content> robotsTxtContent) {
    LOG.trace("getRobotRules({}, {}, {})", urlstr, datum, robotsTxtContent);

    URL url = null;
    URL robotsURL = null;
    try {
      // calculate new URL
      url = new URI(urlstr.toString()).toURL();
      robotsURL = getRobotsUrl(url);
      LOG.debug("Robots URL = {}", robotsURL);

      
      // if we are running multithreaded, make only one thread at a time check
      // the cache. It means if we miss, only one thread will go and fetch/parse
      // robots.txt while other threads will wait
      synchronized(robotsCache) {          
        if (robotsCache.containsKey(robotsURL.toString())) {
            LOG.debug("Found {} in cache", robotsURL);
            return robotsCache.get(robotsURL.toString());
        }
      
        try (Connection connection = getSMBConnection(url)) {
          try (DiskShare share = getDiskShare(url, connection)) {
            // search for the file compliant to https://www.rfc-editor.org/rfc/rfc9309.html
            // chapter 2.3
            if (!share.fileExists("/robots.txt")) {
              // no robots file? Then we can scan everything
              LOG.info("No robots.txt found for {} -> crawl everything", robotsURL);
              BaseRobotRules rules = RobotRulesParser.EMPTY_RULES;
              robotsCache.put(robotsURL.toString(), rules); // cache the value - we will need it more often
              return rules;
            }

            Metadata metadata = new Metadata();
            Content content = getFileContent(urlstr.toString(), url.toURI().resolve("..").toString(), share, "/robots.txt", metadata);

            // make use of
            // https://crawler-commons.github.io/crawler-commons/1.4/crawlercommons/robots/SimpleRobotRulesParser.html#parseContent(java.lang.String,byte%5B%5D,java.lang.String,java.util.Collection)
            SimpleRobotRulesParser simpleRobotsRulesParser = new SimpleRobotRulesParser();
            SimpleRobotRules rules =  simpleRobotsRulesParser.parseContent(urlstr.toString(), content.getContent(), content.getContentType(), agentNames);
            robotsCache.put(robotsURL.toString(), rules); // cache the value - we will need it more often
            LOG.info("found and parsed {}", robotsURL);
            return rules;
          } catch (SMBApiException e) {
            if (e.getStatus() == NtStatus.STATUS_BAD_NETWORK_NAME) {

              // this URL makes to sense to be scanned. But we assume 'empty rules' as no robots.txt exists and
              // in getProtocolOutput we can make sure this URL gets evicted from the CrawlDB.
              LOG.error("Bad network name: {} -> crawl everything", urlstr);
              BaseRobotRules rules = RobotRulesParser.EMPTY_RULES;
              robotsCache.put(robotsURL.toString(), rules); // cache the value - we will need it more often
              return rules;
            } else {
              throw e;
            }
          } // DiskShare
        } // Connection
      } // synchronized
      
    } catch (Exception e) {
      LOG.info("Could not get robot rules for {} (initially {})", robotsURL, urlstr, e);
      return RobotRulesParser.DEFER_VISIT_RULES;
    }
  }

  /**
   * Closes this resource, relinquishing any underlying resources.
   * 
   * Some statistics is printed.
   */
  public void close() {
    LOG.info("Closing plugin");
    LOG.info("Scanned folders: {}", scannedFolderCount);
    LOG.info("Scanned files    {}", scannedFileCount);
    LOG.info("Truncated files  {}", truncatedFileCount);
  }
  
  /**
   * As Nutch does not close protocols let's do that before GC.
   */
  public void finalize() {
      close();
  }
}
