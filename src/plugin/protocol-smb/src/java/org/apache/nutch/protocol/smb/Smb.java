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
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
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
import com.hierynomus.msdtyp.AccessMask;
import com.hierynomus.msfscc.FileAttributes;
import com.hierynomus.msfscc.fileinformation.FileAllInformation;
import com.hierynomus.msfscc.fileinformation.FileIdBothDirectoryInformation;
import com.hierynomus.mssmb2.SMB2CreateDisposition;
import com.hierynomus.mssmb2.SMB2CreateOptions;
import com.hierynomus.mssmb2.SMB2ShareAccess;
import com.hierynomus.smbj.auth.AuthenticationContext;
import com.hierynomus.smbj.connection.Connection;
import com.hierynomus.smbj.session.Session;
import com.hierynomus.smbj.share.DiskShare;
import com.hierynomus.smbj.share.File;
import com.hierynomus.smbj.SMBClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import crawlercommons.robots.BaseRobotRules;

public class Smb implements Protocol {
  protected static final Logger LOG = LoggerFactory.getLogger(Smb.class);

  private Configuration conf;

  private String user;
  private String password;
  private String domain;
  private int contentLimit;
  private Set<String> ignoreFiles;

  public Smb() {
    // todo: files that should be skipped could be configurable.
    this.ignoreFiles = new HashSet<>();
    ignoreFiles.add(".");
    ignoreFiles.add("..");
    ignoreFiles.add(".svn");
    ignoreFiles.add(".git");
  }

  @Override
  public Configuration getConf() {
    LOG.debug("getConf()");
    return this.conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;

    // todo: is it possible to use configuration "per server" or "per share"?
    user = conf.getTrimmed("smb.user");
    if (user == null || user.isEmpty()) {
      throw new IllegalArgumentException("Config parameter 'smb.user' not set.");
    }
    password = conf.getTrimmed("smb.password");
    if (password == null || password.isEmpty()) {
      throw new IllegalArgumentException("Config parameter 'smb.password' not set.");
    }
    domain = conf.getTrimmed("smb.domain");
    contentLimit = conf.getInt("smb.content.limit", Integer.MAX_VALUE);
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
          LOG.warn("File skipped: " + f.getFileName());
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
    LOG.warn("getProtocolOutput({}, {})", urlstr, datum);



    try {
        String u = java.net.URLDecoder.decode(urlstr.toString(), StandardCharsets.UTF_8.name());
        u = u.split("://")[1];
        LOG.warn("u={}", u);
        String[] components = u.split("[:/]", 2);
        String hostname = components[0];
        String shareAndPath = components[1];
        LOG.warn("hostname={}", hostname);
        LOG.warn("shareAndPath={}", shareAndPath);
        components = shareAndPath.split("/", 2);
        String shareName = components[0];
        String path = components.length>1 ? "/" + components[1]: "/";
        LOG.warn("share={}", shareName);
        LOG.warn("path={}", path);

        // todo: we construct and destruct the connection for each and every URL. Can connection pools improve?
        SMBClient client = new SMBClient();
        try(Connection connection = client.connect(hostname)) {

          AuthenticationContext ac = new AuthenticationContext(user, password.toCharArray(), domain);
          Session session = connection.authenticate(ac);

          // Connect to Share
          try (DiskShare share = (DiskShare) session.connectShare(shareName)) {

            // now get the content
            if (share.folderExists(path)) {
              String c = getDirectoryContent(share, shareName, path);

              String base = urlstr.toString();
              if (base.endsWith("/")) {
                base = base + ".";
              }
              if (!base.endsWith("/.")) {
                base = base + "/.";
              }

              LOG.warn("base={}", base);
              LOG.warn("directory={}", c);

              return new ProtocolOutput(
                new Content(base, base, c.getBytes(), "text/html", new Metadata(), getConf()), 
                ProtocolStatus.STATUS_SUCCESS
              );
            } else if (share.fileExists(path)) {
              // todo: how can we store this, and maybe more metadata?
              Metadata metadata = new Metadata();
              metadata.set(Metadata.CONTENT_TYPE, "application/octet-stream");

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
                LOG.warn("trunkating {}", urlstr);
                fetchSize = contentLimit;

                // todo: this metadata seems to be not available for the indexer. However it might be useful to know the content
                // discovery is incomplete
                metadata.add("truncated", String.valueOf(fetchSize));
              }

              bytes = IOUtils.toByteArray(fileIn, fetchSize); // read inputstream into byte array

              LOG.warn("retrieved {} bytes", bytes.length);

              StringBuilder sb = new StringBuilder();
              for (int i=0; i<Math.min(16, fetchSize);i++) {
                int b = bytes[i] & 0xFF;
                sb.append(" ").append(HEX_ARRAY[b>>>4]).append(HEX_ARRAY[b & 0xF]);
              }
              LOG.warn("retrieved {} bytes starting with {}", bytes.length, sb.toString());
              LOG.warn("metadata={}", metadata);

              // create content and return result
              String base = urlstr.toString();
              return new ProtocolOutput(
                new Content(base, base, bytes, "application/octet-stream", metadata, getConf()), 
                ProtocolStatus.STATUS_SUCCESS
              );
            } else {
              // communicate error
              String message = "File not found: " + urlstr;
              LOG.warn(message);
              String base = urlstr.toString();
              return new ProtocolOutput(
                new Content(base, base, message.getBytes(), "text/plain", new Metadata(), getConf()),
                ProtocolStatus.STATUS_NOTFOUND
              );
            }

          }

        } catch (Exception e) {
          LOG.error("Could not establish session", e);

          // todo: we can communicate the reason for error as ProtocolStatus
        }

        throw new UnsupportedOperationException("neither directory nor file: " + urlstr);
    } catch(Exception e) {
      LOG.error("Could not get protocol output for " + urlstr, e);
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
  public BaseRobotRules getRobotRules(Text url, CrawlDatum datum,
      List<Content> robotsTxtContent) {
    LOG.debug("getRobotRules({}, {}, {})", url, datum, robotsTxtContent);

    // todo: we should read some robots file from the smb share
    return RobotRulesParser.EMPTY_RULES;
  }
}
