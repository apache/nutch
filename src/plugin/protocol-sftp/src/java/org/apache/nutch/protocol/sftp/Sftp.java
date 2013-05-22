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

package org.apache.nutch.protocol.sftp;

//JDK imports
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

//APACHE imports
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.Protocol;
import org.apache.nutch.protocol.ProtocolOutput;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.storage.WebPage.Field;
import org.apache.nutch.protocol.RobotRulesParser;

//JSCH imports
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import com.jcraft.jsch.ChannelSftp.LsEntry;

import crawlercommons.robots.BaseRobotRules;

/**
 * This class uses the Jsch package to fetch content using the Sftp protocol.
 * 
 */
public class Sftp implements Protocol {

  private static final Logger logger = Logger.getLogger(Sftp.class);
  private static final Map<String, BlockingQueue<ChannelSftp>> channelSftpByHostMap = new Hashtable<String, BlockingQueue<ChannelSftp>>();

  private Configuration configuration;

  private String server;
  private int port;
  private String user;
  private String password;

  public Sftp() {
  }

  public ProtocolOutput getProtocolOutput(String url, WebPage page) {
    URL sUrl = null;
    String urlStr = url.toString().trim();

    ChannelSftp channelSftp = null;
    try {
      sUrl = new URL(urlStr);
      channelSftp = getChannelSftp(sUrl);

      String urlFile = sUrl.getFile();
      if (urlFile.endsWith(".htm") || urlFile.endsWith(".html")) {
        ProtocolOutput po = getFileProtocolOutput(sUrl, channelSftp,
            "text/html");
        return po;
      } else if (urlFile.endsWith(".pdf")) {
        ProtocolOutput po = getFileProtocolOutput(sUrl, channelSftp,
            "application/pdf");
        return po;
      } else {
        ProtocolOutput po = getDirectoryProtocolOutput(sUrl, channelSftp);
        return po;
      }
    } catch (MalformedURLException e) {
      logger.error("Bad URL String: " + urlStr, e);
      return null;
    } catch (InterruptedException e) {
      return null;
    } catch (SftpException e) {
      return null;
    } catch (IOException e) {
      return null;
    } catch (Exception e) {
      logger.error("Unknown Exception in getProtocolOutput()", e);
      return null;
    } finally {
      if (channelSftp != null) {
        try {
          putChannelSftp(sUrl, channelSftp);
        } catch (InterruptedException e) {
          logger.error("Cannot return ChannelSftp object to Queue", e);
        }
      }
    }
  }

  private ChannelSftp getChannelSftp(URL url) throws InterruptedException {
    String host = url.getHost();
    BlockingQueue<ChannelSftp> queue = channelSftpByHostMap.get(host);
    if (queue == null) {
      return null;
    }

    try {
      ChannelSftp cSftp = queue.take();
      return cSftp;
    } catch (InterruptedException e) {
      logger
          .error("Wait for getChannelSftp() interrupted for host: " + host, e);
      throw e;
    }
  }

  private void putChannelSftp(URL url, ChannelSftp cSftp)
      throws InterruptedException {
    String host = url.getHost();
    BlockingQueue<ChannelSftp> queue = channelSftpByHostMap.get(host);
    if (queue == null) {
      return;
    }

    try {
      queue.put(cSftp);
    } catch (InterruptedException e) {
      logger
          .error("Wait for putChannelSftp() interrupted for host: " + host, e);
      throw e;
    }
  }

  private ProtocolOutput getFileProtocolOutput(URL url,
      ChannelSftp channelSftp, String contentType) throws SftpException,
      IOException {
    byte[] bytes = null;
    InputStream iStream = null;
    try {
      int size = (int) channelSftp.lstat(url.getFile()).getSize();
      iStream = channelSftp.get(url.getFile());
      bytes = new byte[size];
      iStream.read(bytes);
    } catch (SftpException e) {
      logger.error("SftpException in getFileProtocolOutput(), file: "
          + url.getFile(), e);
      throw e;
    } catch (IOException e) {
      logger.error("IOException in getFileProtocolOutput(), file: "
          + url.getFile(), e);
      throw e;
    } finally {
      if (iStream != null) {
        iStream.close();
      }
    }

    String urlStr = url.toExternalForm();

    Metadata metadata = new Metadata();
    metadata.set(Response.CONTENT_TYPE, contentType);
    metadata.set(Response.CONTENT_LENGTH, String.valueOf(bytes.length));
    metadata.set(Response.LAST_MODIFIED, channelSftp.lstat(url.getFile())
        .getMtimeString());
    metadata.set(Response.LOCATION, urlStr);

    Content content = new Content(urlStr, urlStr, bytes, contentType, metadata,
        configuration);
    ProtocolOutput po = new ProtocolOutput(content);
    return po;
  }

  @SuppressWarnings("unchecked")
  private ProtocolOutput getDirectoryProtocolOutput(URL url,
      ChannelSftp channelSftp) throws SftpException {
    try {
      channelSftp.cd(url.getFile());

      int count = 1;
      String directoryList = "<html><body>";
      Vector<LsEntry> vector = (Vector<LsEntry>) channelSftp.ls(".");
      for (LsEntry entry : vector) {
        String fileName = entry.getFilename();
        if (!fileName.equals(".") && !fileName.equals("..")) {
          directoryList += "<a href=\"" + url + "/" + fileName + "\">" + count
              + "</a>\r\n";
        }
        count++;
      }

      directoryList += "</body></html>";

      Metadata metadata = new Metadata();
      metadata.set(Response.CONTENT_TYPE, "text/html");
      metadata.set(Response.CONTENT_LENGTH, String.valueOf(directoryList
          .length()));
      metadata.set(Response.LAST_MODIFIED, channelSftp.lstat(url.getFile())
          .getMtimeString());
      metadata.set(Response.LOCATION, url.toExternalForm());

      Content content = new Content(url.toExternalForm(), url.toExternalForm(),
          directoryList.getBytes(), "text/html", metadata, configuration);
      ProtocolOutput po = new ProtocolOutput(content);
      return po;
    } catch (SftpException e) {
      logger.error("SftpException in getDirectoryProtocolOutput()", e);
      throw e;
    }
  }

  /**
   * Get the {@link Configuration} object
   */
  public Configuration getConf() {
    return configuration;
  }

  /**
   * Set the {@link Configuration} object
   */
  public void setConf(Configuration arg0) {
    configuration = arg0;

    server = configuration.get("sftp.server");
    port = configuration.getInt("sftp.port", 22);
    user = configuration.get("sftp.user", "anonymous");
    password = configuration.get("sftp.password", "guest");

    if (server == null) {
      return;
    }
    
    if (channelSftpByHostMap.containsKey(server)) {
      return;
    }

    synchronized (channelSftpByHostMap) {
      if (channelSftpByHostMap.containsKey(server)) {
        return;
      }

      JSch jsch = new JSch();
      Session session = null;
      try {
        session = jsch.getSession(user, server, port);
      } catch (JSchException e) {
        logger.error("Cannot create JSch session for user: " + user
            + ", host: " + server + ", port: " + port);
        return;
      }

      session.setPassword(password);
      Hashtable<String, String> config = new Hashtable<String, String>();
      config.put("StrictHostKeyChecking", "no");
      session.setConfig(config);

      ChannelSftp cSftp = null;
      try {
        session.connect(10000);

        cSftp = (ChannelSftp) session.openChannel("sftp");
        cSftp.connect();
      } catch (JSchException e) {
        logger.error("Cannot connect to JSch session for user: " + user
            + ", host: " + server + ", port: " + port);
        return;
      }

      BlockingQueue<ChannelSftp> queue = new ArrayBlockingQueue<ChannelSftp>(1,
          true);
      try {
        queue.put(cSftp);
      } catch (InterruptedException e) {
        logger.error("Interrupted during setConf()", e);
        return;
      }
      channelSftpByHostMap.put(server, queue);
    }
  }

  @Override
  public BaseRobotRules getRobotRules(String url, WebPage page) {
    return RobotRulesParser.EMPTY_RULES;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.nutch.plugin.FieldPluggable#getFields()
   */
  @Override
  public Collection<Field> getFields() {
    return Collections.emptySet();
  }
}
