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
package org.apache.nutch.protocol.ftp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.protocol.Protocol;
import org.apache.nutch.protocol.ProtocolOutput;
import org.apache.nutch.protocol.ProtocolPluginIntegrationTest;
import org.apache.nutch.util.NutchConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockftpserver.fake.FakeFtpServer;
import org.mockftpserver.fake.UserAccount;
import org.mockftpserver.fake.filesystem.DirectoryEntry;
import org.mockftpserver.fake.filesystem.FileEntry;
import org.mockftpserver.fake.filesystem.UnixFakeFileSystem;

/**
 * Integration tests for protocol-ftp using an in-process FakeFtpServer.
 *
 * <p>FTP passive mode with Testcontainers requires that the PASV response IP
 * matches the host-visible address of the container, which is not reliable
 * across Docker Desktop (macOS/Windows) and Linux Docker environments. An
 * in-process {@link FakeFtpServer} from MockFtpServer avoids this constraint
 * while still testing the Nutch FTP client against a real FTP protocol
 * implementation.
 */
public class FtpProtocolIT implements ProtocolPluginIntegrationTest {

  private static final String FTP_USER = "testuser";
  private static final String FTP_PASS = "testpass";
  private static final String FTP_HOME = "/home/testuser";
  private static final String TEST_FILE = "test.txt";
  private static final String TEST_CONTENT = "FTP integration test content";

  private static FakeFtpServer fakeFtpServer;
  private Ftp protocol;

  @BeforeAll
  static void startFtpServer() {
    fakeFtpServer = new FakeFtpServer();
    fakeFtpServer.setServerControlPort(0); // bind to a random free port

    UserAccount userAccount = new UserAccount(FTP_USER, FTP_PASS, FTP_HOME);
    fakeFtpServer.addUserAccount(userAccount);

    UnixFakeFileSystem fileSystem = new UnixFakeFileSystem();
    fileSystem.add(new DirectoryEntry(FTP_HOME));
    fileSystem.add(new FileEntry(FTP_HOME + "/" + TEST_FILE, TEST_CONTENT));
    fakeFtpServer.setFileSystem(fileSystem);

    fakeFtpServer.start();
  }

  @AfterAll
  static void stopFtpServer() {
    if (fakeFtpServer != null) {
      fakeFtpServer.stop();
    }
  }

  @BeforeEach
  @Override
  public void setUpProtocol() throws Exception {
    Configuration conf = NutchConfiguration.create();
    conf.set("plugin.includes", "protocol-ftp|nutch-extensionpoints");
    conf.set("http.agent.name", "NutchFtpProtocolIT");
    conf.set("ftp.username", FTP_USER);
    conf.set("ftp.password", FTP_PASS);
    conf.setInt("ftp.timeout", 10000);
    protocol = new Ftp();
    protocol.setConf(conf);
  }

  @AfterEach
  @Override
  public void tearDownProtocol() {
    protocol = null;
  }

  @Override
  public Protocol getProtocol() {
    return protocol;
  }

  @Override
  public String getTestUrl() {
    return "ftp://localhost:" + fakeFtpServer.getServerControlPort()
        + FTP_HOME + "/" + TEST_FILE;
  }

  @Test
  void testFtpFileDownload() throws Exception {
    CrawlDatum datum = new CrawlDatum();
    ProtocolOutput output = protocol.getProtocolOutput(new Text(getTestUrl()), datum);

    assertNotNull(output, "ProtocolOutput must not be null");
    int code = Integer.parseInt(
        datum.getMetaData().get(Nutch.PROTOCOL_STATUS_CODE_KEY).toString());
    assertEquals(200, code, "Expected FTP 200 for file download");

    assertNotNull(output.getContent(), "Content must not be null");
    String body = new String(output.getContent().getContent(), StandardCharsets.UTF_8);
    assertTrue(body.contains(TEST_CONTENT),
        "Downloaded content must match the file on the FTP server");
  }

  @Test
  void testFtpDirectoryListing() throws Exception {
    String dirUrl = "ftp://localhost:" + fakeFtpServer.getServerControlPort()
        + FTP_HOME + "/";
    CrawlDatum datum = new CrawlDatum();
    ProtocolOutput output = protocol.getProtocolOutput(new Text(dirUrl), datum);

    assertNotNull(output, "ProtocolOutput for directory listing must not be null");
    int code = Integer.parseInt(
        datum.getMetaData().get(Nutch.PROTOCOL_STATUS_CODE_KEY).toString());
    assertEquals(200, code, "Expected FTP 200 for directory listing");
  }

  @Test
  void testFtpMissingFileReturnsError() throws Exception {
    String missingUrl = "ftp://localhost:" + fakeFtpServer.getServerControlPort()
        + FTP_HOME + "/nonexistent.txt";
    CrawlDatum datum = new CrawlDatum();
    ProtocolOutput output = protocol.getProtocolOutput(new Text(missingUrl), datum);
    assertNotNull(output, "ProtocolOutput must not be null even for missing files");
    // FTP 550 "No such file" maps to a non-200 Nutch status
    int code = Integer.parseInt(
        datum.getMetaData().get(Nutch.PROTOCOL_STATUS_CODE_KEY).toString());
    assertTrue(code != 200, "Expected non-200 code for missing FTP file, got: " + code);
  }
}
