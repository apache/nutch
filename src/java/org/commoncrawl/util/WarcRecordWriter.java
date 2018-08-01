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
package org.commoncrawl.util;

import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.codec.binary.Base32;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.net.protocols.HttpDateFormat;
import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.protocol.ProtocolStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class WarcRecordWriter extends RecordWriter<Text, WarcCapture> {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Holds duration of fetch in metadata, see
   * {@link org.apache.nutch.protocol.http.api.HttpBase#RESPONSE_TIME}
   */
  protected static final Text FETCH_DURATION = new Text("_rs_");
  public static final String CRLF = "\r\n";
  public static final String COLONSP = ": ";
  protected static final Pattern PROBLEMATIC_HEADERS = Pattern
      .compile("(?i)(?:Content-(?:Encoding|Length)|Transfer-Encoding)");

  private DataOutputStream warcOut;
  private WarcWriter warcWriter;
  private DataOutputStream crawlDiagnosticsWarcOut;
  private WarcWriter crawlDiagnosticsWarcWriter;
  private DataOutputStream robotsTxtWarcOut;
  private WarcWriter robotsTxtWarcWriter;
  private DataOutputStream cdxOut;
  private DataOutputStream crawlDiagnosticsCdxOut;
  private DataOutputStream robotsTxtCdxOut;
  private URI warcinfoId;
  private URI crawlDiagnosticsWarcinfoId;
  private URI robotsTxtWarcinfoId;
  private MessageDigest sha1 = null;
  private Base32 base32 = new Base32();
  private boolean generateCrawlDiagnostics;
  private boolean generateRobotsTxt;
  private boolean generateCdx;
  private boolean deduplicate;
  private String lastURL = ""; // for deduplication

  public WarcRecordWriter(Configuration conf, Path outputPath, int partition) throws IOException {

    FileSystem fs = outputPath.getFileSystem(conf);

    SimpleDateFormat fileDate = new SimpleDateFormat("yyyyMMddHHmmss",
        Locale.US);
    fileDate.setTimeZone(TimeZone.getTimeZone("GMT"));

    String prefix = conf.get("warc.export.prefix", "NUTCH-CRAWL");

    /*
     * WARC-Date : "The timestamp shall represent the instant that data capture
     * for record creation began."
     * (http://iipc.github.io/warc-specifications/specifications/warc-format/
     * warc-1.1/#warc-date-mandatory)
     */
    String date = conf.get("warc.export.date", fileDate.format(new Date()));
    String endDate = conf.get("warc.export.date.end", date);
    Date captureStartDate = new Date();
    try {
      captureStartDate = fileDate.parse(date);
    } catch (ParseException e) {
      LOG.error("Failed to parse warc.export.date {}: {}", date,
          e.getMessage());
    }

    String hostname = conf.get("warc.export.hostname", getHostname());
    String filename = getFileName(prefix, date, endDate, hostname, partition);

    String publisher = conf.get("warc.export.publisher", null);
    String operator = conf.get("warc.export.operator", null);
    String software = conf.get("warc.export.software", "Apache Nutch");
    String isPartOf = conf.get("warc.export.isPartOf", null);
    String description = conf.get("warc.export.description", null);
    generateCrawlDiagnostics = conf.getBoolean("warc.export.crawldiagnostics",
        false);
    generateRobotsTxt = conf.getBoolean("warc.export.robotstxt", false);
    generateCdx = conf.getBoolean("warc.export.cdx", false);
    deduplicate = conf.getBoolean("warc.deduplicate", false);

    Path warcPath = new Path(new Path(outputPath, "warc"), filename);
    warcOut = fs.create(warcPath);
    Path cdxPath = null;
    if (generateCdx) {
      cdxPath = new Path(
          conf.get("warc.export.cdx.path", outputPath.toString()));
      cdxOut = openCdxOutputStream(new Path(cdxPath, "warc"), filename, conf);
    }
    warcWriter = openWarcWriter(warcPath, warcOut, cdxOut);
    warcinfoId = warcWriter.writeWarcinfoRecord(filename, hostname, publisher,
        operator, software, isPartOf, description, captureStartDate);

    if (generateCrawlDiagnostics) {
      Path crawlDiagnosticsWarcPath = new Path(
          new Path(outputPath, "crawldiagnostics"), filename);
      crawlDiagnosticsWarcOut = fs.create(crawlDiagnosticsWarcPath);
      if (generateCdx) {
        crawlDiagnosticsCdxOut = openCdxOutputStream(
            new Path(cdxPath, "crawldiagnostics"), filename, conf);
      }
      crawlDiagnosticsWarcWriter = openWarcWriter(crawlDiagnosticsWarcPath, crawlDiagnosticsWarcOut,crawlDiagnosticsCdxOut);
      crawlDiagnosticsWarcinfoId = crawlDiagnosticsWarcWriter
          .writeWarcinfoRecord(filename, hostname, publisher, operator,
              software, isPartOf, description, captureStartDate);
    }

    if (generateRobotsTxt) {
      Path robotsTxtWarcPath = new Path(new Path(outputPath, "robotstxt"),
          filename);
      robotsTxtWarcOut = fs.create(robotsTxtWarcPath);
      if (generateCdx) {
        robotsTxtCdxOut = openCdxOutputStream(new Path(cdxPath, "robotstxt"),
            filename, conf);
      }
      robotsTxtWarcWriter = openWarcWriter(robotsTxtWarcPath, robotsTxtWarcOut,
          robotsTxtCdxOut);
      robotsTxtWarcinfoId = robotsTxtWarcWriter.writeWarcinfoRecord(filename,
          hostname, publisher, operator, software, isPartOf, description,
          captureStartDate);
    }

    try {
      sha1 = MessageDigest.getInstance("SHA1");
    } catch (NoSuchAlgorithmException e) {
      LOG.error("Unable to instantiate SHA1 MessageDigest object");
      throw new RuntimeException(e);
    }
  }

  /**
   * Compose a unique WARC file name.
   * 
   * The WARC specification recommends:
   * <code>Prefix-Timestamp-Serial-Crawlhost.warc.gz</code> (<a href=
   * "http://iipc.github.io/warc-specifications/specifications/warc-format/
   * warc-1.1/#annex-c-informative-warc-file-size-and-name-recommendations">WARC
   * 1.1, Annex C</a>)
   * 
   * @param prefix
   *          WARC file name prefix
   * @param startDate
   *          capture start date
   * @param endDate
   *          capture end date
   * @param hostname
   *          name of the crawling host
   * @param partition
   *          MapReduce partition
   * @return (unique) WARC file name
   */
  protected String getFileName(String prefix, String startDate, String endDate,
      String host, int partition) {
    NumberFormat numberFormat = NumberFormat.getInstance();
    numberFormat.setMinimumIntegerDigits(5);
    numberFormat.setGroupingUsed(false);
    return prefix + "-" + startDate + "-" + endDate + "-"
        + numberFormat.format(partition) + ".warc.gz";
  }

  protected String getSha1DigestWithAlg(byte[] bytes) {
    sha1.reset();
    return "sha1:" + base32.encodeAsString(sha1.digest(bytes));
  }

  protected static String getStatusLine(String httpHeader) {
    int eol = httpHeader.indexOf('\n');
    if (eol == -1) {
      return httpHeader;
    }
    if (eol > 0 && httpHeader.charAt(eol - 1) == '\r') {
      eol--;
    }
    return httpHeader.substring(0, eol);
  }

  protected static int getStatusCode(String statusLine) {
    int start = statusLine.indexOf(" ");
    int end = statusLine.indexOf(" ", start + 1);
    if (end == -1)
      end = statusLine.length();
    int code = 200;
    try {
      code = Integer.parseInt(statusLine.substring(start + 1, end));
    } catch (NumberFormatException e) {
    }
    return code;
  }

  /** Format status line and pair-wise list of headers as string */
  public static String formatHttpHeaders(String statusLine, List<String> headers) {
    StringBuilder sb = new StringBuilder();
    sb.append(statusLine).append(CRLF);
    Iterator<String> it = headers.iterator();
    while (it.hasNext()) {
      String name = it.next();
      if (!it.hasNext()) {
        // no value for name
        break;
      }
      String value = it.next();
      sb.append(name).append(COLONSP).append(value).append(CRLF);
    }
    return sb.toString();
  }

  /**
   * Modify verbatim HTTP response headers: fix, remove or replace headers
   * <code>Content-Length</code>, <code>Content-Encoding</code> and
   * <code>Transfer-Encoding</code> which may confuse WARC readers.
   * 
   * @param headers
   *          HTTP 1.1 or 1.0 response header string, CR-LF-separated lines,
   *          first line is status line
   * @return safe HTTP response header
   */
  public static String fixHttpHeaders(String headers, int contentLength) {
    int start = 0, lineEnd = 0, last = 0;
    StringBuilder replace = new StringBuilder();
    while ((lineEnd = headers.indexOf(CRLF, start)) != -1) {
      lineEnd += 2;
      int colonPos = -1;
      for (int i = start; i < lineEnd; i++) {
        if (headers.charAt(i) == ':') {
          colonPos = i;
          break;
        }
      }
      if (colonPos == -1) {
        if (start == 0) {
          // status line (without colon)
          // TODO: http/2
        } else {
          // TODO: invalid header line
        }
        start = lineEnd;
        continue;
      }
      String name = headers.substring(start, colonPos);
      if (PROBLEMATIC_HEADERS.matcher(name).matches()) {
        if (last < start) {
          replace.append(headers.substring(last, start));
        }
        last = lineEnd;
        replace.append("X-Crawler-").append(headers.substring(start, lineEnd));
        if (name.equalsIgnoreCase("content-length")) {
          // add effective uncompressed and unchunked length of content bytes
          replace.append("Content-Length").append(COLONSP).append(contentLength)
              .append(CRLF);
        }
      }
      start = lineEnd;
    }
    if (last > 0) {
      if (last < headers.length()) {
        // append trailing headers
        replace.append(headers.substring(last));
      }
      return replace.toString();
    }
    return headers;
  }

  protected static String getHostname() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      LOG.warn("Failed to get hostname: {}", e.getMessage());
    }
    return "localhost";
  }

  private WarcWriter openWarcWriter(Path warcPath, DataOutputStream warcOut,
      DataOutputStream cdxOut) {
    if (cdxOut != null) {
      return new WarcCdxWriter(warcOut, cdxOut, warcPath);
    }
    return new WarcWriter(warcOut);
  }

  protected static DataOutputStream openCdxOutputStream(Path cdxPath,
      String warcFilename, Configuration conf) throws IOException {
    String cdxFilename = warcFilename.replaceFirst("\\.warc\\.gz$", ".cdx.gz");
    Path cdxFile = new Path(cdxPath, cdxFilename);
    FileSystem fs = cdxPath.getFileSystem(conf);
    return new DataOutputStream(new GZIPOutputStream(fs.create(cdxFile)));
  }

  public synchronized void write(Text key, WarcCapture value)
      throws IOException {
    URI targetUri;

    String url = value.url.toString();
    try {
      targetUri = new URI(url);
    } catch (URISyntaxException e) {
      LOG.error("Cannot write WARC record, invalid URI: {}", value.url);
      return;
    }

    if (value.content == null) {
      LOG.warn("Cannot write WARC record, no content for {}", value.url);
      return;
    }

    if (deduplicate) {
      if (lastURL.equals(url)) {
        LOG.info("Skipping duplicate record: {}", value.url);
        return;
      }
      lastURL = url;
    }

    String ip = "0.0.0.0";
    Date date = null;
    boolean notModified = false;
    String verbatimResponseHeaders = null;
    String verbatimRequestHeaders = null;
    List<String> headers = new ArrayList<>();
    String responseHeaders = null;
    String statusLine = "";
    int httpStatusCode = 200;
    String fetchDuration = null;

    if (value.datum != null) {
      date = new Date(value.datum.getFetchTime());
      // This is for older crawl dbs that don't include the verbatim status
      // line in the metadata
      ProtocolStatus pstatus = (ProtocolStatus) value.datum.getMetaData()
          .get(Nutch.WRITABLE_PROTO_STATUS_KEY);
      if (pstatus == null) {
        LOG.warn("Cannot write WARC record, no protocol status for {}",
            value.url);
        return;
      } else {
        switch (pstatus.getCode()) {
        case ProtocolStatus.SUCCESS:
          statusLine = "HTTP/1.1 200 OK";
          httpStatusCode = 200;
          break;
        case ProtocolStatus.TEMP_MOVED:
          statusLine = "HTTP/1.1 302 Found";
          httpStatusCode = 302;
          break;
        case ProtocolStatus.MOVED:
          statusLine = "HTTP/1.1 301 Moved Permanently";
          httpStatusCode = 301;
          break;
        case ProtocolStatus.NOTMODIFIED:
          statusLine = "HTTP/1.1 304 Not Modified";
          httpStatusCode = 304;
          notModified = true;
          break;
        default:
          if (value.content.getMetadata()
              .get(Response.RESPONSE_HEADERS) == null) {
            LOG.warn("Unknown or ambiguous protocol status: {}", pstatus);
            return;
          }
        }
      }
      if (value.datum.getMetaData().get(FETCH_DURATION) != null) {
        fetchDuration = value.datum.getMetaData().get(FETCH_DURATION)
            .toString();
      }
    } else {
      // robots.txt, no CrawlDatum available
      String fetchTime = value.content.getMetadata().get(Nutch.FETCH_TIME_KEY);
      if (fetchTime != null) {
        try {
          date = new Date(new Long(fetchTime));
        } catch (NumberFormatException e) {
          LOG.error("Invalid fetch time '{}' in content metadata of {}",
              fetchTime, value.url.toString());
        }
      }
      if (date == null) {
        String httpDate = value.content.getMetadata().get("Date");
        if (httpDate != null) {
          try {
            date = HttpDateFormat.toDate(httpDate);
          } catch (ParseException e) {
            LOG.warn("Failed to parse HTTP Date {} for {}", httpDate,
                targetUri);
            date = new Date();
          }
        } else {
          LOG.warn("No HTTP Date for {}", targetUri);
          date = new Date();
        }
      }
      // status is taken from header
    }

    boolean useVerbatimResponseHeaders = false;
    String truncatedReason = null;

    for (String name : value.content.getMetadata().names()) {
      String val = value.content.getMetadata().get(name);
      switch (name) {
      case Response.IP_ADDRESS:
        ip = val;
        break;
      case Response.REQUEST:
        verbatimRequestHeaders = val;
        break;
      case Response.RESPONSE_HEADERS:
        verbatimResponseHeaders = val;
        if (verbatimResponseHeaders.contains(CRLF)) {
          useVerbatimResponseHeaders = true;
        }
        statusLine = getStatusLine(verbatimResponseHeaders);
        httpStatusCode = getStatusCode(statusLine);
        break;
      case Response.TRUNCATED_CONTENT_REASON:
        truncatedReason = val;
        break;
      case Nutch.SEGMENT_NAME_KEY:
      case Nutch.FETCH_STATUS_KEY:
      case Nutch.SCORE_KEY:
      case Nutch.SIGNATURE_KEY:
        break; // ignore, not required for WARC record
      default:
        // We have to fix up a few headers because we don't have the raw
        // responses to avoid that WARC readers try to read the content
        // as chunked or gzip-compressed.
        if (name.equalsIgnoreCase("Content-Length")) {
          int origContentLength = -1;
          try {
            origContentLength = Integer.parseInt(val);
          } catch (NumberFormatException e) {
            // ignore
          }
          headers.add("Content-Length");
          if (origContentLength != value.content.getContent().length) {
            headers.add("" + value.content.getContent().length);
            headers.add("X-Crawler-Content-Length");
          }
        } else if (name.equalsIgnoreCase("Content-Encoding")) {
          if (val.equalsIgnoreCase("identity")) {
            headers.add(name);
          } else {
            headers.add("X-Crawler-Content-Encoding");
          }
        } else if (name.equalsIgnoreCase("Transfer-Encoding")) {
          if (val.equalsIgnoreCase("identity")) {
            headers.add(name);
          } else {
            headers.add("X-Crawler-Transfer-Encoding");
          }
        } else {
          headers.add(name);
        }
        headers.add(val);
      }
    }

    if (verbatimRequestHeaders == null) {
      LOG.error("No request headers for {}", url);
    }

    if (useVerbatimResponseHeaders && verbatimResponseHeaders != null) {
      responseHeaders = fixHttpHeaders(verbatimResponseHeaders, value.content.getContent().length);
    } else {
      responseHeaders = formatHttpHeaders(statusLine, headers);
    }

    WarcWriter writer = warcWriter;
    URI infoId = this.warcinfoId;
    if (value.datum == null) {
      // no CrawlDatum: must be a robots.txt
      if (!generateRobotsTxt)
        return;
      writer = robotsTxtWarcWriter;
      infoId = robotsTxtWarcinfoId;
    } else if (value.datum.getStatus() != CrawlDatum.STATUS_FETCH_SUCCESS) {
      if (!generateCrawlDiagnostics)
        return;
      writer = crawlDiagnosticsWarcWriter;
      infoId = crawlDiagnosticsWarcinfoId;
    }

    URI requestId = null;
    if (verbatimRequestHeaders != null) {
      LOG.warn("c {} {} {}", targetUri, infoId, writer);
      requestId = writer.writeWarcRequestRecord(targetUri, ip, date, infoId,
          verbatimRequestHeaders.getBytes(StandardCharsets.UTF_8));
    }

    if (notModified) {
      LOG.warn("Revisit records not supported: {}", key);
      /*
       * writer.writeWarcRevisitRecord(targetUri, ip, date, infoId, requestId,
       * WarcWriter.PROFILE_REVISIT_NOT_MODIFIED, payloadDigest,
       * abbreviatedResponse, abbreviatedResponseLength);
       */
    } else {
      StringBuilder responsesb = new StringBuilder(4096);
      responsesb.append(responseHeaders).append(CRLF);

      byte[] responseHeaderBytes = responsesb.toString()
          .getBytes(StandardCharsets.UTF_8);
      byte[] responseBytes = new byte[responseHeaderBytes.length
          + value.content.getContent().length];
      System.arraycopy(responseHeaderBytes, 0, responseBytes, 0,
          responseHeaderBytes.length);
      System.arraycopy(value.content.getContent(), 0, responseBytes,
          responseHeaderBytes.length, value.content.getContent().length);

      if (generateCdx) {
        value.content.getMetadata().add("HTTP-Status-Code",
            String.format("%d", httpStatusCode));
      }

      URI responseId = writer.writeWarcResponseRecord(targetUri, ip, date,
          infoId, requestId, getSha1DigestWithAlg(value.content.getContent()),
          getSha1DigestWithAlg(responseBytes), truncatedReason, responseBytes,
          value.content);

      // Write metadata record
      StringBuilder metadatasb = new StringBuilder(4096);
      Map<String, String> metadata = new LinkedHashMap<String, String>();

      if (fetchDuration != null) {
        metadata.put("fetchTimeMs", fetchDuration);
      }

      if (metadata.size() > 0) {
        for (Map.Entry<String, String> entry : metadata.entrySet()) {
          metadatasb.append(entry.getKey()).append(COLONSP)
              .append(entry.getValue()).append(CRLF);
        }
        metadatasb.append(CRLF);

        writer.writeWarcMetadataRecord(targetUri, date, infoId, responseId,
            null, metadatasb.toString().getBytes(StandardCharsets.UTF_8));
      }
    }
  }

  public synchronized void close(TaskAttemptContext context)
      throws IOException {
    warcOut.close();
    if (generateCrawlDiagnostics) {
      crawlDiagnosticsWarcOut.close();
    }
    if (generateRobotsTxt) {
      robotsTxtWarcOut.close();
    }
    if (generateCdx) {
      cdxOut.close();
      if (generateCrawlDiagnostics) {
        crawlDiagnosticsCdxOut.close();
      }
      if (generateRobotsTxt) {
        robotsTxtCdxOut.close();
      }
    }
  }
}