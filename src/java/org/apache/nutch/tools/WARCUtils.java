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
package org.apache.nutch.tools;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.util.StringUtil;
import org.archive.format.http.HttpHeaders;
import org.archive.format.warc.WARCConstants;
import org.archive.io.warc.WARCRecordInfo;
import org.archive.uid.UUIDGenerator;
import org.archive.util.DateUtils;
import org.archive.util.anvl.ANVLRecord;

public class WARCUtils {
  public final static String SOFTWARE = "software";
  public final static String HTTP_HEADER_FROM = "http-header-from";
  public final static String HTTP_HEADER_USER_AGENT = "http-header-user-agent";
  public final static String HOSTNAME = "hostname";
  public final static String ROBOTS = "robots";
  public final static String OPERATOR = "operator";
  public final static String FORMAT = "format";
  public final static String CONFORMS_TO = "conformsTo";
  public final static String IP = "ip";
  public final static UUIDGenerator generator = new UUIDGenerator();
  public static final String CRLF = "\r\n";
  public static final String COLONSP = ": ";
  protected static final Pattern PROBLEMATIC_HEADERS = Pattern
      .compile("(?i)(?:Content-(?:Encoding|Length)|Transfer-Encoding)");
  protected static final String X_HIDE_HEADER = "X-Crawler-";

  public static final ANVLRecord getWARCInfoContent(Configuration conf) {
    ANVLRecord record = new ANVLRecord();

    // informative headers
    record.addLabelValue(FORMAT, "WARC File Format 1.0");
    record.addLabelValue(CONFORMS_TO, "http://bibnum.bnf.fr/WARC/WARC_ISO_28500_version1_latestdraft.pdf");

    record.addLabelValue(SOFTWARE, conf.get("http.agent.name", ""));
    record.addLabelValue(HTTP_HEADER_USER_AGENT,
            getAgentString(conf.get("http.agent.name", ""),
                    conf.get("http.agent.version", ""),
                    conf.get("http.agent.description", ""),
                    conf.get("http.agent.url", ""),
                    conf.get("http.agent.email", "")));
    record.addLabelValue(HTTP_HEADER_FROM,
            conf.get("http.agent.email", ""));

    try {
      record.addLabelValue(HOSTNAME, getHostname(conf));
      record.addLabelValue(IP, getIPAddress(conf));
    } catch (UnknownHostException ignored) {
      // do nothing as this fields are optional
    }

    record.addLabelValue(ROBOTS, "classic"); // TODO Make configurable?
    record.addLabelValue(OPERATOR, conf.get("http.agent.email", ""));

    return record;
  }

  public static final String getHostname(Configuration conf)
          throws UnknownHostException {

    return StringUtil.isEmpty(conf.get("http.agent.host", "")) ?
            InetAddress.getLocalHost().getHostName() :
            conf.get("http.agent.host");
  }

  public static final String getIPAddress(Configuration conf)
          throws UnknownHostException {

    return InetAddress.getLocalHost().getHostAddress();
  }

  public static final byte[] toByteArray(HttpHeaders headers)
          throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    headers.write(out);

    return out.toByteArray();
  }

  public static final String getAgentString(String name, String version,
          String description, String URL, String email) {

    StringBuffer buf = new StringBuffer();

    buf.append(name);

    if (version != null) {
      buf.append("/").append(version);
    }

    if (((description != null) && (description.length() != 0)) || (
            (email != null) && (email.length() != 0)) || ((URL != null) && (
                    URL.length() != 0))) {
      buf.append(" (");

      if ((description != null) && (description.length() != 0)) {
        buf.append(description);
        if ((URL != null) || (email != null))
          buf.append("; ");
      }

      if ((URL != null) && (URL.length() != 0)) {
        buf.append(URL);
        if (email != null)
          buf.append("; ");
      }

      if ((email != null) && (email.length() != 0))
        buf.append(email);

      buf.append(")");
    }

    return buf.toString();
  }

  public static final WARCRecordInfo docToMetadata(NutchDocument doc)
          throws UnsupportedEncodingException {
    WARCRecordInfo record = new WARCRecordInfo();

    record.setType(WARCConstants.WARCRecordType.metadata);
    record.setUrl((String) doc.getFieldValue("id"));
    record.setCreate14DigitDate(
            DateUtils.get14DigitDate((Date) doc.getFieldValue("tstamp")));
    record.setMimetype("application/warc-fields");
    record.setRecordId(generator.getRecordID());

    // metadata
    ANVLRecord metadata = new ANVLRecord();

    for (String field : doc.getFieldNames()) {
      List<Object> values = doc.getField(field).getValues();
      for (Object value : values) {
        if (value instanceof Date) {
          metadata.addLabelValue(field, DateUtils.get14DigitDate());
        } else {
          metadata.addLabelValue(field, (String) value);
        }
      }
    }

    record.setContentLength(metadata.getLength());
    record.setContentStream(
            new ByteArrayInputStream(metadata.getUTF8Bytes()));

    return record;
  }
  
  /**
   * Modify verbatim HTTP response headers: fix, remove or replace headers
   * <code>Content-Length</code>, <code>Content-Encoding</code> and
   * <code>Transfer-Encoding</code> which may confuse WARC readers. Ensure that
   * returned header end with a single empty line (<code>\r\n\r\n</code>).
   * 
   * @param headers
   *          HTTP 1.1 or 1.0 response header string, CR-LF-separated lines,
   *          first line is status line
   * @return safe HTTP response header
   */
  public static final String fixHttpHeaders(String headers, int contentLength) {
    int start = 0, lineEnd = 0, last = 0, trailingCrLf= 0;
    StringBuilder replace = new StringBuilder();
    while (start < headers.length()) {
      lineEnd = headers.indexOf(CRLF, start);
      trailingCrLf = 1;
      if (lineEnd == -1) {
        lineEnd = headers.length();
        trailingCrLf = 0;
      }
      int colonPos = -1;
      for (int i = start; i < lineEnd; i++) {
        if (headers.charAt(i) == ':') {
          colonPos = i;
          break;
        }
      }
      if (colonPos == -1) {
        boolean valid = true;
        if (start == 0) {
          // status line (without colon)
          // TODO: http/2
        } else if ((lineEnd + 4) == headers.length()
            && headers.endsWith(CRLF + CRLF)) {
          // ok, trailing empty line
          trailingCrLf = 2;
        } else {
          valid = false;
        }
        if (!valid) {
          if (last < start) {
            replace.append(headers.substring(last, start));
          }
          last = lineEnd + 2 * trailingCrLf;
        }
        start = lineEnd + 2 * trailingCrLf;
        /*
         * skip over invalid header line, no further check for problematic
         * headers required
         */
        continue;
      }
      String name = headers.substring(start, colonPos);
      if (PROBLEMATIC_HEADERS.matcher(name).matches()) {
        boolean needsFix = true;
        if (name.equalsIgnoreCase("content-length")) {
          String value = headers.substring(colonPos + 1, lineEnd).trim();
          try {
            int l = Integer.parseInt(value);
            if (l == contentLength) {
              needsFix = false;
            }
          } catch (NumberFormatException e) {
            // needs to be fixed
          }
        }
        if (needsFix) {
          if (last < start) {
            replace.append(headers.substring(last, start));
          }
          last = lineEnd + 2 * trailingCrLf;
          replace.append(X_HIDE_HEADER)
              .append(headers.substring(start, lineEnd + 2 * trailingCrLf));
          if (trailingCrLf == 0) {
            replace.append(CRLF);
            trailingCrLf = 1;
          }
          if (name.equalsIgnoreCase("content-length")) {
            // add effective uncompressed and unchunked length of content
            replace.append("Content-Length").append(COLONSP)
                .append(contentLength).append(CRLF);
          }
        }
      }
      start = lineEnd + 2 * trailingCrLf;
    }
    if (last > 0 || trailingCrLf != 2) {
      if (last < headers.length()) {
        // append trailing headers
        replace.append(headers.substring(last));
      }
      while (trailingCrLf < 2) {
        replace.append(CRLF);
        trailingCrLf++;
      }
      return replace.toString();
    }
    return headers;
  }

  
}
