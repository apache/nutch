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

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashMap;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.MapFileOutputFormat;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.util.StringUtils;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseText;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.ProtocolStatus;
import org.mortbay.jetty.Request;

/**
 * XXX should turn this into a plugin?
 */
public class SegmentHandler extends AbstractTestbedHandler {
  private static final Logger LOG = LoggerFactory.getLogger(SegmentHandler.class);
  private Segment seg;
  
  private static HashMap<Integer,Integer> protoCodes = new HashMap<Integer,Integer>();
  
  static {
    protoCodes.put(ProtocolStatus.ACCESS_DENIED, HttpServletResponse.SC_UNAUTHORIZED);
    protoCodes.put(ProtocolStatus.BLOCKED, HttpServletResponse.SC_SERVICE_UNAVAILABLE);
    protoCodes.put(ProtocolStatus.EXCEPTION, HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    protoCodes.put(ProtocolStatus.FAILED, HttpServletResponse.SC_BAD_REQUEST);
    protoCodes.put(ProtocolStatus.GONE, HttpServletResponse.SC_GONE);
    protoCodes.put(ProtocolStatus.MOVED, HttpServletResponse.SC_MOVED_PERMANENTLY);
    protoCodes.put(ProtocolStatus.NOTFETCHING, HttpServletResponse.SC_BAD_REQUEST);
    protoCodes.put(ProtocolStatus.NOTFOUND, HttpServletResponse.SC_NOT_FOUND);
    protoCodes.put(ProtocolStatus.NOTMODIFIED, HttpServletResponse.SC_NOT_MODIFIED);
    protoCodes.put(ProtocolStatus.PROTO_NOT_FOUND, HttpServletResponse.SC_BAD_REQUEST);
    protoCodes.put(ProtocolStatus.REDIR_EXCEEDED, HttpServletResponse.SC_BAD_REQUEST);
    protoCodes.put(ProtocolStatus.RETRY, HttpServletResponse.SC_BAD_REQUEST);
    protoCodes.put(ProtocolStatus.ROBOTS_DENIED, HttpServletResponse.SC_FORBIDDEN);
    protoCodes.put(ProtocolStatus.SUCCESS, HttpServletResponse.SC_OK);
    protoCodes.put(ProtocolStatus.TEMP_MOVED, HttpServletResponse.SC_MOVED_TEMPORARILY);
    protoCodes.put(ProtocolStatus.WOULDBLOCK, HttpServletResponse.SC_BAD_REQUEST);
  }
  
  private static class SegmentPathFilter implements PathFilter {
    public static final SegmentPathFilter INSTANCE = new SegmentPathFilter();
    
    @Override
    public boolean accept(Path p) {
      return p.getName().startsWith("part-");
    }
    
  }
  
  private static class Segment implements Closeable {
    
    private static final Partitioner PARTITIONER = new HashPartitioner();

    private FileSystem fs;
    private Path segmentDir;

    private Object cLock = new Object();
    private Object crawlLock = new Object();
    private MapFile.Reader[] content;
    private MapFile.Reader[] parseText;
    private MapFile.Reader[] parseData;
    private MapFile.Reader[] crawl;
    private Configuration conf;

    public Segment(FileSystem fs, Path segmentDir, Configuration conf) throws IOException {
      this.fs = fs;
      this.segmentDir = segmentDir;
      this.conf = conf;
    }

    public CrawlDatum getCrawlDatum(Text url) throws IOException {
      synchronized (crawlLock) {
        if (crawl == null)
          crawl = getReaders(CrawlDatum.FETCH_DIR_NAME);
      }
      return (CrawlDatum)getEntry(crawl, url, new CrawlDatum());
    }
    
    public Content getContent(Text url) throws IOException {
      synchronized (cLock) {
        if (content == null)
          content = getReaders(Content.DIR_NAME);
      }
      return (Content)getEntry(content, url, new Content());
    }

    /** Open the output generated by this format. */
    private MapFile.Reader[] getReaders(String subDir) throws IOException {
      Path dir = new Path(segmentDir, subDir);
      FileSystem fs = dir.getFileSystem(conf);
      Path[] names = FileUtil.stat2Paths(fs.listStatus(dir, SegmentPathFilter.INSTANCE));

      // sort names, so that hash partitioning works
      Arrays.sort(names);
      
      MapFile.Reader[] parts = new MapFile.Reader[names.length];
      for (int i = 0; i < names.length; i++) {
        parts[i] = new MapFile.Reader(fs, names[i].toString(), conf);
      }
      return parts;
    }
    
    private Writable getEntry(MapFile.Reader[] readers, Text url,
                              Writable entry) throws IOException {
      return MapFileOutputFormat.getEntry(readers, PARTITIONER, url, entry);
    }

    public void close() throws IOException {
      if (content != null) { closeReaders(content); }
      if (parseText != null) { closeReaders(parseText); }
      if (parseData != null) { closeReaders(parseData); }
      if (crawl != null) { closeReaders(crawl); }
    }

    private void closeReaders(MapFile.Reader[] readers) throws IOException {
      for (int i = 0; i < readers.length; i++) {
        readers[i].close();
      }
    }

  }
  
  public SegmentHandler(Configuration conf, Path name) throws Exception {
    seg = new Segment(FileSystem.get(conf), name, conf);
  }

  @Override
  public void handle(Request req, HttpServletResponse res, String target,
          int dispatch) throws IOException, ServletException {
    try {
      String uri = req.getUri().toString();
      LOG.info("URI: " + uri);
      addMyHeader(res, "URI", uri);
      Text url = new Text(uri.toString());
      CrawlDatum cd = seg.getCrawlDatum(url);
      if (cd != null) {
        addMyHeader(res, "Res", "found");
        LOG.info("-got " + cd.toString());
        ProtocolStatus ps = (ProtocolStatus)cd.getMetaData().get(Nutch.WRITABLE_PROTO_STATUS_KEY);
        if (ps != null) {
          Integer TrCode = protoCodes.get(ps.getCode());
          if (TrCode != null) {
            res.setStatus(TrCode.intValue());            
          } else {
            res.setStatus(HttpServletResponse.SC_OK);
          }
          addMyHeader(res, "ProtocolStatus", ps.toString());
        } else {
          res.setStatus(HttpServletResponse.SC_OK);          
        }
        Content c = seg.getContent(url);
        if (c == null) { // missing content
          req.setHandled(true);
          res.addHeader("X-Handled-By", getClass().getSimpleName());
          return;
        }
        byte[] data = c.getContent();
        LOG.debug("-data len=" + data.length);
        Metadata meta = c.getMetadata();
        String[] names = meta.names();
        LOG.debug("- " + names.length + " meta");
        for (int i = 0; i < names.length; i++) {
          boolean my = true;
          char ch = names[i].charAt(0);
          if (Character.isLetter(ch) && Character.isUpperCase(ch)) {
            // pretty good chance it's a standard header
            my = false;
          }
          String[] values = meta.getValues(names[i]);
          for (int k = 0; k < values.length; k++) {
            if (my) {
              addMyHeader(res, names[i], values[k]);
            } else {
              res.addHeader(names[i], values[k]);
            }
          }
        }
        req.setHandled(true);
        res.addHeader("X-Handled-By", getClass().getSimpleName());
        res.setContentType(meta.get(Metadata.CONTENT_TYPE));
        res.setContentLength(data.length);
        OutputStream os = res.getOutputStream();
        os.write(data, 0, data.length);
        res.flushBuffer();
      } else {
        addMyHeader(res, "Res", "not found");
        LOG.info(" -not found " + url);
      }
    } catch (Exception e) {
      e.printStackTrace();
      LOG.warn(StringUtils.stringifyException(e));
      addMyHeader(res, "Res", "Exception: " + StringUtils.stringifyException(e));
    }
  }

}
