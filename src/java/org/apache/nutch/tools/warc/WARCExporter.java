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
package org.apache.nutch.tools.warc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.NutchWritable;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseSegment;
import org.apache.nutch.parse.ParseText;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.tools.WARCUtils;
import org.apache.nutch.util.HadoopFSUtil;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.TimingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import com.martinkl.warc.WARCRecord;
import com.martinkl.warc.WARCWritable;
import com.martinkl.warc.mapreduce.WARCOutputFormat;

/**
 * MapReduce job to exports Nutch segments as WARC files. The file format is
 * documented in the [ISO
 * Standard](http://bibnum.bnf.fr/warc/WARC_ISO_28500_version1_latestdraft.pdf).
 * Generates elements of type response if the configuration 'store.http.headers'
 * was set to true during the fetching and the http headers were stored
 * verbatim; generates elements of type 'resource' otherwise.
 */
public class WARCExporter extends Configured implements Tool {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());
  private static final String ONLY_SUCCESSFUL_RESPONSES = "warc.exporter.only.successful.responses";
  private static final String CRLF = "\r\n";
  private static final byte[] CRLF_BYTES = { 13, 10 };

  public WARCExporter() {
    super(null);
  }

  public WARCExporter(Configuration conf) {
    super(conf);
  }

  public static class WARCMapReduce {

    public static class WARCMapper
        extends Mapper<Text, Writable, Text, NutchWritable> {
      @Override
      public void map(Text key, Writable value, Context context)
          throws IOException, InterruptedException {
        context.write(key, new NutchWritable(value));
      }
    }

    public static class WARCReducer
        extends Reducer<Text, NutchWritable, NullWritable, WARCWritable> {

      // Metadata to JSON
      Gson gson = new Gson();

      @Override
      public void reduce(Text key, Iterable<NutchWritable> values,
          Context context) throws IOException, InterruptedException {
        boolean onlySuccessfulResponses = context.getConfiguration()
            .getBoolean(ONLY_SUCCESSFUL_RESPONSES, false);
        ParseData parseData = null;
        ParseText parseText = null;
        Content content = null;
        CrawlDatum cd = null;
        SimpleDateFormat warcdf = new SimpleDateFormat(
            "yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.ENGLISH);

        // aggregate the values found
        for (NutchWritable val : values) {
          final Writable value = val.get(); // unwrap
          if (value instanceof Content) {
            content = (Content) value;
            continue;
          }
          if (value instanceof CrawlDatum) {
            cd = (CrawlDatum) value;
            continue;
          }
          if (value instanceof ParseData) {
            parseData = (ParseData) value;
            continue;
          }
          if (value instanceof ParseText) {
            parseText = (ParseText) value;
            continue;
          }
        }

        // check that we have everything we need
        if (content == null) {
          LOG.info("Missing content for {}", key);
          context.getCounter("WARCExporter", "missing content").increment(1);
          return;
        }

        if (cd == null) {
          LOG.info("Missing fetch datum for {}", key);
          context.getCounter("WARCExporter", "missing metadata").increment(1);
          return;
        }

        if (onlySuccessfulResponses) {
          // Empty responses is everything that was not a regular response
          if (!(cd.getStatus() == CrawlDatum.STATUS_FETCH_SUCCESS
              || cd.getStatus() == CrawlDatum.STATUS_FETCH_NOTMODIFIED)) {
            context.getCounter("WARCExporter", "omitted empty response")
                .increment(1);
            return;
          }
        }

        // were the headers stored as is? Can write a response element then
        String headersVerbatim = content.getMetadata()
            .get("_response.headers_");
        headersVerbatim = WARCUtils.fixHttpHeaders(headersVerbatim,
            content.getContent().length);
        byte[] httpheaders = new byte[0];
        if (StringUtils.isNotBlank(headersVerbatim)) {
          // check that ends with an empty line
          if (!headersVerbatim.endsWith(CRLF + CRLF)) {
            headersVerbatim += CRLF + CRLF;
          }
          httpheaders = headersVerbatim.getBytes();
        }

        String mainId = UUID.randomUUID().toString();
        StringBuilder buffer = new StringBuilder();
        buffer.append(WARCRecord.WARC_VERSION);
        buffer.append(CRLF);
        buffer.append("WARC-Record-ID").append(": ").append("<urn:uuid:")
            .append(mainId).append(">").append(CRLF);

        int contentLength = 0;
        if (content != null) {
          contentLength = content.getContent().length;
        }

        // add the length of the http header
        contentLength += httpheaders.length;

        buffer.append("Content-Length").append(": ")
            .append(Integer.toString(contentLength)).append(CRLF);

        Date fetchedDate = new Date(cd.getFetchTime());
        buffer.append("WARC-Date").append(": ")
            .append(warcdf.format(fetchedDate)).append(CRLF);

        // check if http headers have been stored verbatim
        // if not generate a response instead
        String warcTypeValue = "resource";

        if (StringUtils.isNotBlank(headersVerbatim)) {
          warcTypeValue = "response";
        }

        buffer.append("WARC-Type").append(": ").append(warcTypeValue)
            .append(CRLF);

        // "WARC-IP-Address" if present
        String IP = content.getMetadata().get("_ip_");
        if (StringUtils.isNotBlank(IP)) {
          buffer.append("WARC-IP-Address").append(": ").append("IP")
              .append(CRLF);
        }

        // detect if truncated only for fetch success
        String status = CrawlDatum.getStatusName(cd.getStatus());
        if (status.equalsIgnoreCase("STATUS_FETCH_SUCCESS")
            && ParseSegment.isTruncated(content)) {
          buffer.append("WARC-Truncated").append(": ").append("unspecified")
              .append(CRLF);
        }

        // must be a valid URI
        try {
          String normalised = key.toString().replaceAll(" ", "%20");
          URI uri = URI.create(normalised);
          buffer.append("WARC-Target-URI").append(": ")
              .append(uri.toASCIIString()).append(CRLF);
        } catch (Exception e) {
          LOG.error("Invalid URI {} ", key);
          context.getCounter("WARCExporter", "invalid URI").increment(1);
          return;
        }

        // provide a ContentType if type response
        if (warcTypeValue.equals("response")) {
          buffer.append("Content-Type: application/http; msgtype=response")
              .append(CRLF);
        }

        // finished writing the WARC headers, now let's serialize it

        ByteArrayOutputStream bos = new ByteArrayOutputStream();

        // store the headers
        bos.write(buffer.toString().getBytes("UTF-8"));
        bos.write(CRLF_BYTES);
        // the http headers
        bos.write(httpheaders);

        // the binary content itself
        if (content.getContent() != null) {
          bos.write(content.getContent());
        }
        bos.write(CRLF_BYTES);
        bos.write(CRLF_BYTES);

        try {
          DataInput in = new DataInputStream(
              new ByteArrayInputStream(bos.toByteArray()));
          WARCRecord record = new WARCRecord(in);
          context.write(NullWritable.get(), new WARCWritable(record));
          context.getCounter("WARCExporter", "records generated").increment(1);
        } catch (IOException | IllegalStateException exception) {
          LOG.error(
              "Exception when generating WARC resource record for {} : {}", key,
              exception.getMessage());
          context.getCounter("WARCExporter", "exception").increment(1);
        }

        // Do we need to emit a metadata record too?
        if (parseData != null) {
          // Header builder
          buffer = new StringBuilder();

          JsonObject jsonObject = new JsonObject();
          jsonObject.add("contentMeta",
              metadataToJson(parseData.getContentMeta()));
          jsonObject.add("parseMeta", metadataToJson(parseData.getParseMeta()));

          // Payload builder
          StringBuilder payload = new StringBuilder();
          payload.append(gson.toJson(jsonObject));
          payload.append(CRLF);

          buffer.append(WARCRecord.WARC_VERSION);
          buffer.append(CRLF);
          buffer.append("WARC-Record-ID").append(": ").append("<urn:uuid:")
              .append(UUID.randomUUID().toString()).append(">").append(CRLF);
          buffer.append("WARC-Refers-To").append(": ").append("<urn:uuid:")
              .append(mainId).append(">").append(CRLF);
          buffer.append("WARC-Date").append(": ")
              .append(warcdf.format(fetchedDate)).append(CRLF);
          buffer.append("WARC-Type").append(": ").append("metadata")
              .append(CRLF);
          buffer.append("Content-Type").append(": ").append("application/json")
              .append(CRLF);

          contentLength = payload.toString().getBytes("UTF-8").length;
          buffer.append("Content-Length").append(": ")
              .append(Integer.toString(contentLength)).append(CRLF);

          try {
            String normalised = key.toString().replaceAll(" ", "%20");
            URI uri = URI.create(normalised);
            buffer.append("WARC-Target-URI").append(": ")
                .append(uri.toASCIIString()).append(CRLF);
          } catch (Exception e) {
            LOG.error("Invalid URI {} ", key);
            context.getCounter("WARCExporter", "invalid URI").increment(1);
            return;
          }

          bos = new ByteArrayOutputStream();
          bos.write(buffer.toString().getBytes("UTF-8"));
          bos.write(CRLF_BYTES); // separate header and payload
          bos.write(payload.toString().getBytes("UTF-8"));
          bos.write(CRLF_BYTES);
          bos.write(CRLF_BYTES); // separation between records

          try {
            DataInput in = new DataInputStream(
                new ByteArrayInputStream(bos.toByteArray()));
            WARCRecord record = new WARCRecord(in);
            context.write(NullWritable.get(), new WARCWritable(record));
            context.getCounter("WARCExporter", "records generated")
                .increment(1);
          } catch (IOException | IllegalStateException exception) {
            LOG.error(
                "Exception when generating WARC metadata record for {} : {}",
                key, exception.getMessage(), exception);
            context.getCounter("WARCExporter", "exception").increment(1);
          }
        }

        // Do we need to emit a text record too?
        if (parseText != null) {
          // Header builder
          buffer = new StringBuilder();

          // Payload builder
          StringBuilder payload = new StringBuilder();
          payload.append(parseText);
          payload.append(CRLF);

          buffer.append(WARCRecord.WARC_VERSION);
          buffer.append(CRLF);
          buffer.append("WARC-Record-ID").append(": ").append("<urn:uuid:")
              .append(UUID.randomUUID().toString()).append(">").append(CRLF);
          buffer.append("WARC-Refers-To").append(": ").append("<urn:uuid:")
              .append(mainId).append(">").append(CRLF);
          buffer.append("WARC-Date").append(": ")
              .append(warcdf.format(fetchedDate)).append(CRLF);
          buffer.append("WARC-Type").append(": ").append("conversion")
              .append(CRLF);
          buffer.append("Content-Type").append(": ").append("text/plain")
              .append(CRLF);

          contentLength = payload.toString().getBytes("UTF-8").length;
          buffer.append("Content-Length").append(": ")
              .append(Integer.toString(contentLength)).append(CRLF);

          try {
            String normalised = key.toString().replaceAll(" ", "%20");
            URI uri = URI.create(normalised);
            buffer.append("WARC-Target-URI").append(": ")
                .append(uri.toASCIIString()).append(CRLF);
          } catch (Exception e) {
            LOG.error("Invalid URI {} ", key);
            context.getCounter("WARCExporter", "invalid URI").increment(1);
            return;
          }

          bos = new ByteArrayOutputStream();
          bos.write(buffer.toString().getBytes("UTF-8"));
          bos.write(CRLF_BYTES); // separate header and payload
          bos.write(payload.toString().getBytes("UTF-8"));
          bos.write(CRLF_BYTES);
          bos.write(CRLF_BYTES); // separation between records

          try {
            DataInput in = new DataInputStream(
                new ByteArrayInputStream(bos.toByteArray()));
            WARCRecord record = new WARCRecord(in);
            context.write(NullWritable.get(), new WARCWritable(record));
            context.getCounter("WARCExporter", "records generated")
                .increment(1);
          } catch (IOException | IllegalStateException exception) {
            LOG.error(
                "Exception when generating WARC metadata record for {} : {}",
                key, exception.getMessage(), exception);
            context.getCounter("WARCExporter", "exception").increment(1);
          }
        }
      }

      /**
       * Adds keys/values of a Nuta metadata container to a JsonObject.
       *
       * @param meta
       *          Nutch metadata container
       * @return json object
       */
      protected JsonObject metadataToJson(Metadata meta) {
        JsonObject obj = new JsonObject();

        for (String key : meta.names()) {
          if (meta.isMultiValued(key)) {
            obj.add(key, gson.toJsonTree(meta.getValues(key)));
          } else {
            obj.addProperty(key, meta.get(key));
          }
        }

        return obj;
      }
    }
  }

  public int generateWARC(String output, List<Path> segments,
      boolean onlySuccessfulResponses, boolean includeParseData,
      boolean includeParseText) throws IOException {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    long start = System.currentTimeMillis();
    LOG.info("WARCExporter: starting at {}", sdf.format(start));

    final Job job = NutchJob.getInstance(getConf());
    job.setJobName("warc-exporter " + output);

    job.getConfiguration().setBoolean(ONLY_SUCCESSFUL_RESPONSES,
        onlySuccessfulResponses);

    for (final Path segment : segments) {
      LOG.info("warc-exporter: adding segment: {}", segment);
      FileInputFormat.addInputPath(job, new Path(segment, Content.DIR_NAME));
      FileInputFormat.addInputPath(job,
          new Path(segment, CrawlDatum.FETCH_DIR_NAME));

      if (includeParseData) {
        FileInputFormat.addInputPath(job,
            new Path(segment, ParseData.DIR_NAME));
      }

      if (includeParseText) {
        FileInputFormat.addInputPath(job,
            new Path(segment, ParseText.DIR_NAME));
      }
    }

    job.setInputFormatClass(SequenceFileInputFormat.class);

    job.setJarByClass(WARCMapReduce.class);
    job.setMapperClass(WARCMapReduce.WARCMapper.class);
    job.setReducerClass(WARCMapReduce.WARCReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(NutchWritable.class);

    FileOutputFormat.setOutputPath(job, new Path(output));
    // using the old api
    job.setOutputFormatClass(WARCOutputFormat.class);

    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(WARCWritable.class);

    try {
      boolean success = job.waitForCompletion(true);
      if (!success) {
        String message = NutchJob.getJobFailureLogMessage("WARCExporter", job);
        LOG.error(message);
        throw new RuntimeException(message);
      }
      LOG.info(job.getCounters().toString());
      long end = System.currentTimeMillis();
      LOG.info("WARCExporter: finished at {}, elapsed: {}", sdf.format(end),
          TimingUtil.elapsedTime(start, end));
    } catch (IOException | InterruptedException | ClassNotFoundException e) {
      LOG.error("WARCExporter job failed: {}", e.getMessage());
      return -1;
    }

    return 0;
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println(
          "Usage: WARCExporter <output> (<segment> ... | -dir <segments>) [-onlySuccessfulResponses] [-includeParseData] [-includeParseText]");
      return -1;
    }

    boolean onlySuccessfulResponses = false;
    boolean includeParseData = false;
    boolean includeParseText = false;
    final List<Path> segments = new ArrayList<>();

    for (int i = 1; i < args.length; i++) {
      if (args[i].equals("-onlySuccessfulResponses")) {
        onlySuccessfulResponses = true;
        continue;
      }
      if (args[i].equals("-includeParseData")) {
        includeParseData = true;
        continue;
      }
      if (args[i].equals("-includeParseText")) {
        includeParseText = true;
        continue;
      }
      if (args[i].equals("-dir")) {
        Path dir = new Path(args[++i]);
        FileSystem fs = dir.getFileSystem(getConf());
        FileStatus[] fstats = fs.listStatus(dir,
            HadoopFSUtil.getPassDirectoriesFilter(fs));
        Path[] files = HadoopFSUtil.getPaths(fstats);
        for (Path p : files) {
          segments.add(p);
        }
      } else {
        segments.add(new Path(args[i]));
      }
    }

    return generateWARC(args[0], segments, onlySuccessfulResponses,
        includeParseData, includeParseText);
  }

  public static void main(String[] args) throws Exception {
    final int res = ToolRunner.run(NutchConfiguration.create(),
        new WARCExporter(), args);
    System.exit(res);
  }
}
