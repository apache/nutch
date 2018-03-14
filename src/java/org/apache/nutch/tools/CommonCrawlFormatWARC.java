package org.apache.nutch.tools;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.parse.ParseData;

import org.apache.nutch.parse.ParseSegment;
import org.apache.nutch.protocol.Content;
import org.archive.format.warc.WARCConstants;
import org.archive.io.WriterPoolMember;
import org.archive.io.warc.WARCRecordInfo;
import org.archive.io.warc.WARCWriter;
import org.archive.io.warc.WARCWriterPoolSettingsData;
import org.archive.uid.UUIDGenerator;
import org.archive.util.DateUtils;
import org.archive.util.anvl.ANVLRecord;

public class CommonCrawlFormatWARC extends AbstractCommonCrawlFormat {

  public static final String MAX_WARC_FILE_SIZE = "warc.file.size.max";
  public static final String TEMPLATE = "${prefix}-${timestamp17}-${serialno}";

  private static final AtomicInteger SERIALNO = new AtomicInteger();
  private static final UUIDGenerator GENERATOR = new UUIDGenerator();

  private String outputDir = null;
  private ByteArrayOutputStream out;
  private WARCWriter writer;
  private ParseData parseData;

  public CommonCrawlFormatWARC(Configuration nutchConf,
      CommonCrawlConfig config) throws IOException {
    super(null, null, null, nutchConf, config);

    this.out = new ByteArrayOutputStream();

    ANVLRecord info = WARCUtils.getWARCInfoContent(nutchConf);
    List<String> md = Collections.singletonList(info.toString());

    this.outputDir = config.getOutputDir();

    if (null == outputDir) {
      String message = "Missing output directory configuration: " + outputDir;

      throw new RuntimeException(message);
    }

    File file = new File(outputDir);

    long maxSize = WARCConstants.DEFAULT_MAX_WARC_FILE_SIZE;

    if (config.getWarcSize() > 0) {
      maxSize = config.getWarcSize();
    }

    WARCWriterPoolSettingsData settings = new WARCWriterPoolSettingsData(
        WriterPoolMember.DEFAULT_PREFIX, TEMPLATE, maxSize,
        config.isCompressed(), Arrays.asList(new File[] { file }), md,
        new UUIDGenerator());

    writer = new WARCWriter(SERIALNO, settings);
  }

  public CommonCrawlFormatWARC(String url, Content content, Metadata metadata,
      Configuration nutchConf, CommonCrawlConfig config, ParseData parseData)
      throws IOException {
    super(url, content, metadata, nutchConf, config);

    this.out = new ByteArrayOutputStream();
    this.parseData = parseData;

    ANVLRecord info = WARCUtils.getWARCInfoContent(conf);
    List<String> md = Collections.singletonList(info.toString());

    this.outputDir = config.getOutputDir();

    if (null == outputDir) {
      String message = "Missing output directory configuration: " + outputDir;

      throw new RuntimeException(message);
    }

    File file = new File(outputDir);

    long maxSize = WARCConstants.DEFAULT_MAX_WARC_FILE_SIZE;

    if (config.getWarcSize() > 0) {
      maxSize = config.getWarcSize();
    }

    WARCWriterPoolSettingsData settings = new WARCWriterPoolSettingsData(
        WriterPoolMember.DEFAULT_PREFIX, TEMPLATE, maxSize,
        config.isCompressed(), Arrays.asList(new File[] { file }), md,
        new UUIDGenerator());

    writer = new WARCWriter(SERIALNO, settings);
  }

  public String getJsonData(String url, Content content, Metadata metadata,
      ParseData parseData) throws IOException {
    this.url = url;
    this.content = content;
    this.metadata = metadata;
    this.parseData = parseData;

    return this.getJsonData();
  }

  @Override
  public String getJsonData() throws IOException {

    long position = writer.getPosition();

    try {
      // See if we need to open a new file because we've exceeded maxBytes

      // checkSize will open a new file if we exceeded the maxBytes setting
      writer.checkSize();

      if (writer.getPosition() != position) {
        // We just closed the file because it was larger than maxBytes.
        position = writer.getPosition();
      }

      // response record
      URI id = writeResponse();

      if (StringUtils.isNotBlank(metadata.get("_request_"))) {
        // write the request method if any request info is found
        writeRequest(id);
      }
    } catch (IOException e) {
      // Launch the corresponding IO error
      throw e;
    } catch (ParseException e) {
      // do nothing, as we can't establish a valid WARC-Date for this record
      // lets skip it altogether
      LOG.error("Can't get a valid date from: {}", url);
    }

    return null;
  }

  protected URI writeResponse() throws IOException, ParseException {
    WARCRecordInfo record = new WARCRecordInfo();

    record.setType(WARCConstants.WARCRecordType.response);
    record.setUrl(getUrl());

    String fetchTime;

    record.setCreate14DigitDate(DateUtils
        .getLog14Date(Long.parseLong(metadata.get("nutch.fetch.time"))));
    record.setMimetype(WARCConstants.HTTP_RESPONSE_MIMETYPE);
    record.setRecordId(GENERATOR.getRecordID());

    String IP = getResponseAddress();

    if (StringUtils.isNotBlank(IP))
      record.addExtraHeader(WARCConstants.HEADER_KEY_IP, IP);

    if (ParseSegment.isTruncated(content))
      record.addExtraHeader(WARCConstants.HEADER_KEY_TRUNCATED, "unspecified");

    ByteArrayOutputStream output = new ByteArrayOutputStream();

    String httpHeaders = metadata.get("_response.headers_");

    if (StringUtils.isNotBlank(httpHeaders)) {
      output.write(httpHeaders.getBytes());
    } else {
      // change the record type to resource as we not have information about
      // the headers
      record.setType(WARCConstants.WARCRecordType.resource);
      record.setMimetype(content.getContentType());
    }

    output.write(getResponseContent().getBytes());

    record.setContentLength(output.size());
    record.setContentStream(new ByteArrayInputStream(output.toByteArray()));

    if (output.size() > 0) {
      // avoid generating a 0 sized record, as the webarchive library will
      // complain about it
      writer.writeRecord(record);
    }

    return record.getRecordId();
  }

  protected URI writeRequest(URI id) throws IOException, ParseException {
    WARCRecordInfo record = new WARCRecordInfo();

    record.setType(WARCConstants.WARCRecordType.request);
    record.setUrl(getUrl());
    record.setCreate14DigitDate(DateUtils
        .getLog14Date(Long.parseLong(metadata.get("nutch.fetch.time"))));
    record.setMimetype(WARCConstants.HTTP_REQUEST_MIMETYPE);
    record.setRecordId(GENERATOR.getRecordID());

    if (id != null) {
      ANVLRecord headers = new ANVLRecord();
      headers.addLabelValue(WARCConstants.HEADER_KEY_CONCURRENT_TO,
          '<' + id.toString() + '>');
      record.setExtraHeaders(headers);
    }

    ByteArrayOutputStream output = new ByteArrayOutputStream();

    output.write(metadata.get("_request_").getBytes());
    record.setContentLength(output.size());
    record.setContentStream(new ByteArrayInputStream(output.toByteArray()));

    writer.writeRecord(record);

    return record.getRecordId();
  }

  @Override
  protected String generateJson() throws IOException {
    return null;
  }

  @Override
  protected void writeKeyValue(String key, String value) throws IOException {
    throw new NotImplementedException();
  }

  @Override
  protected void writeKeyNull(String key) throws IOException {
    throw new NotImplementedException();
  }

  @Override
  protected void startArray(String key, boolean nested, boolean newline)
      throws IOException {
    throw new NotImplementedException();
  }

  @Override
  protected void closeArray(String key, boolean nested, boolean newline)
      throws IOException {
    throw new NotImplementedException();
  }

  @Override
  protected void writeArrayValue(String value) throws IOException {
    throw new NotImplementedException();
  }

  @Override
  protected void startObject(String key) throws IOException {
    throw new NotImplementedException();
  }

  @Override
  protected void closeObject(String key) throws IOException {
    throw new NotImplementedException();
  }

  @Override
  public void close() {
    if (writer != null)
      try {
        writer.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
  }
}
