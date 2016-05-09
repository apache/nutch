/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nutch.tools;

//JDK imports

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
//Commons imports
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.FilenameUtils;

//Hadoop
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.Inlink;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.crawl.LinkDbReader;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.DumpFileUtil;
import org.apache.nutch.util.NutchConfiguration;
//Tika imports
import org.apache.tika.Tika;

import com.fasterxml.jackson.dataformat.cbor.CBORFactory;
import com.fasterxml.jackson.dataformat.cbor.CBORGenerator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.icu.text.DateFormat;
import com.ibm.icu.text.SimpleDateFormat;

/**
 * <p>
 * The Common Crawl Data Dumper tool enables one to reverse generate the raw
 * content from Nutch segment data directories into a common crawling data
 * format, consumed by many applications. The data is then serialized as <a
 * href="http://cbor.io">CBOR</a>
 * </p>
 * <p>
 * Text content will be stored in a structured document format. Below is a
 * schema for storage of data and metadata related to a crawling request, with
 * the response body truncated for readability. This document must be encoded
 * using CBOR and should be compressed with gzip after encoding. The timestamped
 * URL key for these records' keys follows the same layout as the media file
 * directory structure, with underscores in place of directory separators. </li>
 * </p>
 * <p>
 * Thus, the timestamped url key for the record is provided below followed by an
 * example record:
 * <p/>
 * <pre>
 * {@code
 * com_somepage_33a3e36bbef59c2a5242c2ccee59239ab30d51f3_1411623696000
 *
 *     {
 *         "url": "http:\/\/somepage.com\/22\/14560817",
 *         "timestamp": "1411623696000",
 *         "request": {
 *             "method": "GET",
 *             "client": {
 *                 "hostname": "crawler01.local",
 *                 "address": "74.347.129.200",
 *                 "software": "Apache Nutch v1.10",
 *                 "robots": "classic",
 *                 "contact": {
 *                     "name": "Nutch Admin",
 *                     "email": "nutch.pro@nutchadmin.org"
 *                 }
 *             },
 *             "headers": {
 *                 "Accept": "text\/html,application\/xhtml+xml,application\/xml",
 *                 "Accept-Encoding": "gzip,deflate,sdch",
 *                 "Accept-Language": "en-US,en",
 *                 "User-Agent": "Mozilla\/5.0",
 *                 "...": "..."
 *             },
 *             "body": null
 *         },
 *         "response": {
 *             "status": "200",
 *             "server": {
 *                 "hostname": "somepage.com",
 *                 "address": "55.33.51.19",
 *             },
 *             "headers": {
 *                 "Content-Encoding": "gzip",
 *                 "Content-Type": "text\/html",
 *                 "Date": "Thu, 25 Sep 2014 04:16:58 GMT",
 *                 "Expires": "Thu, 25 Sep 2014 04:16:57 GMT",
 *                 "Server": "nginx",
 *                 "...": "..."
 *             },
 *             "body": "\r\n  <!DOCTYPE html PUBLIC ... \r\n\r\n  \r\n    </body>\r\n    </html>\r\n  \r\n\r\n",
 *         },
 *         "key": "com_somepage_33a3e36bbef59c2a5242c2ccee59239ab30d51f3_1411623696000",
 *         "imported": "1411623698000"
 *     }
 *     }
 * </pre>
 * <p/>
 * <p>
 * Upon successful completion the tool displays a very convenient JSON snippet
 * detailing the mimetype classifications and the counts of documents which fall
 * into those classifications. An example is as follows:
 * </p>
 * <p/>
 * <pre>
 * {@code
 * INFO: File Types:
 *   TOTAL Stats:    {
 *     {"mimeType":"application/xml","count":19"}
 *     {"mimeType":"image/png","count":47"}
 *     {"mimeType":"image/jpeg","count":141"}
 *     {"mimeType":"image/vnd.microsoft.icon","count":4"}
 *     {"mimeType":"text/plain","count":89"}
 *     {"mimeType":"video/quicktime","count":2"}
 *     {"mimeType":"image/gif","count":63"}
 *     {"mimeType":"application/xhtml+xml","count":1670"}
 *     {"mimeType":"application/octet-stream","count":40"}
 *     {"mimeType":"text/html","count":1863"}
 *   }
 * }
 * </pre>
 */
public class CommonCrawlDataDumper extends Configured implements Tool {

  private static final Logger LOG = LoggerFactory
      .getLogger(CommonCrawlDataDumper.class.getName());
  private static final int MAX_INLINKS = 5000;
  
  private CommonCrawlConfig config = null;

  // Gzip initialization
  private FileOutputStream fileOutput = null;
  private BufferedOutputStream bufOutput = null;
  private GzipCompressorOutputStream gzipOutput = null;
  private TarArchiveOutputStream tarOutput = null;
  private ArrayList<String> fileList = null;

  /**
   * Main method for invoking this tool
   *
   * @param args 1) output directory (which will be created if it does not
   *             already exist) to host the CBOR data and 2) a directory
   *             containing one or more segments from which we wish to generate
   *             CBOR data from. Optionally, 3) a list of mimetypes and the 4)
   *             the gzip option may be provided.
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    Configuration conf = NutchConfiguration.create();
    int res = ToolRunner.run(conf, new CommonCrawlDataDumper(), args);
    System.exit(res);
  }

  /**
   * Constructor
   */
  public CommonCrawlDataDumper(CommonCrawlConfig config) {
    this.config = config;
  }

  public CommonCrawlDataDumper() {
  }

  /**
   * Dumps the reverse engineered CBOR content from the provided segment
   * directories if a parent directory contains more than one segment,
   * otherwise a single segment can be passed as an argument. If the boolean
   * argument is provided then the CBOR is also zipped.
   *
   * @param outputDir      the directory you wish to dump the raw content to. This
   *                       directory will be created.
   * @param segmentRootDir a directory containing one or more segments.
   * @param linkdb         Path to linkdb.
   * @param gzip           a boolean flag indicating whether the CBOR content should also
   *                       be gzipped.
   * @param epochFilename  if {@code true}, output files will be names using the epoch time (in milliseconds).
   * @param extension      a file extension to use with output documents.
   * @throws Exception if any exception occurs.
   */
  public void dump(File outputDir, File segmentRootDir, File linkdb, boolean gzip,
      String[] mimeTypes, boolean epochFilename, String extension, boolean warc)
      throws Exception {
    if (gzip) {
      LOG.info("Gzipping CBOR data has been skipped");
    }
    // total file counts
    Map<String, Integer> typeCounts = new HashMap<String, Integer>();
    // filtered file counters
    Map<String, Integer> filteredCounts = new HashMap<String, Integer>();

    Configuration nutchConfig = NutchConfiguration.create();
    final FileSystem fs = FileSystem.get(nutchConfig);
    Path segmentRootPath = new Path(segmentRootDir.toString());

    //get all paths
    List<Path> parts = new ArrayList<>();
    RemoteIterator<LocatedFileStatus> files = fs.listFiles(segmentRootPath, true);
    String partPattern = ".*" + File.separator + Content.DIR_NAME
        + File.separator + "part-[0-9]{5}" + File.separator + "data";
    while (files.hasNext()) {
      LocatedFileStatus next = files.next();
      if (next.isFile()) {
        Path path = next.getPath();
        if (path.toString().matches(partPattern)){
          parts.add(path);
        }
      }
    }

    LinkDbReader linkDbReader = null;
    if (linkdb != null) {
      linkDbReader = new LinkDbReader(fs.getConf(), new Path(linkdb.toString()));
    }
    if (parts == null || parts.size() == 0) {
      LOG.error( "No segment directories found in {} ",
          segmentRootDir.getAbsolutePath());
      System.exit(1);
    }
    LOG.info("Found {} segment parts", parts.size());
    if (gzip && !warc) {
      fileList = new ArrayList<>();
      constructNewStream(outputDir);
    }

    for (Path segmentPart : parts) {
      LOG.info("Processing segment Part : [ {} ]", segmentPart);
      try {
        SequenceFile.Reader reader = new SequenceFile.Reader(nutchConfig,
            SequenceFile.Reader.file(segmentPart));

        Writable key = (Writable) reader.getKeyClass().newInstance();

        Content content = null;
        while (reader.next(key)) {
          content = new Content();
          reader.getCurrentValue(content);
          Metadata metadata = content.getMetadata();
          String url = key.toString();

          String baseName = FilenameUtils.getBaseName(url);
          String extensionName = FilenameUtils.getExtension(url);

          if (!extension.isEmpty()) {
            extensionName = extension;
          } else if ((extensionName == null) || extensionName.isEmpty()) {
            extensionName = "html";
          }

          String outputFullPath = null;
          String outputRelativePath = null;
          String filename = null;
          String timestamp = null;
          String reverseKey = null;

          if (epochFilename || config.getReverseKey()) {
            try {
              long epoch = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss z")
                  .parse(getDate(metadata.get("Date"))).getTime();
              timestamp = String.valueOf(epoch);
            } catch (ParseException pe) {
              LOG.warn(pe.getMessage());
            }

            reverseKey = reverseUrl(url);
            config.setReverseKeyValue(
                reverseKey.replace("/", "_") + "_" + DigestUtils.sha1Hex(url)
                    + "_" + timestamp);
          }

          if (!warc) {
            if (epochFilename) {
              outputFullPath = DumpFileUtil
                  .createFileNameFromUrl(outputDir.getAbsolutePath(),
                      reverseKey, url, timestamp, extensionName, !gzip);
              outputRelativePath = outputFullPath
                  .substring(0, outputFullPath.lastIndexOf(File.separator) - 1);
              filename = content.getMetadata().get(Metadata.DATE) + "."
                  + extensionName;
            } else {
              String md5Ofurl = DumpFileUtil.getUrlMD5(url);
              String fullDir = DumpFileUtil
                  .createTwoLevelsDirectory(outputDir.getAbsolutePath(),
                      md5Ofurl, !gzip);
              filename = DumpFileUtil
                  .createFileName(md5Ofurl, baseName, extensionName);
              outputFullPath = String.format("%s/%s", fullDir, filename);

              String[] fullPathLevels = fullDir.split(File.separator);
              String firstLevelDirName = fullPathLevels[fullPathLevels.length
                  - 2];
              String secondLevelDirName = fullPathLevels[fullPathLevels.length
                  - 1];
              outputRelativePath = firstLevelDirName + secondLevelDirName;
            }
          }
          // Encode all filetypes if no mimetypes have been given
          Boolean filter = (mimeTypes == null);

          String jsonData = "";
          try {
            String mimeType = new Tika().detect(content.getContent());
            // Maps file to JSON-based structure

            Set<String> inUrls = null; //there may be duplicates, so using set
            if (linkDbReader != null) {
              Inlinks inlinks = linkDbReader.getInlinks((Text) key);
              if (inlinks != null) {
                Iterator<Inlink> iterator = inlinks.iterator();
                inUrls = new LinkedHashSet<>();
                while (inUrls.size() <= MAX_INLINKS && iterator.hasNext()){
                  inUrls.add(iterator.next().getFromUrl());
                }
              }
            }
            //TODO: Make this Jackson Format implementation reusable
            try (CommonCrawlFormat format = CommonCrawlFormatFactory
                .getCommonCrawlFormat(warc ? "WARC" : "JACKSON", nutchConfig, config)) {
              if (inUrls != null) {
                format.setInLinks(new ArrayList<>(inUrls));
              }
              jsonData = format.getJsonData(url, content, metadata);
            }

            collectStats(typeCounts, mimeType);
            // collects statistics for the given mimetypes
            if ((mimeType != null) && (mimeTypes != null) && Arrays
                .asList(mimeTypes).contains(mimeType)) {
              collectStats(filteredCounts, mimeType);
              filter = true;
            }
          } catch (IOException ioe) {
            LOG.error("Fatal error in creating JSON data: " + ioe.getMessage());
            return;
          }

          if (!warc) {
            if (filter) {
              byte[] byteData = serializeCBORData(jsonData);

              if (!gzip) {
                File outputFile = new File(outputFullPath);
                if (outputFile.exists()) {
                  LOG.info("Skipping writing: [" + outputFullPath
                      + "]: file already exists");
                } else {
                  LOG.info("Writing: [" + outputFullPath + "]");
                  IOUtils.copy(new ByteArrayInputStream(byteData),
                      new FileOutputStream(outputFile));
                }
              } else {
                if (fileList.contains(outputFullPath)) {
                  LOG.info("Skipping compressing: [" + outputFullPath
                      + "]: file already exists");
                } else {
                  fileList.add(outputFullPath);
                  LOG.info("Compressing: [" + outputFullPath + "]");
                  //TarArchiveEntry tarEntry = new TarArchiveEntry(firstLevelDirName + File.separator + secondLevelDirName + File.separator + filename);
                  TarArchiveEntry tarEntry = new TarArchiveEntry(
                      outputRelativePath + File.separator + filename);
                  tarEntry.setSize(byteData.length);
                  tarOutput.putArchiveEntry(tarEntry);
                  tarOutput.write(byteData);
                  tarOutput.closeArchiveEntry();
                }
              }
            }
          }
        }
        reader.close();
      } catch (Exception e){
        LOG.warn("SKIPPED: {} Because : {}", segmentPart, e.getMessage());
      } finally {
        fs.close();
      }
    }

    if (gzip && !warc) {
      closeStream();
    }

    if (!typeCounts.isEmpty()) {
      LOG.info("CommonsCrawlDataDumper File Stats: " + DumpFileUtil
          .displayFileTypes(typeCounts, filteredCounts));
    }

  }

  private void closeStream() {
    try {
      tarOutput.finish();

      tarOutput.close();
      gzipOutput.close();
      bufOutput.close();
      fileOutput.close();
    } catch (IOException ioe) {
      LOG.warn("Error in closing stream: " + ioe.getMessage());
    }
  }

  private void constructNewStream(File outputDir) throws IOException {
    String archiveName = new SimpleDateFormat("yyyyMMddhhmm'.tar.gz'")
        .format(new Date());
    LOG.info("Creating a new gzip archive: " + archiveName);
    fileOutput = new FileOutputStream(
        new File(outputDir + File.separator + archiveName));
    bufOutput = new BufferedOutputStream(fileOutput);
    gzipOutput = new GzipCompressorOutputStream(bufOutput);
    tarOutput = new TarArchiveOutputStream(gzipOutput);
    tarOutput.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU);
  }

  /**
   * Writes the CBOR "Self-Describe Tag" (value 55799, serialized as 3-byte
   * sequence of {@code 0xd9d9f7}) at the current position. This method must
   * be used to write the CBOR magic number at the beginning of the document.
   * Since version 2.5, <a
   * href="https://github.com/FasterXML/jackson-dataformat-cbor"
   * >jackson-dataformat-cbor</a> will support the {@code WRITE_TYPE_HEADER}
   * feature to write that type tag at the beginning of the document.
   *
   * @param generator {@link CBORGenerator} object used to create a CBOR-encoded document.
   * @throws IOException if any I/O error occurs.
   * @see <a href="https://tools.ietf.org/html/rfc7049#section-2.4.5">RFC
   * 7049</a>
   */
  private void writeMagicHeader(CBORGenerator generator) throws IOException {
    // Writes self-describe CBOR
    // https://tools.ietf.org/html/rfc7049#section-2.4.5
    // It will be supported in jackson-cbor since 2.5
    byte[] header = new byte[3];
    header[0] = (byte) 0xd9;
    header[1] = (byte) 0xd9;
    header[2] = (byte) 0xf7;
    generator.writeBytes(header, 0, header.length);
  }

  private byte[] serializeCBORData(String jsonData) {
    CBORFactory factory = new CBORFactory();

    CBORGenerator generator = null;
    ByteArrayOutputStream stream = null;

    try {
      stream = new ByteArrayOutputStream();
      generator = factory.createGenerator(stream);
      // Writes CBOR tag
      writeMagicHeader(generator);
      generator.writeString(jsonData);
      generator.flush();
      stream.flush();

      return stream.toByteArray();

    } catch (Exception e) {
      LOG.warn("CBOR encoding failed: " + e.getMessage());
    } finally {
      try {
        generator.close();
        stream.close();
      } catch (IOException e) {
        // nothing to do
      }
    }

    return null;
  }

  private void collectStats(Map<String, Integer> typeCounts, String mimeType) {
    typeCounts.put(mimeType,
        typeCounts.containsKey(mimeType) ? typeCounts.get(mimeType) + 1 : 1);
  }

  /**
   * Gets the current date if the given timestamp is empty or null.
   *
   * @param timestamp the timestamp
   * @return the current timestamp if the given one is null.
   */
  private String getDate(String timestamp) {
    if (timestamp == null || timestamp.isEmpty()) {
      DateFormat dateFormat = new SimpleDateFormat(
          "EEE, d MMM yyyy HH:mm:ss z");
      timestamp = dateFormat.format(new Date());
    }
    return timestamp;

  }

  public static String reverseUrl(String urlString) {
    URL url;
    String reverseKey = null;
    try {
      url = new URL(urlString);

      String[] hostPart = url.getHost().replace('.', '/').split("/");

      StringBuilder sb = new StringBuilder();
      sb.append(hostPart[hostPart.length - 1]);
      for (int i = hostPart.length - 2; i >= 0; i--) {
        sb.append("/" + hostPart[i]);
      }

      reverseKey = sb.toString();

    } catch (MalformedURLException e) {
      LOG.error("Failed to parse URL: {}", urlString);
    }

    return reverseKey;
  }

  @Override
  public int run(String[] args) throws Exception {
    Option helpOpt = new Option("h", "help", false, "show this help message.");
    // argument options
    @SuppressWarnings("static-access")
    Option outputOpt = OptionBuilder.withArgName("outputDir").hasArg()
        .withDescription(
            "output directory (which will be created) to host the CBOR data.")
        .create("outputDir");
    // WARC format
    Option warcOpt = new Option("warc", "export to a WARC file");

    @SuppressWarnings("static-access")
    Option segOpt = OptionBuilder.withArgName("segment").hasArgs()
        .withDescription("the segment or directory containing segments to use").create("segment");
    // create mimetype and gzip options
    @SuppressWarnings("static-access")
    Option mimeOpt = OptionBuilder.isRequired(false).withArgName("mimetype")
        .hasArgs().withDescription(
            "an optional list of mimetypes to dump, excluding all others. Defaults to all.")
        .create("mimetype");
    @SuppressWarnings("static-access")
    Option gzipOpt = OptionBuilder.withArgName("gzip").hasArg(false)
        .withDescription(
            "an optional flag indicating whether to additionally gzip the data.")
        .create("gzip");
    @SuppressWarnings("static-access")
    Option keyPrefixOpt = OptionBuilder.withArgName("keyPrefix").hasArg(true)
        .withDescription("an optional prefix for key in the output format.")
        .create("keyPrefix");
    @SuppressWarnings("static-access")
    Option simpleDateFormatOpt = OptionBuilder.withArgName("SimpleDateFormat")
        .hasArg(false).withDescription(
            "an optional format for timestamp in GMT epoch milliseconds.")
        .create("SimpleDateFormat");
    @SuppressWarnings("static-access")
    Option epochFilenameOpt = OptionBuilder.withArgName("epochFilename")
        .hasArg(false)
        .withDescription("an optional format for output filename.")
        .create("epochFilename");
    @SuppressWarnings("static-access")
    Option jsonArrayOpt = OptionBuilder.withArgName("jsonArray").hasArg(false)
        .withDescription("an optional format for JSON output.")
        .create("jsonArray");
    @SuppressWarnings("static-access")
    Option reverseKeyOpt = OptionBuilder.withArgName("reverseKey").hasArg(false)
        .withDescription("an optional format for key value in JSON output.")
        .create("reverseKey");
    @SuppressWarnings("static-access")
    Option extensionOpt = OptionBuilder.withArgName("extension").hasArg(true)
        .withDescription("an optional file extension for output documents.")
        .create("extension");
    @SuppressWarnings("static-access")
    Option sizeOpt = OptionBuilder.withArgName("warcSize").hasArg(true)
        .withType(Number.class)
        .withDescription("an optional file size in bytes for the WARC file(s)")
        .create("warcSize");
    @SuppressWarnings("static-access")
    Option linkDbOpt = OptionBuilder.withArgName("linkdb").hasArg(true)
        .withDescription("an optional linkdb parameter to include inlinks in dump files")
        .isRequired(false)
        .create("linkdb");

    // create the options
    Options options = new Options();
    options.addOption(helpOpt);
    options.addOption(outputOpt);
    options.addOption(segOpt);
    // create mimetypes and gzip options
    options.addOption(warcOpt);
    options.addOption(mimeOpt);
    options.addOption(gzipOpt);
    // create keyPrefix option
    options.addOption(keyPrefixOpt);
    // create simpleDataFormat option
    options.addOption(simpleDateFormatOpt);
    options.addOption(epochFilenameOpt);
    options.addOption(jsonArrayOpt);
    options.addOption(reverseKeyOpt);
    options.addOption(extensionOpt);
    options.addOption(sizeOpt);
    options.addOption(linkDbOpt);

    CommandLineParser parser = new GnuParser();
    try {
      CommandLine line = parser.parse(options, args);
      if (line.hasOption("help") || !line.hasOption("outputDir") || (!line
          .hasOption("segment"))) {
        HelpFormatter formatter = new HelpFormatter();
        formatter
            .printHelp(CommonCrawlDataDumper.class.getName(), options, true);
        return 0;
      }

      File outputDir = new File(line.getOptionValue("outputDir"));
      File segmentRootDir = new File(line.getOptionValue("segment"));
      String[] mimeTypes = line.getOptionValues("mimetype");
      boolean gzip = line.hasOption("gzip");
      boolean epochFilename = line.hasOption("epochFilename");

      String keyPrefix = line.getOptionValue("keyPrefix", "");
      boolean simpleDateFormat = line.hasOption("SimpleDateFormat");
      boolean jsonArray = line.hasOption("jsonArray");
      boolean reverseKey = line.hasOption("reverseKey");
      String extension = line.getOptionValue("extension", "");
      boolean warc = line.hasOption("warc");
      long warcSize = 0;

      if (line.getParsedOptionValue("warcSize") != null) {
        warcSize = (Long) line.getParsedOptionValue("warcSize");
      }
      String linkdbPath = line.getOptionValue("linkdb");
      File linkdb = linkdbPath == null ? null : new File(linkdbPath);

      CommonCrawlConfig config = new CommonCrawlConfig();
      config.setKeyPrefix(keyPrefix);
      config.setSimpleDateFormat(simpleDateFormat);
      config.setJsonArray(jsonArray);
      config.setReverseKey(reverseKey);
      config.setCompressed(gzip);
      config.setWarcSize(warcSize);
      config.setOutputDir(line.getOptionValue("outputDir"));

      if (!outputDir.exists()) {
        LOG.warn("Output directory: [" + outputDir.getAbsolutePath()
            + "]: does not exist, creating it.");
        if (!outputDir.mkdirs())
          throw new Exception(
              "Unable to create: [" + outputDir.getAbsolutePath() + "]");
      }

      CommonCrawlDataDumper dumper = new CommonCrawlDataDumper(config);

      dumper.dump(outputDir, segmentRootDir, linkdb, gzip, mimeTypes, epochFilename,
          extension, warc);

    } catch (Exception e) {
      LOG.error(CommonCrawlDataDumper.class.getName() + ": " + StringUtils
          .stringifyException(e));
      e.printStackTrace();
      return -1;
    }

    return 0;
  }
}
