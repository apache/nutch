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

package org.apache.nutch.tools;

//JDK imports
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileOutputStream;
import java.io.ByteArrayInputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
//Commons imports
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.FilenameUtils;

//Hadoop
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.NutchConfiguration;

//Tika imports
import org.apache.tika.Tika;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * The file dumper tool enables one to reverse generate the raw content from
 * Nutch segment data directories.
 * </p>
 * <p>
 * The tool has a number of immediate uses:
 * <ol>
 * <li>one can see what a page looked like at the time it was crawled</li>
 * <li>one can see different media types acquired as part of the crawl</li>
 * <li>it enables us to see webpages before we augment them with additional
 * metadata, this can be handy for providing a provenance trail for your crawl
 * data.</li>
 * </ol>
 * </p>
 * <p>
 * Upon successful completion the tool displays a very convenient JSON snippet
 * detailing the mimetype classifications and the counts of documents which fall
 * into those classifications. An example is as follows:
 * </p>
 * 
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
 *   FILTER Stats:    {
 *     {"mimeType":"image/png","count":47"}
 *     {"mimeType":"image/jpeg","count":141"}
 *     {"mimeType":"image/vnd.microsoft.icon","count":4"}
 *     {"mimeType":"video/quicktime","count":2"}
 *     {"mimeType":"image/gif","count":63"}
 *   }
 * }
 * </pre>
 * <p>
 * In the case above the tool would have been run with the <b>-mimeType
 * image/png image/jpeg image/vnd.microsoft.icon video/quicktime image/gif</b>
 * flag and corresponding values activated.
 * 
 */
public class FileDumper {

  private static final Logger LOG = LoggerFactory.getLogger(FileDumper.class
      .getName());

  /**
   * Dumps the reverse engineered raw content from the provided segment
   * directories if a parent directory contains more than one segment, otherwise
   * a single segment can be passed as an argument.
   * 
   * @param outputDir
   *          the directory you wish to dump the raw content to. This directory
   *          will be created.
   * @param segmentRootDir
   *          a directory containing one or more segments.
   * @param mimeTypes
   *          an array of mime types we have to dump, all others will be
   *          filtered out.
   * @throws Exception
   */
  public void dump(File outputDir, File segmentRootDir, String[] mimeTypes)
      throws Exception {
    if (mimeTypes == null)
      LOG.info("Accepting all mimetypes.");
    // total file counts
    Map<String, Integer> typeCounts = new HashMap<String, Integer>();
    // filtered file counts
    Map<String, Integer> filteredCounts = new HashMap<String, Integer>();
    Configuration conf = NutchConfiguration.create();
    FileSystem fs = FileSystem.get(conf);
    int fileCount = 0;
    File[] segmentDirs = segmentRootDir.listFiles(new FileFilter() {

      @Override
      public boolean accept(File file) {
        return file.canRead() && file.isDirectory();
      }
    });
    if (segmentDirs == null) {
      System.err.println("No segment directories found in ["
          + segmentRootDir.getAbsolutePath() + "]");
      return;
    }

    for (File segment : segmentDirs) {
      LOG.info("Processing segment: [" + segment.getAbsolutePath() + "]");
      DataOutputStream doutputStream = null;
      try {
        String segmentPath = segment.getAbsolutePath() + "/" + Content.DIR_NAME
            + "/part-00000/data";
        Path file = new Path(segmentPath);
        if (!new File(file.toString()).exists()) {
          LOG.warn("Skipping segment: [" + segmentPath
              + "]: no data directory present");
          continue;
        }
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, file, conf);

        Writable key = (Writable) reader.getKeyClass().newInstance();
        Content content = null;

        while (reader.next(key)) {
          content = new Content();
          reader.getCurrentValue(content);
          String url = key.toString();
          String baseName = FilenameUtils.getBaseName(url);
          String extension = FilenameUtils.getExtension(url);
          if (extension == null || (extension != null && extension.equals(""))) {
            extension = "html";
          }

          String filename = baseName + "." + extension;
          ByteArrayInputStream bas = null;
          Boolean filter = false;
          try {
            bas = new ByteArrayInputStream(content.getContent());
            String mimeType = new Tika().detect(content.getContent());
            collectStats(typeCounts, mimeType);
            if (mimeType != null) {
              if (mimeTypes == null
                  || Arrays.asList(mimeTypes).contains(mimeType)) {
                collectStats(filteredCounts, mimeType);
                filter = true;
              }
            }
          } catch (Exception e) {
            e.printStackTrace();
            LOG.warn("Tika is unable to detect type for: [" + url + "]");
          } finally {
            if (bas != null) {
              try {
                bas.close();
              } catch (Exception ignore) {
              }
            }
          }

          if (filter) {
            String outputFullPath = outputDir + "/" + filename;
            File outputFile = new File(outputFullPath);
            if (!outputFile.exists()) {
              LOG.info("Writing: [" + outputFullPath + "]");
              try {
                FileOutputStream output = new FileOutputStream(outputFile);
                IOUtils.write(content.getContent(), output);
                fileCount++;
                  
              } catch (Exception e) {
                outputFullPath = outputDir + "/" + System.currentTimeMillis();
                File newOutPutFile = new File(outputFullPath);
                FileOutputStream output = new FileOutputStream(newOutPutFile);
                IOUtils.write(content.getContent(), output);
                fileCount++;
                LOG.info("File name is too long. Renamed to system time.");
                
                //e.printStackTrace();
              }
              
            } else {
              LOG.info("Skipping writing: [" + outputFullPath
                  + "]: file already exists");
            }
          }
        }
        reader.close();
      } finally {
        fs.close();
        if (doutputStream != null) {
          try {
            doutputStream.close();
          } catch (Exception ignore) {
          }
        }
      }
    }
    LOG.info("Dumper File Stats: "
        + displayFileTypes(typeCounts, filteredCounts));

  }

  /**
   * Main method for invoking this tool
   * 
   * @param args
   *          1) output directory (which will be created) to host the raw data
   *          and 2) a directory containing one or more segments.
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    // boolean options
    Option helpOpt = new Option("h", "help", false, "show this help message");
    // argument options
    @SuppressWarnings("static-access")
    Option outputOpt = OptionBuilder
        .withArgName("outputDir")
        .hasArg()
        .withDescription(
            "output directory (which will be created) to host the raw data")
        .create("outputDir");
    @SuppressWarnings("static-access")
    Option segOpt = OptionBuilder.withArgName("segment").hasArgs()
        .withDescription("the segment(s) to use").create("segment");
    @SuppressWarnings("static-access")
    Option mimeOpt = OptionBuilder
        .withArgName("mimetype")
        .hasArgs()
        .withDescription(
            "an optional list of mimetypes to dump, excluding all others. Defaults to all.")
        .create("mimetype");

    // create the options
    Options options = new Options();
    options.addOption(helpOpt);
    options.addOption(outputOpt);
    options.addOption(segOpt);
    options.addOption(mimeOpt);

    CommandLineParser parser = new GnuParser();
    try {
      CommandLine line = parser.parse(options, args);
      if (line.hasOption("help") || !line.hasOption("outputDir")
          || (!line.hasOption("segment"))) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("FileDumper", options, true);
        return;
      }

      File outputDir = new File(line.getOptionValue("outputDir"));
      File segmentRootDir = new File(line.getOptionValue("segment"));
      String[] mimeTypes = line.getOptionValues("mimetype");

      if (!outputDir.exists()) {
        LOG.warn("Output directory: [" + outputDir.getAbsolutePath()
            + "]: does not exist, creating it.");
        if (!outputDir.mkdirs())
          throw new Exception("Unable to create: ["
              + outputDir.getAbsolutePath() + "]");
      }

      FileDumper dumper = new FileDumper();
      dumper.dump(outputDir, segmentRootDir, mimeTypes);
    } catch (Exception e) {
      LOG.error("FileDumper: " + StringUtils.stringifyException(e));
      e.printStackTrace();
      return;
    }
  }

  private void collectStats(Map<String, Integer> typeCounts, String mimeType) {
    typeCounts.put(mimeType,
        typeCounts.containsKey(mimeType) ? typeCounts.get(mimeType) + 1 : 1);
  }

  private String displayFileTypes(Map<String, Integer> typeCounts,
      Map<String, Integer> filteredCounts) {
    StringBuilder builder = new StringBuilder();
    // print total stats
    builder.append("\n  TOTAL Stats:\n");
    builder.append("                {\n");
    for (String mimeType : typeCounts.keySet()) {
      builder.append("    {\"mimeType\":\"");
      builder.append(mimeType);
      builder.append("\",\"count\":");
      builder.append(typeCounts.get(mimeType));
      builder.append("\"}\n");
    }
    builder.append("}\n");
    if (!filteredCounts.isEmpty()) {
      // print dumper stats
      builder.append("\n  FILTERED Stats:\n");
      builder.append("                {\n");
      for (String mimeType : filteredCounts.keySet()) {
        builder.append("    {\"mimeType\":\"");
        builder.append(mimeType);
        builder.append("\",\"count\":");
        builder.append(filteredCounts.get(mimeType));
        builder.append("\"}\n");
      }
      builder.append("}\n");
    }
    return builder.toString();
  }

}
