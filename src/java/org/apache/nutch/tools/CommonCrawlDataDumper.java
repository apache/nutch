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
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
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
import com.fasterxml.jackson.dataformat.cbor.CBORFactory;
import com.fasterxml.jackson.dataformat.cbor.CBORGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * 
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
 * 
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
 * }
 * </pre>
 * 
 */
public class CommonCrawlDataDumper {

	private static final Logger LOG = LoggerFactory.getLogger(CommonCrawlDataDumper.class.getName());

	/**
	 * Main method for invoking this tool
	 * 
	 * @param args
	 *            1) output directory (which will be created if it does not
	 *            already exist) to host the CBOR data and 2) a directory
	 *            containing one or more segments from which we wish to generate
	 *            CBOR data from. Optionally, 3) a list of mimetypes and the 4) 
	 *            the gzip option may be provided.
	 * @throws Exception
	 */
	@SuppressWarnings("static-access")
	public static void main(String[] args) throws Exception {
		Option helpOpt = new Option("h", "help", false,
				"show this help message");
		// argument options
		Option outputOpt = OptionBuilder
				.withArgName("outputDir")
				.hasArg()
				.withDescription(
						"output directory (which will be created) to host the CBOR data")
				.create("outputDir");
		Option segOpt = OptionBuilder.withArgName("segment").hasArgs()
				.withDescription("the segment(s) to use").create("segment");
		// GIUSEPPE: create mimetype and gzip options
		Option mimeOpt = OptionBuilder
				.isRequired(false)
				.withArgName("mimetype")
				.hasArgs()
				.withDescription(
						"an optional list of mimetypes to dump, excluding all others. Defaults to all.")
				.create("mimetype");
		Option gzipOpt = OptionBuilder
				.isRequired(false)
				.hasArg(false)
				.withDescription(
						"an optional flag indicating whether to additionally gzip the data")
				.create("gzip");

		// create the options
		Options options = new Options();
		options.addOption(helpOpt);
		options.addOption(outputOpt);
		options.addOption(segOpt);
		// create mimetypes and gzip options
		options.addOption(mimeOpt);
		options.addOption(gzipOpt);

		CommandLineParser parser = new GnuParser();
		try {
			CommandLine line = parser.parse(options, args);
			if (line.hasOption("help") || !line.hasOption("outputDir") || (!line.hasOption("segment"))) {
				HelpFormatter formatter = new HelpFormatter();
				formatter.printHelp(CommonCrawlDataDumper.class.getName(), options, true);
				return;
			}

			File outputDir = new File(line.getOptionValue("outputDir"));
			File segmentRootDir = new File(line.getOptionValue("segment"));
			String[] mimeTypes = line.getOptionValues("mimetype");
			boolean gzip = line.hasOption("gzip");

			if (!outputDir.exists()) {
				LOG.warn("Output directory: [" + outputDir.getAbsolutePath() + "]: does not exist, creating it.");
				if (!outputDir.mkdirs())
					throw new Exception("Unable to create: [" + outputDir.getAbsolutePath() + "]");
			}

			CommonCrawlDataDumper dumper = new CommonCrawlDataDumper();
			
			dumper.dump(outputDir, segmentRootDir, gzip, mimeTypes);
			
		} catch (Exception e) {
			LOG.error(CommonCrawlDataDumper.class.getName() + ": " + StringUtils.stringifyException(e));
			e.printStackTrace();
			return;
		}
	}
	
	/**
	 * Dumps the reverse engineered CBOR content from the provided segment
	 * directories if a parent directory contains more than one segment,
	 * otherwise a single segment can be passed as an argument. If the boolean
	 * argument is provided then the CBOR is also zipped.
	 * 
	 * @param outputDir
	 *            the directory you wish to dump the raw content to. This
	 *            directory will be created.
	 * @param segmentRootDir
	 *            a directory containing one or more segments.
	 * @param gzip
	 *            a boolean flag indicating whether the CBOR content should also
	 *            be gzipped.
	 * @param mimetypes
	 *            an array of mime types we have to dump, all others will be
     *            filtered out.
	 * @throws Exception
	 */
	public void dump(File outputDir, File segmentRootDir, boolean gzip,	String[] mimeTypes) throws Exception {
		if (!gzip) {
			LOG.info("Gzipping CBOR data has been skipped");
		}
		// total file counts
		Map<String, Integer> typeCounts = new HashMap<String, Integer>();
		// filtered file counters
		Map<String, Integer> filteredCounts = new HashMap<String, Integer>();
		
		Configuration conf = NutchConfiguration.create();
		FileSystem fs = FileSystem.get(conf);
		File[] segmentDirs = segmentRootDir.listFiles(new FileFilter() {
			@Override
			public boolean accept(File file) {
				return file.canRead() && file.isDirectory();
			}
		});
		
		if (segmentDirs == null) {
			LOG.error("No segment directories found in [" + segmentRootDir.getAbsolutePath() + "]");
			System.exit(1);
		}
		
		// Gzip initialization
		FileOutputStream fileOutput = null;
	    BufferedOutputStream bufOutput = null;
	    GzipCompressorOutputStream gzipOutput = null;
	    TarArchiveOutputStream tarOutput = null;
	    
	    ArrayList<String> fileList = null;
	    
		if (gzip) {
			String archiveName = new SimpleDateFormat("yyyyMMddhhmm'.tar.gz'").format(new Date());
		    fileOutput = new FileOutputStream(new File(outputDir + File.separator + archiveName));
		    bufOutput = new BufferedOutputStream(fileOutput);
		    gzipOutput = new GzipCompressorOutputStream(bufOutput);
		    tarOutput = new TarArchiveOutputStream(gzipOutput);
		    
		    fileList = new ArrayList<String>();
		}

		for (File segment : segmentDirs) {
			LOG.info("Processing segment: [" + segment.getAbsolutePath() + "]");
			// GIUSEPPE: Never used (also in FileDumper.java)!
			//DataOutputStream doutputStream = null;
			try {
				String segmentContentPath = segment.getAbsolutePath() + File.separator + Content.DIR_NAME + "/part-00000/data";
				Path file = new Path(segmentContentPath);

				if (!new File(file.toString()).exists()) {
					LOG.warn("Skipping segment: [" + segmentContentPath	+ "]: no data directory present");
					continue;
				}
				SequenceFile.Reader reader = new SequenceFile.Reader(fs, file, conf);

				if (!new File(file.toString()).exists()) {
					LOG.warn("Skipping segment: [" + segmentContentPath	+ "]: no data directory present");
					continue;
				}
				Writable key = (Writable) reader.getKeyClass().newInstance();
				
				Content content = null;

				while (reader.next(key)) {
					content = new Content();
					reader.getCurrentValue(content);
					String url = key.toString();
					String baseName = FilenameUtils.getBaseName(url);
					String extension = FilenameUtils.getExtension(url);
					if (extension == null || extension.equals("")) {
						extension = "html";
					}

					String filename = baseName + "." + extension;
					
					// Encode all filetypes if no mimetypes have been given
					Boolean filter = (mimeTypes == null);
					
					String jsonData = "";
					try {
						String mimeType = new Tika().detect(content.getContent());
						// Maps file to JSON-based structure
						CommonCrawlFormat format = CommonCrawlFormatFactory.getCommonCrawlFormat("JACKSON", url, content.getContent(), content.getMetadata(), conf);
						jsonData = format.getJsonData(false);

						collectStats(typeCounts, mimeType);
						// collects statistics for the given mimetypes
						if ((mimeType != null) && (mimeTypes != null) && Arrays.asList(mimeTypes).contains(mimeType)) {
							collectStats(filteredCounts, mimeType);
							filter = true;
						}
					} catch (Exception e) {
						e.printStackTrace();
						LOG.warn("Tika is unable to detect type for: [" + url
								+ "]");
					}

					if (filter) {
						
						byte[] byteData = serializeCBORData(jsonData);
						
						if (!gzip) {
							String outputFullPath = outputDir + File.separator + filename;
							File outputFile = new File(outputFullPath);
							if (outputFile.exists()) {
								LOG.info("Skipping writing: [" + outputFullPath	+ "]: file already exists");
							}
							else {
								LOG.info("Writing: [" + outputFullPath + "]");
								IOUtils.copy(new ByteArrayInputStream(byteData), new FileOutputStream(outputFile));
							}
						}
						else {
							if (fileList.contains(filename)) {
								LOG.info("Skipping compressing: [" + filename	+ "]: file already exists");
							}
							else {
								fileList.add(filename);
								LOG.info("Compressing: [" + filename + "]");
								TarArchiveEntry tarEntry = new TarArchiveEntry(filename);
								tarEntry.setSize(byteData.length);
								tarOutput.putArchiveEntry(tarEntry);
								IOUtils.copy(new ByteArrayInputStream(byteData), tarOutput);
								tarOutput.closeArchiveEntry();
							}
						}
					}
				}
				reader.close();
			} finally {
				fs.close();
			}
		}
		
		if (gzip) {
			tarOutput.finish();
			 
	        tarOutput.close();
	        gzipOutput.close();
	        bufOutput.close();
	        fileOutput.close();
		}
		
		LOG.info("CommonsCrawlDataDumper File Stats: " + displayFileTypes(typeCounts, filteredCounts));
	}
	
	private byte[] serializeCBORData(String jsonData) {
		CBORFactory factory = new CBORFactory();
		
		CBORGenerator generator = null;
		ByteArrayOutputStream stream = null;
		
		try {
			stream = new ByteArrayOutputStream();
			generator = factory.createGenerator(stream);
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
		typeCounts.put(mimeType, typeCounts.containsKey(mimeType) ? typeCounts.get(mimeType) + 1 : 1);
	}

	private String displayFileTypes(Map<String, Integer> typeCounts, Map<String, Integer> filteredCounts) {
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
		// filtered types stats
		if (!filteredCounts.isEmpty()) {
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
