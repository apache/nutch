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
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

//Commons imports
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.FilenameUtils;

//Hadoop
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

//Nutch imports
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.NutchConfiguration;

//Tika imports
import org.apache.tika.Tika;

public class FileDumper {

    private static final Logger LOG = Logger.getLogger(FileDumper.class
						       .getName());


    public void dump(File outputDir, File segmentRootDir) throws Exception {
        Map<String, Integer> typeCounts = new HashMap<String, Integer>();
        Configuration conf = NutchConfiguration.create();
        FileSystem fs = FileSystem.get(conf);
        int fileCount = 0;
        File[] segmentDirs = segmentRootDir
            .listFiles(new FileFilter() {

                    @Override
                        public boolean accept(File file) {
                        return file.canRead() && file.isDirectory();
                    }
                });

        for (File segment : segmentDirs) {
            LOG.log(Level.INFO,
                    "Processing segment: [" + segment.getAbsolutePath() + "]");
            DataOutputStream doutputStream = null;
            try {
                String segmentPath = segment.getAbsolutePath()
                    + "/" + Content.DIR_NAME + "/part-00000/data";
                Path file = new Path(segmentPath);
                if (!new File(file.toString()).exists()) {
                    LOG.log(Level.WARNING, "Skipping segment: [" + segmentPath
                            + "]: no data directory present");
                    continue;
                }
                SequenceFile.Reader reader = new SequenceFile.Reader(fs, file,
                                                                     conf);

                Writable key = (Writable)reader.getKeyClass().newInstance();
                Content content = null;

                while (reader.next(key)) {
		    content = new Content();
		    reader.getCurrentValue(content);
                    String url = key.toString();
		    String baseName = FilenameUtils.getBaseName(url);
		    String extension = FilenameUtils.getExtension(url);
		    if (extension == null || (extension != null && 
					      extension.equals(""))){
			    extension = "html";
		    }

		    String filename = baseName + "." + extension;

		    ByteArrayInputStream bas = null;
		    try{
			bas = new ByteArrayInputStream(content.getContent());
			String mimeType = new Tika().detect(content.getContent());
                        collectStats(typeCounts, mimeType);
		    }
		    catch(Exception e){
			e.printStackTrace();
			LOG.log(Level.WARNING, "Unable to detect type for: ["+url+"]: Message: "+e.getMessage());
		    }
		    finally{
			if(bas != null){
			    try{
				bas.close();
			    }
			    catch(Exception ignore){}
			    bas = null;
			}
		    }

                    String outputFullPath = outputDir + "/" + filename;
                    File outputFile = new File(outputFullPath);
                    if (!outputFile.exists()) {
                        LOG.log(Level.INFO, "Writing: [" + outputFullPath + "]");
			FileOutputStream output = new FileOutputStream(outputFile);
			IOUtils.write(content.getContent(), output);
                        fileCount++;
                    } else {
                        LOG.log(Level.INFO, "Skipping writing: ["
                                + outputFullPath + "]: file already exists");
                    }
                    content = null;
                }
                reader.close();
            }
             finally {
                fs.close();
                if (doutputStream != null){
		    try{
			doutputStream.close();
		    }
		    catch (Exception ignore){}
		}
            }
        }

        LOG.log(Level.INFO, "Processed: [" + fileCount + "] files.");
        LOG.log(Level.INFO, "File Types: " + displayFileTypes(typeCounts));

    }

    public static void main(String[] args) throws Exception {
	String usage = "Usage: FileDumper <output directory> <segments dir>\n";
	if (args.length != 2) {
	    System.err.println(usage);
	    System.exit(1);
	}

        String outputDir = args[0];
        String segmentRootDir = args[1];
	File outputDirFile = new File(outputDir);
	File segmentRootDirFile = new File(segmentRootDir);

	if (!outputDirFile.exists()) {
	    LOG.log(Level.WARNING, "Output directory: [" + outputDir
		    + "]: does not exist, creating it.");
	    if(!outputDirFile.mkdirs()) throw new Exception("Unable to create: ["+outputDir+"]");
	}

	FileDumper dumper = new FileDumper();
	dumper.dump(outputDirFile, segmentRootDirFile);
    }

    private void collectStats(Map<String, Integer> typeCounts, String mimeType) {
	typeCounts.put(mimeType,
		       typeCounts.containsKey(mimeType) ? typeCounts.get(mimeType) + 1
		       : 1);
    }

    private String displayFileTypes(Map<String, Integer> typeCounts) {
	StringBuilder  builder = new StringBuilder();
	builder.append("{\n");
	for (String mimeType : typeCounts.keySet()) {
	    builder.append("{\"mimeType\":\"");
	    builder.append(mimeType);
	    builder.append("\",\"count\":");
	    builder.append(typeCounts.get(mimeType));
	    builder.append("\"}\n");
	}
	builder.append("}\n");
	return builder.toString();
    }

}
