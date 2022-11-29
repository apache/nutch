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
package earth.elio.nutch.indexwriter.json;

import java.lang.invoke.MethodHandles;
import java.io.IOException;
import java.net.Inet4Address;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.indexer.*;
import org.apache.nutch.util.NutchConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import net.minidev.json.JSONObject;

/**
 * JsonIndexWriter. This pluggable indexer writes the fields configured to a file in json-lines
 * format (one json object per line) separated by newlines. It takes the configured base output path
 * and writes the files to a unique location so that it can be run in parallel.
 */
public class JsonIndexWriter implements IndexWriter {

    private static final Logger LOG = LoggerFactory
            .getLogger(MethodHandles.lookup().lookupClass());
    private static final String ENCODING = "UTF-8";
    private static final DateFormat INSERTION_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");

    private Configuration config;
    /** list of fields in the JSON file */
    private String[] fields = new String[] {"id", "title", "content"};

    /** output path / directory */
    private String baseOutputPath = "";
    /** Output stream for json data. */
    protected FSDataOutputStream jsonOut;

    @Override
    public void open(Configuration conf, String name) throws IOException {
        //Implementation not required
    }

    /**
     * Initializes the internal variables from a given index writer configuration.
     *
     * @param parameters Params from the index writer configuration.
     * @throws IOException Some exception thrown by writer.
     */
    @Override
    public void open(IndexWriterParams parameters) throws IOException {
        fields = parameters.getStrings(JsonConstants.JSON_FIELDS, fields);
        LOG.info("fields =");
        for (String f : fields) {
            LOG.info("\t" + f);
        }

        baseOutputPath = parameters.get(JsonConstants.JSON_BASE_OUTPUT_PATH);
        if(StringUtils.isBlank(baseOutputPath)) {
            throw new IOException("Base output path is missing");
        } else {
            LOG.info("Base Output path is {}", baseOutputPath);
        }
        // standardize the base output path to NOT have a trailing slash
        if (baseOutputPath.endsWith("/")) {
            baseOutputPath = baseOutputPath.substring(0, baseOutputPath.length() - 1);
        }

        String outputPath = String.format("%s/insert=%s/", baseOutputPath, INSERTION_DATE_FORMAT.format(new Date()));

        Path outputDir = new Path(outputPath);
        FileSystem fs = outputDir.getFileSystem(config);
        if (!fs.exists(outputDir)) {
            fs.mkdirs(outputDir);
        }

        String hostName = Inet4Address.getLocalHost().getHostAddress();
        // default to local if we can't find the hostname
        if (StringUtils.isBlank(hostName)) {
            hostName = "local";
        }

        String filename = String.format("%s-%s-%s.jsonl", hostName, Long.valueOf(System.currentTimeMillis()).toString(), UUID.randomUUID());
        Path jsonLocalOutFile = new Path(outputDir, filename);
        if (fs.exists(jsonLocalOutFile)) {
            // clean-up
            LOG.warn("Removing existing output path {}", jsonLocalOutFile);
            fs.delete(jsonLocalOutFile, true);
        }
        jsonOut = fs.create(jsonLocalOutFile);
    }

    @Override
    public void delete(String key) throws IOException {
        // deletion of documents not supported
        // maybe we search through the file and delete?
    }

    @Override
    public void update(NutchDocument doc) throws IOException {
        write(doc);
    }

    @Override
    public void write(NutchDocument doc) throws IOException {
        JSONObject obj = new JSONObject();
        for (int i = 0; i < fields.length; i++) {
            NutchField field = doc.getField(fields[i]);
            if (field != null) {
                List<Object> values = field.getValues();
                if (values.size() == 1) {
                    obj.put(fields[i], values.get(0));
                } else {
                    obj.put(fields[i], values);
                }
            }
        }

        jsonOut.write(obj.toString().getBytes(ENCODING));
        jsonOut.write("\r\n".getBytes(ENCODING));
    }

    @Override
    public void close() throws IOException {
        jsonOut.close();
    }

    @Override
    public void commit() throws IOException {
        // nothing to do
    }

    @Override
    public Configuration getConf() {
        return config;
    }

    @Override
    public void setConf(Configuration conf) {
        config = conf;
    }

    /**
     * Returns {@link Map} with the specific parameters the IndexWriter instance can take.
     *
     * @return The values of each row. It must have the form &#60;KEY,&#60;DESCRIPTION,VALUE&#62;&#62;.
     */
    @Override
    public Map<String, Map.Entry<String, Object>> describe() {
        Map<String, Map.Entry<String, Object>> properties = new LinkedHashMap<>();

        properties.put(JsonConstants.JSON_FIELDS, new AbstractMap.SimpleEntry<>(
                "List of fields (columns) in the JSON file",
                this.fields == null ? "" : String.join(",", this.fields)));

        properties.put(JsonConstants.JSON_BASE_OUTPUT_PATH, new AbstractMap.SimpleEntry<>(
                "The base output path for the data. Must be specified. We add partition information to the end.",
                this.baseOutputPath));

        return properties;
    }

    public static void main(String[] args) throws Exception {
        final int res = ToolRunner.run(NutchConfiguration.create(),
                new IndexingJob(), args);
        System.exit(res);
    }
}
