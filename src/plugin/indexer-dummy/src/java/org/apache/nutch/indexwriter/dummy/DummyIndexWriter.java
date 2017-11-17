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
package org.apache.nutch.indexwriter.dummy;

import java.lang.invoke.MethodHandles;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.FileWriter;
import java.io.Writer;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.StringUtils;
import org.apache.nutch.indexer.IndexWriter;
import org.apache.nutch.indexer.IndexWriterParams;
import org.apache.nutch.indexer.IndexerMapReduce;
import org.apache.nutch.indexer.NutchDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DummyIndexWriter. This pluggable indexer writes &lt;action&gt;\t&lt;url&gt;\n lines to a
 * plain text file for debugging purposes. Possible actions are delete, update
 * and add.
 */
public class DummyIndexWriter implements IndexWriter {
  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());
  private Configuration config;
  private Writer writer;
  private boolean delete = false;

  @Override
  public void open(JobConf job, String name) throws IOException {
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
    delete = parameters.getBoolean(DummyConstants.DELETE, false);

    String path = parameters.get(DummyConstants.PATH, "/");
    if (path == null) {
      String message = "Missing path.";
      message += "\n" + describe();
      LOG.error(message);
      throw new RuntimeException(message);
    }

    if (writer != null) {
      LOG.warn("Dummy index file already open for writing");
      return;
    }

    try {
      LOG.debug("Opening dummy index file {}", path);
      writer = new BufferedWriter(new FileWriter(path));
    } catch (IOException ex) {
      LOG.error("Failed to open index file {}: {}", path,
          StringUtils.stringifyException(ex));
    }
  }

  @Override
  public void delete(String key) throws IOException {
    if (delete) {
      writer.write("delete\t" + key + "\n");
    }
  }

  @Override
  public void update(NutchDocument doc) throws IOException {
    writer.write("update\t" + doc.getFieldValue("id") + "\n");
  }

  @Override
  public void write(NutchDocument doc) throws IOException {
    writer.write("add\t" + doc.getFieldValue("id") + "\n");
  }

  public void close() throws IOException {
    LOG.debug("Closing dummy index file");
    writer.flush();
    writer.close();
  }

  @Override
  public void commit() throws IOException {
    writer.write("commit\n");
  }

  @Override
  public Configuration getConf() {
    return config;
  }

  @Override
  public void setConf(Configuration conf) {
    config = conf;
  }

  public String describe() {
    StringBuffer sb = new StringBuffer("DummyIndexWriter\n");
    sb.append("\t").append(
        "path : Path of the file to write to (mandatory)\n");
    sb.append("\t").append(
        "delete : write deletions to dummy index file\n");
    return sb.toString();
  }
}
