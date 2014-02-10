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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.FileWriter;
import java.io.Writer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.nutch.indexer.IndexWriter;
import org.apache.nutch.indexer.IndexerMapReduce;
import org.apache.nutch.indexer.NutchDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DummyIndexWriter. This pluggable indexer writes <action>\t<url>\n lines to a
 * plain text file for debugging purposes. Possible actions are delete, update
 * and add.
 */
public class DummyIndexWriter implements IndexWriter {
  public static final Logger LOG = LoggerFactory.getLogger(DummyIndexWriter.class);
  private Configuration config;
  private Writer writer;
  private boolean delete = false;

  public void open(JobConf job, String name) throws IOException {  
    delete = job.getBoolean(IndexerMapReduce.INDEXER_DELETE, false);
  }

  @Override
  public void delete(String key) throws IOException {
    if (delete) {
      writer.write("delete\t" + key + "\n");
    }
  }

  @Override
  public void update(NutchDocument doc) throws IOException {
    writer.write("update\t" + doc.getFieldValue("url") + "\n");
  }

  @Override
  public void write(NutchDocument doc) throws IOException {
    writer.write("add\t" + doc.getFieldValue("url") + "\n");
  }

  public void close() throws IOException {
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
    String path = conf.get("dummy.path");
    if (path == null) {
      String message = "Missing path. Should be set via -Ddummy.path";
      message+="\n"+describe();
      LOG.error(message);
      throw new RuntimeException(message);
    }

    try {
      writer = new BufferedWriter(new FileWriter(conf.get("dummy.path")));
    } catch (IOException e) {}
  }

  public String describe(){
    StringBuffer sb = new StringBuffer("DummyIndexWriter\n");
    sb.append("\t").append("dummy.path : Path of the file to write to (mandatory)\n");
    return sb.toString();
  }
}
