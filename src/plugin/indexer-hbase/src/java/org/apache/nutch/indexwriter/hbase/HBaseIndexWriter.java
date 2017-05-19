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
package org.apache.nutch.indexwriter.hbase;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.nutch.indexer.IndexWriter;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseIndexWriter implements IndexWriter {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  private Configuration config;

  private HBaseMappingReader hbaseMapping;
  private String zkQuorum;
  private int zkClientPort;
  private int batchSize;

  private HBaseAdmin hBaseAdmin;
  private HTable hTable;
  private static List<Put> puts = new ArrayList<Put>();

  @Override
  public Configuration getConf() {
    return config;
  }

  @Override
  public void setConf(Configuration conf) {
    this.config = conf;
  }

  @Override
  public void open(Configuration conf) throws IOException {
    hbaseMapping = HBaseMappingReader.getInstance(conf);
    batchSize = conf.getInt(HBaseConstants.HBASE_COMMIT_SIZE,
        HBaseConstants.DEFAULT_COMMIT_SIZE);
    zkQuorum = config.get(HBaseConstants.ZOOKEEPER_QUORUM,
        HBaseConstants.DEFAULT_ZOOKEEPER_QUORUM);
    zkClientPort = config.getInt(HBaseConstants.ZOOKEEPER_CLIENT_PORT,
        HBaseConstants.DEFAULT_ZOOKEEPER_CLIENT_PORT);

    Configuration hbaseConfig = HBaseConfiguration.create();
    hbaseConfig.set(HConstants.ZOOKEEPER_QUORUM, zkQuorum);
    hbaseConfig.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, zkClientPort);
    try {
      hBaseAdmin = new HBaseAdmin(hbaseConfig);
    } catch (MasterNotRunningException ex) {
      LOG.error(ex.toString());
      throw ex;
    } catch (ZooKeeperConnectionException ex) {
      LOG.error(ex.toString());
      throw ex;
    }

    if (!hBaseAdmin.tableExists(hbaseMapping.getTableName())) {
      String msg = "Table '" + hbaseMapping.getTableName()
          + "' doesn't exist in hbase";
      LOG.error(msg);
      throw new RuntimeException(msg);
    }
    if (hBaseAdmin.isTableDisabled(hbaseMapping.getTableName())) {
      String msg = "Table '" + hbaseMapping.getTableName() + "' is disabled";
      LOG.error(msg);
      throw new RuntimeException(msg);
    }
    hTable = new HTable(hbaseConfig, hbaseMapping.getTableName());
  }

  @Override
  public void write(NutchDocument doc) throws IOException {
    Iterator<Entry<String, List<String>>> entries = doc.iterator();
    byte[] rowKey = Bytes.toBytes(doc.getFieldValue(hbaseMapping.getRowKey()));
    while (entries.hasNext()) {
      Entry<String, List<String>> entry = entries.next();
      LOG.info(entry.getKey() + " => " + entry.getValue());
      Put put = new Put(rowKey);
      put.add(Bytes.toBytes(hbaseMapping.getFamily(entry.getKey())),
          Bytes.toBytes(hbaseMapping.getMappedKey(entry.getKey())),
          Bytes.toBytes(entry.getValue().toString()));
      puts.add(put);
    }
    if (puts.size() >= batchSize) {
      commit();
    }
  }

  @Override
  public void delete(String key) throws IOException {
    Delete delete = new Delete(Bytes.toBytes(key));
    hTable.delete(delete);
  }

  @Override
  public void update(NutchDocument doc) throws IOException {
    write(doc);
  }

  @Override
  public void commit() throws IOException {
    if (!puts.isEmpty()) {
      hTable.put(puts);
      puts.clear();
    }
  }

  @Override
  public void close() throws IOException {
    commit();
    hTable.close();
    hBaseAdmin.close();
  }

  @Override
  public String describe() {
    StringBuffer sb = new StringBuffer("HBaseIndexWriter\n");
    sb.append("\t").append(HBaseConstants.ZOOKEEPER_QUORUM)
        .append(" : Zookeeper quorum (default \"localhost\")\n");
    sb.append("\t").append(HBaseConstants.ZOOKEEPER_CLIENT_PORT)
        .append(" : Zookeeper client port (default 2181)\n");
    sb.append("\t").append(HBaseConstants.COMMIT_SIZE)
        .append(" : buffer size when sending to Hbase (default 250)");
    return sb.toString();
  }

}
