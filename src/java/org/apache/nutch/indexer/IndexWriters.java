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
package org.apache.nutch.indexer;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.plugin.Extension;
import org.apache.nutch.plugin.ExtensionPoint;
import org.apache.nutch.plugin.PluginRepository;
import org.apache.nutch.plugin.PluginRuntimeException;
import org.apache.nutch.util.ObjectCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Creates and caches {@link IndexWriter} implementing plugins. */
public class IndexWriters {

  public final static Logger LOG = LoggerFactory.getLogger(IndexWriters.class);

  private IndexWriter[] indexWriters;

  public IndexWriters(Configuration conf) {
    ObjectCache objectCache = ObjectCache.get(conf);
    synchronized (objectCache) {
      this.indexWriters = (IndexWriter[]) objectCache
          .getObject(IndexWriter.class.getName());
      if (this.indexWriters == null) {
        try {
          ExtensionPoint point = PluginRepository.get(conf).getExtensionPoint(
              IndexWriter.X_POINT_ID);
          if (point == null)
            throw new RuntimeException(IndexWriter.X_POINT_ID + " not found.");
          Extension[] extensions = point.getExtensions();
          HashMap<String, IndexWriter> indexerMap = new HashMap<String, IndexWriter>();
          for (int i = 0; i < extensions.length; i++) {
            Extension extension = extensions[i];
            IndexWriter writer = (IndexWriter) extension.getExtensionInstance();
            LOG.info("Adding " + writer.getClass().getName());
            if (!indexerMap.containsKey(writer.getClass().getName())) {
              indexerMap.put(writer.getClass().getName(), writer);
            }
          }
          objectCache.setObject(IndexWriter.class.getName(), indexerMap
              .values().toArray(new IndexWriter[0]));
        } catch (PluginRuntimeException e) {
          throw new RuntimeException(e);
        }
        this.indexWriters = (IndexWriter[]) objectCache
            .getObject(IndexWriter.class.getName());
      }
    }
  }

  public void open(JobConf job, String name) throws IOException {
    for (int i = 0; i < this.indexWriters.length; i++) {
      try {
        this.indexWriters[i].open(job, name);
      } catch (IOException ioe) {
        throw ioe;
      }
    }
  }

  public void write(NutchDocument doc) throws IOException {
    for (int i = 0; i < this.indexWriters.length; i++) {
      try {
        this.indexWriters[i].write(doc);
      } catch (IOException ioe) {
        throw ioe;
      }
    }
  }

  public void update(NutchDocument doc) throws IOException {
    for (int i = 0; i < this.indexWriters.length; i++) {
      try {
        this.indexWriters[i].update(doc);
      } catch (IOException ioe) {
        throw ioe;
      }
    }
  }

  public void delete(String key) throws IOException {
    for (int i = 0; i < this.indexWriters.length; i++) {
      try {
        this.indexWriters[i].delete(key);
      } catch (IOException ioe) {
        throw ioe;
      }
    }
  }

  public void close() throws IOException {
    for (int i = 0; i < this.indexWriters.length; i++) {
      try {
        this.indexWriters[i].close();
      } catch (IOException ioe) {
        throw ioe;
      }
    }
  }

  public void commit() throws IOException {
    for (int i = 0; i < this.indexWriters.length; i++) {
      try {
        this.indexWriters[i].commit();
      } catch (IOException ioe) {
        throw ioe;
      }
    }
  }

  // lists the active IndexWriters and their configuration
  public String describe() throws IOException {
    StringBuffer buffer = new StringBuffer();
    if (this.indexWriters.length == 0)
      buffer.append("No IndexWriters activated - check your configuration\n");
    else
      buffer.append("Active IndexWriters :\n");
    for (int i = 0; i < this.indexWriters.length; i++) {
      buffer.append(this.indexWriters[i].describe()).append("\n");
    }
    return buffer.toString();
  }

}
