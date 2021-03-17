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
package org.apache.nutch.metadata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.nutch.crawl.NutchWritable;

/**
 * This is a simple decorator that adds metadata to any Writable-s that can be
 * serialized by {@link NutchWritable}. This is useful when data needs to be
 * temporarily enriched during processing, but this temporary metadata doesn't
 * need to be permanently stored after the job is done.
 * 
 * @author Andrzej Bialecki
 */
public class MetaWrapper extends NutchWritable {
  private Metadata metadata;

  public MetaWrapper() {
    super();
    metadata = new Metadata();
  }

  public MetaWrapper(Writable instance, Configuration conf) {
    super(instance);
    metadata = new Metadata();
    setConf(conf);
  }

  public MetaWrapper(Metadata metadata, Writable instance, Configuration conf) {
    super(instance);
    if (metadata == null)
      metadata = new Metadata();
    this.metadata = metadata;
    setConf(conf);
  }

  /**
   * Get all metadata.
   * @return a populated {@link Metadata} object
   */
  public Metadata getMetadata() {
    return metadata;
  }

  /**
   * Add metadata.
   * @see Metadata#add(String, String)
   * @param name metadata name to add
   * @param value metadata value to add
   */
  public void addMeta(String name, String value) {
    metadata.add(name, value);
  }

  /**
   * Set metadata.
   * @see Metadata#set(String, String)
   * @param name metadata key to set
   * @param value metadata value to set
   */
  public void setMeta(String name, String value) {
    metadata.set(name, value);
  }

  /**
   * Get metadata value for a given key.
   * @see Metadata#getValues(String)
   * @param name key to retrieve a value for
   * @return metadata value
   */
  public String getMeta(String name) {
    return metadata.get(name);
  }

  /**
   * Get multiple metadata values for a given key.
   * @see Metadata#getValues(String)
   * @param name key to retrieve values for
   * @return a string array containing metadata values
   */
  public String[] getMetaValues(String name) {
    return metadata.getValues(name);
  }

  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    metadata = new Metadata();
    metadata.readFields(in);
  }

  public void write(DataOutput out) throws IOException {
    super.write(out);
    metadata.write(out);
  }
}
