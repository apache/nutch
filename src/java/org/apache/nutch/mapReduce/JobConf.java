/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nutch.mapReduce;

import java.io.IOException;
import java.io.File;
import java.io.InputStream;
import java.io.FileNotFoundException;
import java.net.URL;

import java.util.Properties;
import java.util.jar.JarFile;
import java.util.jar.JarEntry;
import java.util.List;

import org.apache.nutch.fs.NutchFileSystem;
import org.apache.nutch.util.NutchConf;

import org.apache.nutch.io.Writable;
import org.apache.nutch.io.WritableComparable;
import org.apache.nutch.io.WritableComparator;
import org.apache.nutch.io.LongWritable;
import org.apache.nutch.io.UTF8;

import org.apache.nutch.mapReduce.lib.IdentityMapper;
import org.apache.nutch.mapReduce.lib.IdentityReducer;
import org.apache.nutch.mapReduce.lib.HashPartitioner;

/** A map/reduce job configuration.  This names the {@link Mapper}, combiner
 * (if any), {@link Partitioner}, {@link Reducer}, {@link InputFormat}, and
 * {@link OutputFormat} implementations to be used.  It also indicates the set
 * of input files, and where the output files should be written. */
public class JobConf extends NutchConf {

  /** Construct a map/reduce configuration. */
  public JobConf() {
    super();
  }

  /** Construct a map/reduce configuration.
   *
   * @param config a NutchConf-format XML job description file
   */
  public JobConf(String config) {
    this(new File(config));
  }

  /** Construct a map/reduce configuration.
   *
   * @param config a NutchConf-format XML job description file
   */
  public JobConf(File config) {
    super();
    addConfResource(config);
  }

  public String getJar() {
    String defaultValue = "nutch.jar";

    URL jarUrl = get().getResource(defaultValue); // resolve default
    if (jarUrl != null && "file".equals(jarUrl.getProtocol()))
      defaultValue = jarUrl.getFile();
      
    return get("mapred.jar", defaultValue);
  }
  public void setJar(String jar) { set("mapred.jar", jar); }

  public static File getSystemDir() {
    return new File(NutchConf.get().get("mapred.system.dir",
                                        "/tmp/nutch/mapred/system"));
  }

  public static File getLocalDir() {
    return new File(NutchConf.get().get("mapred.local.dir",
                                        "/tmp/nutch/mapred/local"));
  }

  public File getInputDir() { return new File(get("mapred.input.dir")); }
  public void setInputDir(File dir) { set("mapred.input.dir", dir); }

  public File getOutputDir() { return new File(get("mapred.output.dir")); }
  public void setOutputDir(File dir) { set("mapred.output.dir", dir); }

  public InputFormat getInputFormat() {
    return InputFormats.get(get("mapred.input.format", "text"));
  }
  public void setInputFormat(InputFormat format) {
    set("mapred.input.format", format.getName());
  }
  public OutputFormat getOutputFormat() {
    return OutputFormats.get(get("mapred.output.format", "text"));
  }
  public void setOutputFormat(OutputFormat format) {
    set("mapred.output.format", format.getName());
  }
  
  public Class getInputKeyClass() {
    return getClass("mapred.input.key.class",
                    LongWritable.class, WritableComparable.class);
  }
  public void setInputKeyClass(Class theClass) {
    setClass("mapred.input.key.class", theClass, WritableComparable.class);
  }

  public Class getInputValueClass() {
    return getClass("mapred.input.value.class", UTF8.class, Writable.class);
  }
  public void setInputValueClass(Class theClass) {
    setClass("mapred.input.value.class", theClass, WritableComparable.class);
  }

  public Class getOutputKeyClass() {
    return getClass("mapred.output.key.class",
                    LongWritable.class, WritableComparable.class);
  }
  public void setOutputKeyClass(Class theClass) {
    setClass("mapred.output.key.class", theClass, WritableComparable.class);
  }

  public Class getOutputKeyComparatorClass() {
    return getClass("mapred.output.key.comparator.class",
                    WritableComparator.get(getOutputKeyClass()).getClass(),
                    WritableComparator.class);
  }

  public void setOutputKeyComparatorClass(Class theClass) {
    setClass("mapred.output.key.comparator.class",
             theClass, WritableComparator.class);
  }

  public Class getOutputValueClass() {
    return getClass("mapred.output.value.class", UTF8.class, Writable.class);
  }
  public void setOutputValueClass(Class theClass) {
    setClass("mapred.output.value.class", theClass, WritableComparable.class);
  }

  public Class getMapperClass() {
    return getClass("mapred.mapper.class", IdentityMapper.class, Mapper.class);
  }
  public void setMapperClass(Class theClass) {
    setClass("mapred.mapper.class", theClass, Mapper.class);
  }

  public Class getPartitionerClass() {
    return getClass("mapred.partitioner.class",
                    HashPartitioner.class, Partitioner.class);
  }
  public void setPartitionerClass(Class theClass) {
    setClass("mapred.partitioner.class", theClass, Partitioner.class);
  }

  public Class getReducerClass() {
    return getClass("mapred.reducer.class",
                    IdentityReducer.class, Reducer.class);
  }
  public void setReducerClass(Class theClass) {
    setClass("mapred.reducer.class", theClass, Reducer.class);
  }

  public Class getCombinerClass() {
    return getClass("mapred.combiner.class", null, Reducer.class);
  }
  public void setCombinerClass(Class theClass) {
    setClass("mapred.combiner.class", theClass, Reducer.class);
  }
  
  public int getNumMapTasks() { return getInt("mapred.map.tasks", 10); }
  public void setNumMapTasks(int n) { setInt("mapred.map.tasks", n); }

  public int getNumReduceTasks() { return getInt("mapred.reduce.tasks", 10); }
  public void setNumReduceTasks(int n) { setInt("mapred.reduce.tasks", n); }

  public Configurable newInstance(Class theClass) {
    Configurable result;
    try {
      result = (Configurable)theClass.newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    result.configure(this);
    return result;
  }

  public static void main(String[] args) {
    System.out.println(new JobConf().getJar());
  }
}

