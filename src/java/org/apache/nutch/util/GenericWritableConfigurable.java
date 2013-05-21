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
package org.apache.nutch.util;

import java.io.DataInput;
import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.Writable;

/** A generic Writable wrapper that can inject Configuration to {@link Configurable}s */ 
public abstract class GenericWritableConfigurable extends GenericWritable 
                                                  implements Configurable {

  private Configuration conf;
  
  public Configuration getConf() {
    return conf;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    byte type = in.readByte();
    Class<?> clazz = getTypes()[type];
    try {
      set((Writable) clazz.newInstance());
    } catch (Exception e) {
      e.printStackTrace();
      throw new IOException("Cannot initialize the class: " + clazz);
    }
    Writable w = get();
    if (w instanceof Configurable)
      ((Configurable)w).setConf(conf);
    w.readFields(in);
  }
  
}
