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
package org.commoncrawl.util;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

public abstract class CompressedGenericWritable
    implements Writable, Configurable {

  private static final byte NOT_SET = -1;

  private byte type = NOT_SET;

  private byte[] compressed;

  private Writable instance;

  private Configuration conf = null;

  /**
   * Set the instance that is wrapped.
   *
   * @param obj
   */
  public void set(Writable obj) {
    instance = obj;
    Class<? extends Writable> instanceClazz = instance.getClass();
    Class<? extends Writable>[] clazzes = getTypes();
    for (int i = 0; i < clazzes.length; i++) {
      Class<? extends Writable> clazz = clazzes[i];
      if (clazz.equals(instanceClazz)) {
        type = (byte) i;
        return;
      }
    }
    throw new RuntimeException("The type of instance is: " + instance.getClass()
        + ", which is NOT registered.");
  }

  /**
   * Return the wrapped instance.
   */
  public Writable get() {
    return instance;
  }

  @Override
  public String toString() {
    return "GW["
        + (instance != null
            ? ("class=" + instance.getClass().getName() + ",value="
                + instance.toString())
            : "(null)")
        + "]";
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    type = in.readByte();
    Class<? extends Writable> clazz = getTypes()[type & 0xff];
    try {
      instance = ReflectionUtils.newInstance(clazz, conf);
    } catch (Exception e) {
      e.printStackTrace();
      throw new IOException("Cannot initialize the class: " + clazz);
    }

    int length = in.readInt();
    compressed = new byte[length];
    in.readFully(compressed);

    ByteArrayInputStream deflated = new ByteArrayInputStream(compressed);
    DataInputStream din = new DataInputStream(
        new InflaterInputStream(deflated));

    instance.readFields(din);
    din.close();

    compressed = null;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    if (type == NOT_SET || instance == null)
      throw new IOException(
          "The GenericWritable has NOT been set correctly. type=" + type
              + ", instance=" + instance);
    out.writeByte(type);

    if (compressed == null) {
      ByteArrayOutputStream deflated = new ByteArrayOutputStream();
      Deflater deflater = new Deflater(Deflater.BEST_SPEED);
      DataOutputStream dout = new DataOutputStream(
          new DeflaterOutputStream(deflated, deflater));
      instance.write(dout);
      dout.close();
      deflater.end();
      compressed = deflated.toByteArray();
    }
    out.writeInt(compressed.length);
    out.write(compressed);
  }

  /**
   * Return all classes that may be wrapped. Subclasses should implement this to
   * return a constant array of classes.
   */
  abstract protected Class<? extends Writable>[] getTypes();

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

}
