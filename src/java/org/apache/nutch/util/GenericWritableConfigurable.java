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
    Class clazz = getTypes()[type];
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
