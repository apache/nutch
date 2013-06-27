/*******************************************************************************
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
 ******************************************************************************/
package org.apache.nutch.storage;

import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.util.Utf8;
import org.apache.gora.persistency.StateManager;
import org.apache.gora.persistency.impl.PersistentBase;
import org.apache.gora.persistency.impl.StateManagerImpl;
import org.apache.gora.persistency.StatefulHashMap;
import org.apache.nutch.util.Bytes;

@SuppressWarnings("all")
public class Host extends PersistentBase {
  public static final org.apache.avro.Schema _SCHEMA = org.apache.avro.Schema.parse("{\"type\":\"record\",\"name\":\"Host\",\"namespace\":\"org.apache.nutch.storage\",\"fields\":[{\"name\":\"metadata\",\"type\":{\"type\":\"map\",\"values\":\"bytes\"}},{\"name\":\"outlinks\",\"type\":{\"type\":\"map\",\"values\":\"string\"}},{\"name\":\"inlinks\",\"type\":{\"type\":\"map\",\"values\":\"string\"}}]}");
  public java.util.Map<org.apache.avro.util.Utf8,java.nio.ByteBuffer> metadata;
  public java.util.Map<org.apache.avro.util.Utf8,org.apache.avro.util.Utf8> outlinks;
  public java.util.Map<org.apache.avro.util.Utf8,org.apache.avro.util.Utf8> inlinks;
  
  public static enum Field {
    METADATA(0,"metadata"),
    OUTLINKS(1,"outlinks"),
    INLINKS(2,"inlinks"),
    ;
    private int index;
    private String name;
    Field(int index, String name) {this.index=index;this.name=name;}
    public int getIndex() {return index;}
    public String getName() {return name;}
    public String toString() {return name;}
  };
  public static final String[] _ALL_FIELDS = {"metadata","outlinks","inlinks"};
  static {
    PersistentBase.registerFields(Host.class, _ALL_FIELDS);
  }

  public Host() {
    this(new StateManagerImpl());
  }
  public Host(StateManager stateManager) {
    super(stateManager);
    metadata = new StatefulHashMap<Utf8,ByteBuffer>();
    inlinks = new StatefulHashMap<Utf8,Utf8>();
    outlinks = new StatefulHashMap<Utf8,Utf8>();
  }
  public Host newInstance(StateManager stateManager) {
    return new Host(stateManager);
  }
  public Schema getSchema() { return _SCHEMA; }
  public Object get(int _field) {
    switch (_field) {
    case 0: return metadata;
    case 1: return outlinks;
    case 2: return inlinks;
    default: throw new AvroRuntimeException("Bad index");
    }
  }
  @SuppressWarnings(value="unchecked")
  public void put(int _field, Object _value) {
    
    if(isFieldEqual(_field, _value)) return;
    getStateManager().setDirty(this, _field);
    switch (_field) {
    case 0: metadata = (Map<Utf8,ByteBuffer>)_value; break;
    case 1: outlinks = (Map<Utf8,Utf8>)_value; break;
    case 2: inlinks = (Map<Utf8,Utf8>)_value; break;
    default: throw new AvroRuntimeException("Bad index");
    }
  } 
  @SuppressWarnings("unchecked")
  public Map<Utf8, ByteBuffer> getMetadata() {
    return (Map<Utf8, ByteBuffer>) get(0);
  }
  public ByteBuffer getFromMetadata(Utf8 key) {
    if (metadata == null) { return null; }
    return metadata.get(key);
  }
  
  public void putToMetadata(Utf8 key, ByteBuffer value) {
    getStateManager().setDirty(this, 0);
    metadata.put(key, value);
  }
  public ByteBuffer removeFromMetadata(Utf8 key) {
    if (metadata == null) { return null; }
    getStateManager().setDirty(this, 0);
    return metadata.remove(key);
  }
  @SuppressWarnings("unchecked")
  public Map<Utf8, Utf8> getOutlinks() {
    return (Map<Utf8, Utf8>) get(1);
  }
  public Utf8 getFromOutlinks(Utf8 key) {
    if (outlinks == null) { return null; }
    return outlinks.get(key);
  }
  public void putToOutlinks(Utf8 key, Utf8 value) {
    getStateManager().setDirty(this, 1);
    outlinks.put(key, value);
  }
  public Utf8 removeFromOutlinks(Utf8 key) {
    if (outlinks == null) { return null; }
    getStateManager().setDirty(this, 1);
    return outlinks.remove(key);
  }
  @SuppressWarnings("unchecked")
  public Map<Utf8, Utf8> getInlinks() {
    return (Map<Utf8, Utf8>) get(2);
  }
  public Utf8 getFromInlinks(Utf8 key) {
    if (inlinks == null) { return null; }
    return inlinks.get(key);
  }
  public void putToInlinks(Utf8 key, Utf8 value) {
    getStateManager().setDirty(this, 2);
    inlinks.put(key, value);
  }
  public Utf8 removeFromInlinks(Utf8 key) {
    if (inlinks == null) { return null; }
    getStateManager().setDirty(this, 2);
    return inlinks.remove(key);
  }
  
  public boolean contains(String key) {
    return metadata.containsKey(new Utf8(key));
  }
  
  public String getValue(String key, String defaultValue) {
    if (!contains(key)) return defaultValue;
    return Bytes.toString(metadata.get(new Utf8(key)));
  }
  
  public int getInt(String key, int defaultValue) {
    if (!contains(key)) return defaultValue;
    return Integer.parseInt(getValue(key,null));
  }
  public long getLong(String key, long defaultValue) {
    if (!contains(key)) return defaultValue;
    return Long.parseLong(getValue(key,null));
  }
}
