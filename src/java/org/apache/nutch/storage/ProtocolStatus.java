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
import java.util.HashMap;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Protocol;
import org.apache.avro.util.Utf8;
import org.apache.avro.ipc.AvroRemoteException;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.specific.FixedSize;
import org.apache.avro.specific.SpecificExceptionBase;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificFixed;
import org.apache.gora.persistency.StateManager;
import org.apache.gora.persistency.impl.PersistentBase;
import org.apache.gora.persistency.impl.StateManagerImpl;
import org.apache.gora.persistency.StatefulHashMap;
import org.apache.gora.persistency.ListGenericArray;
import org.apache.nutch.protocol.ProtocolStatusUtils;

@SuppressWarnings("all")
public class ProtocolStatus extends PersistentBase {
  public static final Schema _SCHEMA = Schema.parse("{\"type\":\"record\",\"name\":\"ProtocolStatus\",\"namespace\":\"org.apache.nutch.storage\",\"fields\":[{\"name\":\"code\",\"type\":\"int\"},{\"name\":\"args\",\"type\":{\"type\":\"array\",\"items\":\"string\"}},{\"name\":\"lastModified\",\"type\":\"long\"}]}");
  public static enum Field {
    CODE(0,"code"),
    ARGS(1,"args"),
    LAST_MODIFIED(2,"lastModified"),
    ;
    private int index;
    private String name;
    Field(int index, String name) {this.index=index;this.name=name;}
    public int getIndex() {return index;}
    public String getName() {return name;}
    public String toString() {return name;}
  };
  public static final String[] _ALL_FIELDS = {"code","args","lastModified",};
  static {
    PersistentBase.registerFields(ProtocolStatus.class, _ALL_FIELDS);
  }
  private int code;
  private GenericArray<Utf8> args;
  private long lastModified;
  public ProtocolStatus() {
    this(new StateManagerImpl());
  }
  public ProtocolStatus(StateManager stateManager) {
    super(stateManager);
    args = new ListGenericArray<Utf8>(getSchema().getField("args").schema());
  }
  public ProtocolStatus newInstance(StateManager stateManager) {
    return new ProtocolStatus(stateManager);
  }
  public Schema getSchema() { return _SCHEMA; }
  public Object get(int _field) {
    switch (_field) {
    case 0: return code;
    case 1: return args;
    case 2: return lastModified;
    default: throw new AvroRuntimeException("Bad index");
    }
  }
  @SuppressWarnings(value="unchecked")
  public void put(int _field, Object _value) {
    if(isFieldEqual(_field, _value)) return;
    getStateManager().setDirty(this, _field);
    switch (_field) {
    case 0:code = (Integer)_value; break;
    case 1:args = (GenericArray<Utf8>)_value; break;
    case 2:lastModified = (Long)_value; break;
    default: throw new AvroRuntimeException("Bad index");
    }
  }
  public int getCode() {
    return (Integer) get(0);
  }
  public void setCode(int value) {
    put(0, value);
  }
  @SuppressWarnings("unchecked")
  public GenericArray<Utf8> getArgs() {
    return (GenericArray<Utf8>) get(1);
  }
  public void addToArgs(Utf8 element) {
    getStateManager().setDirty(this, 1);
    args.add(element);
  }
  public long getLastModified() {
    return (Long) get(2);
  }
  public void setLastModified(long value) {
    put(2, value);
  }
  
  /**
   * A convenience method which returns a successful {@link ProtocolStatus}.
   * @return the {@link ProtocolStatus} value for 200 (success).
   */
  public boolean isSuccess() {
    return code == ProtocolStatusUtils.SUCCESS; 
  }
}
