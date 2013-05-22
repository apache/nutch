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

@SuppressWarnings("all")
public class ParseStatus extends PersistentBase {
  public static final Schema _SCHEMA = Schema.parse("{\"type\":\"record\",\"name\":\"ParseStatus\",\"namespace\":\"org.apache.nutch.storage\",\"fields\":[{\"name\":\"majorCode\",\"type\":\"int\"},{\"name\":\"minorCode\",\"type\":\"int\"},{\"name\":\"args\",\"type\":{\"type\":\"array\",\"items\":\"string\"}}]}");
  public static enum Field {
    MAJOR_CODE(0,"majorCode"),
    MINOR_CODE(1,"minorCode"),
    ARGS(2,"args"),
    ;
    private int index;
    private String name;
    Field(int index, String name) {this.index=index;this.name=name;}
    public int getIndex() {return index;}
    public String getName() {return name;}
    public String toString() {return name;}
  };
  public static final String[] _ALL_FIELDS = {"majorCode","minorCode","args",};
  static {
    PersistentBase.registerFields(ParseStatus.class, _ALL_FIELDS);
  }
  private int majorCode;
  private int minorCode;
  private GenericArray<Utf8> args;
  public ParseStatus() {
    this(new StateManagerImpl());
  }
  public ParseStatus(StateManager stateManager) {
    super(stateManager);
    args = new ListGenericArray<Utf8>(getSchema().getField("args").schema());
  }
  public ParseStatus newInstance(StateManager stateManager) {
    return new ParseStatus(stateManager);
  }
  public Schema getSchema() { return _SCHEMA; }
  public Object get(int _field) {
    switch (_field) {
    case 0: return majorCode;
    case 1: return minorCode;
    case 2: return args;
    default: throw new AvroRuntimeException("Bad index");
    }
  }
  @SuppressWarnings(value="unchecked")
  public void put(int _field, Object _value) {
    if(isFieldEqual(_field, _value)) return;
    getStateManager().setDirty(this, _field);
    switch (_field) {
    case 0:majorCode = (Integer)_value; break;
    case 1:minorCode = (Integer)_value; break;
    case 2:args = (GenericArray<Utf8>)_value; break;
    default: throw new AvroRuntimeException("Bad index");
    }
  }
  public int getMajorCode() {
    return (Integer) get(0);
  }
  public void setMajorCode(int value) {
    put(0, value);
  }
  public int getMinorCode() {
    return (Integer) get(1);
  }
  public void setMinorCode(int value) {
    put(1, value);
  }
  @SuppressWarnings("unchecked")
  public GenericArray<Utf8> getArgs() {
    return (GenericArray<Utf8>) get(2);
  }
  public void addToArgs(Utf8 element) {
    getStateManager().setDirty(this, 2);
    args.add(element);
  }
}
