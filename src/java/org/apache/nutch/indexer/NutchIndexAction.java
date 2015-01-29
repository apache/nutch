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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import org.apache.nutch.indexer.NutchDocument;

/**
 * A {@link NutchIndexAction} is the new unit of indexing holding the document
 * and action information.
 */
public class NutchIndexAction implements Writable {

  public static final byte ADD = 0;
  public static final byte DELETE = 1;
  public static final byte UPDATE = 2;

  public NutchDocument doc = null;
  public byte action = ADD;

  public NutchIndexAction(NutchDocument doc, byte action) {
    this.doc = doc;
    this.action = action;
  }

  public void readFields(DataInput in) throws IOException {
    action = in.readByte();
    doc = new NutchDocument();
    doc.readFields(in);
  }

  public void write(DataOutput out) throws IOException {
    out.write(action);
    doc.write(out);
  }
}
