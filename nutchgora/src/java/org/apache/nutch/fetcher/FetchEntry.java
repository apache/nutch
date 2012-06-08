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
package org.apache.nutch.fetcher;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.nutch.storage.WebPage;
import org.apache.gora.util.IOUtils;

public class FetchEntry extends Configured implements Writable {

  private String key;
  private WebPage page;

  public FetchEntry() {
    super(null);
  }

  public FetchEntry(Configuration conf, String key, WebPage page) {
    super(conf);
    this.key = key;
    this.page = page;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    key = Text.readString(in);
    page = IOUtils.deserialize(getConf(), in, null, WebPage.class);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Text.writeString(out, key);
    IOUtils.serialize(getConf(), out, page, WebPage.class);
  }

  public String getKey() {
    return key;
  }

  public WebPage getWebPage() {
    return page;
  }

  @Override
  public String toString() {
    return "FetchEntry [key=" + key + ", page=" + page + "]";
  }
  
  
}
