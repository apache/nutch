/**
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
package org.apache.nutch.service;

import java.io.FileNotFoundException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.service.impl.SequenceReader;
import org.apache.nutch.util.NutchConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface  NutchReader {
  
  public static final Logger LOG = LoggerFactory.getLogger(NutchReader.class);
  public static final Configuration conf = NutchConfiguration.create();
  
  public List read(String path) throws FileNotFoundException;
  public List head(String path, int nrows) throws FileNotFoundException;
  public List slice(String path, int start, int end) throws FileNotFoundException;
  public int count(String path) throws FileNotFoundException;
}
