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
package org.apache.nutch.util;

import java.io.IOException;

import org.apache.nutch.storage.WebPage;
import org.apache.gora.mapreduce.GoraReducer;

public class IdentityPageReducer
extends GoraReducer<String, WebPage, String, WebPage> {

  @Override
  protected void reduce(String key, Iterable<WebPage> values,
      Context context) throws IOException, InterruptedException {
    for (WebPage page : values) {
      context.write(key, page);
    }
  }

}
