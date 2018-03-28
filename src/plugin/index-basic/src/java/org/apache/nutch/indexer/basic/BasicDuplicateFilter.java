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

package org.apache.nutch.indexer.basic;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.indexer.DuplicateFilter;

public class BasicDuplicateFilter implements DuplicateFilter {
  private Configuration conf;

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public CharSequence filter(List<CharSequence> duplicates) {
    CharSequence original = null;
    for (CharSequence duplicate : duplicates) {
      if (isShorter(duplicate, original)) {
        original = duplicate;
      }
    }
    return original;
  }
  
  private boolean isShorter(CharSequence duplicate, CharSequence original) {
    if (original == null) {
      return true;
    }
    int originalPathSegmentCount = original.toString().split("/").length;
    int duplicatePathSegmentCount = duplicate.toString().split("/").length;
    if (duplicatePathSegmentCount < originalPathSegmentCount) {
      return true;
    } else if (duplicatePathSegmentCount > originalPathSegmentCount) {
      return false;
    } else {
      if (duplicate.length() <= original.length()) {
        return true;
      }
    }
    return false;
  }

}
