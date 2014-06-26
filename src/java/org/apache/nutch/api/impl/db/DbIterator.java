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
package org.apache.nutch.api.impl.db;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.avro.util.Utf8;
import org.apache.commons.collections.CollectionUtils;
import org.apache.gora.query.Result;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.TableUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.google.common.collect.UnmodifiableIterator;

public class DbIterator extends UnmodifiableIterator<Map<String, Object>> {
  private static final Logger LOG = LoggerFactory.getLogger(DbIterator.class);

  private Result<String, WebPage> result;
  private boolean hasNext;
  private String url;
  private WebPage page;
  private Utf8 batchId;
  private Set<String> commonFields;

  DbIterator(Result<String, WebPage> res, Set<String> fields, String batchId) {
    this.result = res;
    if (batchId != null) {
      this.batchId = new Utf8(batchId);
    }
    if (fields != null) {
      this.commonFields = Sets.newTreeSet(fields);
    }
    try {
      skipNonRelevant();
    } catch (Exception e) {
      LOG.error("Cannot create db iterator!", e);
    }
  }

  private void skipNonRelevant() throws Exception, IOException {
    hasNext = result.next();
    if (!hasNext) {
      return;
    }
    if (batchId == null) {
      return;
    }

    while (hasNext) {
      WebPage page = result.get();
      Utf8 mark = Mark.UPDATEDB_MARK.checkMark(page);
      if (NutchJob.shouldProcess(mark, batchId)) {
        return;
      }

      LOG.debug("Skipping {}; different batch id", result.getKey());
      hasNext = result.next();
    }
  }

  public boolean hasNext() {
    return hasNext;
  }

  public Map<String, Object> next() {
    url = result.getKey();
    page = WebPage.newBuilder(result.get()).build();
    try {
      skipNonRelevant();
      if (!hasNext) {
        result.close();
      }
    } catch (Exception e) {
      LOG.error("Cannot get next result!", e);
      hasNext = false;
      return null;
    }
    return pageAsMap(url, page);
  }

  private Map<String, Object> pageAsMap(String url, WebPage page) {
    Map<String, Object> result = DbPageConverter.convertPage(page, commonFields);

    if (CollectionUtils.isEmpty(commonFields) || commonFields.contains("url")) {
      result.put("url", TableUtil.unreverseUrl(url));
    }
    return result;
  }

}