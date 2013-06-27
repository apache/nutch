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
package org.apache.nutch.api;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;

import org.apache.avro.util.Utf8;
import org.apache.gora.query.Query;
import org.apache.gora.query.Result;
import org.apache.gora.store.DataStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.parse.ParseStatusUtils;
import org.apache.nutch.protocol.ProtocolStatusUtils;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.ParseStatus;
import org.apache.nutch.storage.ProtocolStatus;
import org.apache.nutch.storage.StorageUtils;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.Bytes;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.StringUtil;
import org.apache.nutch.util.TableUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DbReader {
  private static final Logger LOG = LoggerFactory.getLogger(DbReader.class);

  DataStore<String,WebPage> store;
  Configuration conf;
  
  public DbReader(Configuration conf, String crawlId) {
    conf = new Configuration(conf);
    if (crawlId != null) {
      conf.set(Nutch.CRAWL_ID_KEY, crawlId);
    }
    try {
      store = StorageUtils.createWebStore(conf, String.class, WebPage.class);
    } catch (Exception e) {
      e.printStackTrace();
      store = null;
    }
  }
  
  public Iterator<Map<String,Object>> iterator(String[] fields, String startKey, String endKey,
      String batchId) throws Exception {
    Query<String,WebPage> q = store.newQuery();
    String[] qFields = fields;
    if (fields != null) {
      HashSet<String> flds = new HashSet<String>(Arrays.asList(fields));
      // remove "url"
      flds.remove("url");
      if (flds.size() > 0) {
        qFields = flds.toArray(new String[flds.size()]);
      } else {
        qFields = null;
      }
    }
    q.setFields(qFields);
    if (startKey != null) {
      q.setStartKey(startKey);
      if (endKey != null) {
        q.setEndKey(endKey);
      }
    }
    Result<String,WebPage> res = store.execute(q);
    // XXX we should add the filtering capability to Query
    return new DbIterator(res, fields, batchId);
  }
  
  public void close() throws IOException {
    if (store != null) {
      store.close();
    }
  }
  
  private class DbIterator implements Iterator<Map<String,Object>> {
    private Result<String,WebPage> res;
    private boolean hasNext;
    private String url;
    private WebPage page;
    private Utf8 batchId;
    private TreeSet<String> fields;

    DbIterator(Result<String,WebPage> res, String[] fields, String batchId) throws IOException {
      this.res = res;
      if (batchId != null) {
        this.batchId = new Utf8(batchId);
      }
      if (fields != null) {
        this.fields = new TreeSet<String>(Arrays.asList(fields));
      }
      try {
        advance();
      } catch (Exception e){
        e.printStackTrace();
      }
    }
    
    private void advance() throws Exception, IOException {
      hasNext = res.next();
      if (hasNext && batchId != null) {
        do {
          WebPage page = res.get();
          Utf8 mark = Mark.UPDATEDB_MARK.checkMark(page);
          if (NutchJob.shouldProcess(mark, batchId)) {
            return;
          } else {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Skipping " + 
                TableUtil.unreverseUrl(res.getKey()) + "; different batch id");
            }
            hasNext = res.next();
          }
        } while (hasNext);
      }
    }

    public boolean hasNext() {
      return hasNext;
    }

    public Map<String,Object> next() {
      url = res.getKey();
      page = (WebPage)res.get().clone();
      try {
        advance();
        if (!hasNext) {
          res.close();
        }
      } catch (IOException e) {
        e.printStackTrace();
        hasNext = false;
        return null;
      } catch (Exception e) {
        e.printStackTrace();
        hasNext = false;
        return null;
      }
      return pageAsMap(url, page);
    }

    @SuppressWarnings("unchecked")
    private Map<String,Object> pageAsMap(String url, WebPage page) {
      HashMap<String,Object> res = new HashMap<String,Object>();
      if (fields == null || fields.contains("url")) {
        res.put("url", TableUtil.unreverseUrl(url));
      }
      String[] pfields = page.getFields();
      TreeSet<String> flds = null;
      if (fields != null) {
        flds = (TreeSet<String>)fields.clone();
      } else {
        flds = new TreeSet<String>(Arrays.asList(pfields));
      }
      flds.retainAll(Arrays.asList(pfields));
      for (String f : flds) {
        int idx = page.getFieldIndex(f);
        if (idx < 0) {
          continue;
        }
        Object val = page.get(idx);
        if (val == null) {
          continue;
        }
        if ("metadata".equals(f)) {
          Map<Utf8, ByteBuffer> metadata = page.getMetadata();
          Map<String,String> simpleMeta = new HashMap<String,String>();
          if (metadata != null) {
            Iterator<Entry<Utf8, ByteBuffer>> iterator = metadata.entrySet()
                .iterator();
            while (iterator.hasNext()) {
              Entry<Utf8, ByteBuffer> entry = iterator.next();
              simpleMeta.put(entry.getKey().toString(), 
                  Bytes.toStringBinary(entry.getValue()));
            }
          }
          res.put(f, simpleMeta);
        } else if ("protocolStatus".equals(f)) {
          ProtocolStatus ps = page.getProtocolStatus();
          res.put(f, ProtocolStatusUtils.toString(ps));
        } else if ("parseStatus".equals(f)) {
          ParseStatus ps = page.getParseStatus();
          res.put(f, ParseStatusUtils.toString(ps));
        } else if ("signature".equals(f)) {
          ByteBuffer bb = page.getSignature();
          res.put(f, StringUtil.toHexString(bb));
        } else if ("content".equals(f)) {
          ByteBuffer bb = page.getContent();
          res.put(f, Bytes.toStringBinary(bb));
        } else if ("markers".equals(f)) {
          res.put(f, convertMap(page.getMarkers()));
        } else if ("inlinks".equals(f)) {
          res.put(f, convertMap(page.getInlinks()));
        } else if ("outlinks".equals(f)) {
          res.put(f, convertMap(page.getOutlinks()));
        } else {
          if (val instanceof Utf8) {
            val = val.toString();
          } else if (val instanceof ByteBuffer) {
            val = Bytes.toStringBinary((ByteBuffer)val);
          }
          res.put(f, val);
        }
      }
      return res;
    }
    
    private Map<String,String> convertMap(Map<?,?> map) {
      Map<String,String> res = new HashMap<String,String>();
      for (Object o : map.entrySet()) {
        Entry<?, ?> e = (Entry<?, ?>)o;
        res.put(e.getKey().toString(), e.getValue().toString());
      }
      return res;
    }
    
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
