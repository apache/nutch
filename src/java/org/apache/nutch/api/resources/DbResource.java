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
package org.apache.nutch.api.resources;

import java.util.Iterator;
import java.util.Map;
import java.util.WeakHashMap;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;

import org.apache.nutch.api.impl.db.DbReader;
import org.apache.nutch.api.model.request.DbFilter;
import org.apache.nutch.api.model.response.DbQueryResult;

@Path("/db")
public class DbResource extends AbstractResource {

  private Map<String, DbReader> readers = new WeakHashMap<String, DbReader>();

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public DbQueryResult runQuery(DbFilter filter) {
    if (filter == null) {
      throwBadRequestException("Filter cannot be null!");
    }

    DbQueryResult result = new DbQueryResult();
    Iterator<Map<String, Object>> iterator = getReader().runQuery(filter);
    while (iterator.hasNext()) {
      result.addValue(iterator.next());
    }
    return result;
  }

  private DbReader getReader() {
    String confId = ConfigResource.DEFAULT;
    synchronized (readers) {
      if (!readers.containsKey(confId)) {
        readers.put(confId, new DbReader(configManager.get(confId), null));
      }
      return readers.get(confId);
    }
  }
}
