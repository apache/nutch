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
package org.apache.nutch.webui.client;

import java.util.concurrent.ExecutionException;

import org.apache.nutch.webui.client.impl.NutchClientImpl;
import org.apache.nutch.webui.model.NutchInstance;
import org.springframework.stereotype.Component;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

@Component
public class NutchClientFactory {
  private LoadingCache<NutchInstance, NutchClient> cache;

  public NutchClientFactory() {
    cache = CacheBuilder.newBuilder().build(new NutchClientCacheLoader());
  }

  public NutchClient getClient(NutchInstance instance) {
    try {
      return cache.get(instance);
    } catch (ExecutionException e) {
      throw new IllegalStateException(e);
    }
  }

  private static class NutchClientCacheLoader extends CacheLoader<NutchInstance, NutchClient> {
    @Override
    public NutchClient load(NutchInstance key) throws Exception {
      return new NutchClientImpl(key);
    }
  }
}
