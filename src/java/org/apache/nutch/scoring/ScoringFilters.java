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

package org.apache.nutch.scoring;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.plugin.Extension;
import org.apache.nutch.plugin.ExtensionPoint;
import org.apache.nutch.plugin.PluginRepository;
import org.apache.nutch.plugin.PluginRuntimeException;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.ObjectCache;

/**
 * Creates and caches {@link ScoringFilter} implementing plugins.
 *
 * @author Andrzej Bialecki
 */
public class ScoringFilters extends Configured implements ScoringFilter {

  private ScoringFilter[] filters;

  public ScoringFilters(Configuration conf) {
    super(conf);
    ObjectCache objectCache = ObjectCache.get(conf);
    String order = conf.get("scoring.filter.order");
    this.filters = (ScoringFilter[]) objectCache.getObject(ScoringFilter.class.getName());

    if (this.filters == null) {
      String[] orderedFilters = null;
      if (order != null && !order.trim().equals("")) {
        orderedFilters = order.split("\\s+");
      }

      try {
        ExtensionPoint point = PluginRepository.get(conf).getExtensionPoint(ScoringFilter.X_POINT_ID);
        if (point == null) throw new RuntimeException(ScoringFilter.X_POINT_ID + " not found.");
        Extension[] extensions = point.getExtensions();
        HashMap<String, ScoringFilter> filterMap =
          new HashMap<String, ScoringFilter>();
        for (int i = 0; i < extensions.length; i++) {
          Extension extension = extensions[i];
          ScoringFilter filter = (ScoringFilter) extension.getExtensionInstance();
          if (!filterMap.containsKey(filter.getClass().getName())) {
            filterMap.put(filter.getClass().getName(), filter);
          }
        }
        if (orderedFilters == null) {
          objectCache.setObject(ScoringFilter.class.getName(), filterMap.values().toArray(new ScoringFilter[0]));
        } else {
          ScoringFilter[] filter = new ScoringFilter[orderedFilters.length];
          for (int i = 0; i < orderedFilters.length; i++) {
            filter[i] = filterMap.get(orderedFilters[i]);
          }
          objectCache.setObject(ScoringFilter.class.getName(), filter);
        }
      } catch (PluginRuntimeException e) {
        throw new RuntimeException(e);
      }
      this.filters = (ScoringFilter[]) objectCache.getObject(ScoringFilter.class.getName());
    }
  }

  /** Calculate a sort value for Generate. */
  @Override
  public float generatorSortValue(String url, WebPage row, float initSort)
  throws ScoringFilterException {
    for (ScoringFilter filter : filters) {
      initSort = filter.generatorSortValue(url, row, initSort);
    }
    return initSort;
  }

  /** Calculate a new initial score, used when adding newly discovered pages. */
  @Override
  public void initialScore(String url, WebPage row) throws ScoringFilterException {
    for (ScoringFilter filter : filters) {
      filter.initialScore(url, row);
    }
  }

  /** Calculate a new initial score, used when injecting new pages. */
  @Override
  public void injectedScore(String url, WebPage row) throws ScoringFilterException {
    for (ScoringFilter filter : filters) {
      filter.injectedScore(url, row);
    }
  }

  @Override
  public void distributeScoreToOutlinks(String fromUrl, WebPage row,
      Collection<ScoreDatum> scoreData, int allCount)
      throws ScoringFilterException {
    for (ScoringFilter filter : filters) {
      filter.distributeScoreToOutlinks(fromUrl, row, scoreData, allCount);
    }
  }

  @Override
  public void updateScore(String url, WebPage row,
      List<ScoreDatum> inlinkedScoreData) throws ScoringFilterException {
    for (ScoringFilter filter : filters) {
      filter.updateScore(url, row, inlinkedScoreData);
    }
  }

  @Override
  public float indexerScore(String url, NutchDocument doc, WebPage row,
      float initScore) throws ScoringFilterException {
    for (ScoringFilter filter : filters) {
      initScore = filter.indexerScore(url, doc, row, initScore);
    }
    return initScore;
  }

  @Override
  public Collection<WebPage.Field> getFields() {
    Set<WebPage.Field> fields = new HashSet<WebPage.Field>();
    for (ScoringFilter filter : filters) {
      Collection<WebPage.Field> pluginFields = filter.getFields();
      if (pluginFields != null) {
        fields.addAll(pluginFields);
      }
    }
    return fields;
  }
}
