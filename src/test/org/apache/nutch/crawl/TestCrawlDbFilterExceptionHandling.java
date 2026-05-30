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
package org.apache.nutch.crawl;

import java.lang.reflect.Field;
import java.net.MalformedURLException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.nutch.metrics.ErrorTracker;
import org.apache.nutch.net.URLFilterException;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.net.URLNormalizers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link CrawlDbFilter} exception handling (NUTCH-3164).
 *
 * <p>Verifies that:
 * <ul>
 * <li>{@link MalformedURLException} from the normalizer drops the URL but does
 * NOT increment {@code urlsFilteredCounter} — malformed-input losses are no
 * longer conflated with filtering.
 * <li>{@link RuntimeException} from either the normalizer or filter does NOT
 * drop the URL, so plugin bugs no longer silently delete data.
 * <li>{@link URLFilterException} does NOT drop the URL: per the
 * {@link org.apache.nutch.net.URLFilter} contract, rejection is signaled by
 * returning {@code null}, not by an exception.
 * <li>Every error path routes through {@link ErrorTracker}.
 * </ul>
 *
 * <p>The mapper's {@link CrawlDbFilter#setup} method is bypassed because it
 * would load real plugins from the {@code PluginRepository}. Private fields are
 * populated by reflection, mirroring the pattern used in
 * {@code TestCommonCrawlDataDumper}.
 */
@ExtendWith(MockitoExtension.class)
public class TestCrawlDbFilterExceptionHandling {

  @SuppressWarnings("rawtypes")
  @Mock
  private Mapper.Context context;

  @Mock
  private URLNormalizers normalizers;

  @Mock
  private URLFilters filters;

  @Mock
  private ErrorTracker errorTracker;

  @Mock
  private Counter urlsFilteredCounter;

  private CrawlDbFilter mapper;
  private Text key;
  private CrawlDatum value;

  @BeforeEach
  void setUp() throws Exception {
    mapper = new CrawlDbFilter();
    setField("urlNormalizers", true);
    setField("urlFiltering", true);
    setField("scope", URLNormalizers.SCOPE_CRAWLDB);
    setField("normalizers", normalizers);
    setField("filters", filters);
    setField("errorTracker", errorTracker);
    setField("urlsFilteredCounter", urlsFilteredCounter);

    key = new Text("http://example.com");
    value = new CrawlDatum(CrawlDatum.STATUS_DB_FETCHED, 0, 0.0f);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void malformedUrlFromNormalizer_isDroppedWithoutCountingAsFiltered()
      throws Exception {
    MalformedURLException ex = new MalformedURLException("bad URL");
    when(normalizers.normalize(anyString(), anyString())).thenThrow(ex);

    mapper.map(key, value, context);

    verify(context, never()).write(any(), any());
    verify(errorTracker, times(1)).incrementCounters(ex);
    verify(urlsFilteredCounter, never()).increment(anyLong());
    verify(filters, never()).filter(anyString());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void runtimeExceptionFromNormalizer_keepsUrl() throws Exception {
    RuntimeException ex = new RuntimeException("normalizer plugin bug");
    when(normalizers.normalize(anyString(), anyString())).thenThrow(ex);
    when(filters.filter(anyString())).thenAnswer(inv -> inv.getArgument(0));

    mapper.map(key, value, context);

    verify(context, times(1)).write(eq(key), eq(value));
    verify(errorTracker, times(1)).incrementCounters(ex);
    verify(urlsFilteredCounter, never()).increment(anyLong());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void urlFilterException_keepsUrl() throws Exception {
    when(normalizers.normalize(anyString(), anyString()))
        .thenAnswer(inv -> inv.getArgument(0));
    URLFilterException ex = new URLFilterException("internal filter error");
    when(filters.filter(anyString())).thenThrow(ex);

    mapper.map(key, value, context);

    verify(context, times(1)).write(eq(key), eq(value));
    verify(errorTracker, times(1)).incrementCounters(ex);
    verify(urlsFilteredCounter, never()).increment(anyLong());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void runtimeExceptionFromFilter_keepsUrl() throws Exception {
    when(normalizers.normalize(anyString(), anyString()))
        .thenAnswer(inv -> inv.getArgument(0));
    RuntimeException ex = new RuntimeException("filter plugin bug");
    when(filters.filter(anyString())).thenThrow(ex);

    mapper.map(key, value, context);

    verify(context, times(1)).write(eq(key), eq(value));
    verify(errorTracker, times(1)).incrementCounters(ex);
    verify(urlsFilteredCounter, never()).increment(anyLong());
  }

  private void setField(String name, Object fieldValue) throws Exception {
    Field f = CrawlDbFilter.class.getDeclaredField(name);
    f.setAccessible(true);
    f.set(mapper, fieldValue);
  }
}
