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
package org.apache.nutch.util;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Reducer;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Supplies a {@link Reducer.Context} for unit tests (e.g. {@code IndexerMapReduce},
 * {@code Injector}) without subclassing Hadoop's abstract {@code Reducer.Context}.
 * <p>
 * Hadoop marks several {@code JobContext} methods as {@link Deprecated}; an
 * anonymous subclass that {@code @Override}s them triggers javac deprecation
 * warnings. A Mockito mock implements only behavior we stub here, so test code
 * does not reference those deprecated APIs directly.
 *
 * @param <KEYIN> Type of input keys
 * @param <VALUEIN> Type of input values
 * @param <KEYOUT> Type of output keys
 * @param <VALUEOUT> Type of output values
 */
public class ReducerContextWrapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

  private final Configuration config;
  private final Counters counters = new Counters();
  private final Map<KEYOUT, VALUEOUT> valuesOut;

  private Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context context;

  /**
   * @param reducer   reducer under test (retained for call-site clarity; not used by the mock)
   * @param config    configuration exposed by {@link Reducer.Context#getConfiguration()}
   * @param valuesOut map receiving {@link Reducer.Context#write(Object, Object)} calls
   */
  public ReducerContextWrapper(Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> reducer,
      Configuration config, Map<KEYOUT, VALUEOUT> valuesOut) {
    Objects.requireNonNull(reducer, "reducer");
    this.config = Objects.requireNonNull(config, "config");
    this.valuesOut = Objects.requireNonNull(valuesOut, "valuesOut");
    initContext();
  }

  /**
   * @return context suitable for {@link Reducer#setup(Reducer.Context)} and
   *         {@link Reducer#reduce(Object, Iterable, Reducer.Context)}
   */
  public Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context getContext() {
    return context;
  }

  /**
   * Return the underlying counters updated by the context, for assertions in tests.
   * Uses the real Hadoop Counters API (no mocks).
   *
   * @return the counters instance
   */
  public Counters getCounters() {
    return counters;
  }

  @SuppressWarnings("unchecked")
  private void initContext() {
    context = Mockito.mock(Reducer.Context.class,
        Mockito.withSettings().defaultAnswer(Mockito.RETURNS_DEFAULTS));

    Mockito.lenient().when(context.getConfiguration()).thenReturn(config);

    Mockito.lenient().when(context.getCounter(ArgumentMatchers.<Enum<?>>any()))
        .thenAnswer(inv -> counters.findCounter(inv.getArgument(0, Enum.class)));

    Mockito.lenient().when(context.getCounter(Mockito.anyString(), Mockito.anyString()))
        .thenAnswer(inv -> counters.findCounter(
            inv.getArgument(0, String.class), inv.getArgument(1, String.class)));

    try {
      Mockito.doAnswer(new Answer<Void>() {
        @Override
        @SuppressWarnings("unchecked")
        public Void answer(InvocationOnMock inv) {
          KEYOUT k = inv.getArgument(0);
          VALUEOUT v = inv.getArgument(1);
          valuesOut.put(k, v);
          return null;
        }
      }).when(context).write(Mockito.any(), Mockito.any());
    } catch (IOException | InterruptedException e) {
      throw new IllegalStateException(e);
    }
  }

}
