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

package org.apache.nutch.crawl;

import java.lang.invoke.MethodHandles;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configuration.IntegerRanges;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;

/**
 * Utility to test transitions of {@link CrawlDatum} states during an update of
 * {@link CrawlDb} (command {@literal updatedb}): call
 * {@link CrawlDbReducer#reduce(Text, Iterator, OutputCollector, Reporter)}
 * (using MRUnit) with the old CrawlDatum (db status) and the new one (fetch
 * status)
 */
public class CrawlDbUpdateTestDriver<T extends Reducer<Text, CrawlDatum, Text, CrawlDatum>> {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  private ReduceDriver<Text, CrawlDatum, Text, CrawlDatum> reduceDriver;
  private T reducer;
  private Configuration configuration;

  public static Text dummyURL = new Text("http://nutch.apache.org/");

//  protected CrawlDbUpdateUtilNewAPI(T red, T.Context con) {
  protected CrawlDbUpdateTestDriver(T updateReducer, Configuration conf) {
    reducer = updateReducer;
    configuration = conf;
  }

  /**
   * run
   * {@link CrawlDbReducer#reduce(Text, Iterator, OutputCollector, Reporter)}
   * and return the CrawlDatum(s) which would have been written into CrawlDb
   *
   * @param values
   *          list of input CrawlDatums
   * @return list of resulting CrawlDatum(s) in CrawlDb
   */
  public List<CrawlDatum> update(List<CrawlDatum> values) {
    List<CrawlDatum> result = new ArrayList<CrawlDatum>(0);
    if (values == null || values.size() == 0) {
      return result;
    }
    Collections.shuffle(values); // sorting of values should have no influence
    reduceDriver = ReduceDriver.newReduceDriver(reducer);
    reduceDriver.getConfiguration().addResource(configuration);
    reduceDriver.withInput(dummyURL, values);
    List<Pair<Text,CrawlDatum>> reduceResult;
    try {
      reduceResult = reduceDriver.run();
      for (Pair<Text,CrawlDatum> p : reduceResult) {
        if (p.getFirst().equals(dummyURL)) {
          result.add(p.getSecond());
        }
      }
    } catch (IOException e) {
      LOG.error(StringUtils.stringifyException(e));
      return result;
    }
    return result;
  }

  /**
   * run
   * {@link CrawlDbReducer#reduce(Text, Iterator, OutputCollector, Reporter)}
   * and return the CrawlDatum(s) which would have been written into CrawlDb
   *
   * @param dbDatum
   *          previous CrawlDatum in CrawlDb
   * @param fetchDatum
   *          CrawlDatum resulting from fetching
   * @return list of resulting CrawlDatum(s) in CrawlDb
   */
  public List<CrawlDatum> update(CrawlDatum dbDatum, CrawlDatum fetchDatum) {
    List<CrawlDatum> values = new ArrayList<CrawlDatum>();
    if (dbDatum != null)
      values.add(dbDatum);
    if (fetchDatum != null)
      values.add(fetchDatum);
    return update(values);
  }

  /**
   * see {@link #update(List)}
   */
  public List<CrawlDatum> update(CrawlDatum... values) {
    return update(Arrays.asList(values));
  }

}
