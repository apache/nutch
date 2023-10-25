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
package org.apache.nutch.scoring.orphan;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.scoring.ScoringFilter;
import org.apache.nutch.util.NutchConfiguration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestOrphanScoringFilter {

  @Test
  public void testOrphanScoringFilter() throws Exception {
    Configuration conf = NutchConfiguration.create();

    conf.setInt("scoring.orphan.mark.gone.after", 5);
    conf.setInt("scoring.orphan.mark.orphan.after", 10);

    ScoringFilter filter = new OrphanScoringFilter();
    filter.setConf(conf);

    Text url = new Text("http://nutch.apache.org/");
    CrawlDatum datum = new CrawlDatum();
    datum.setStatus(CrawlDatum.STATUS_DB_NOTMODIFIED);

    List<CrawlDatum> emptyListOfInlinks = new ArrayList<CrawlDatum>();
    List<CrawlDatum> populatedListOfInlinks = new ArrayList<CrawlDatum>();
    populatedListOfInlinks.add(datum);

    // Act as if record has inlinks
    filter.updateDbScore(url, null, datum, populatedListOfInlinks);
    int firstOrphanTime = getTime(datum);
    Assertions.assertTrue(datum.getMetaData()
        .containsKey(OrphanScoringFilter.ORPHAN_KEY_WRITABLE));

    // Wait a little bit
    try {
      Thread.sleep(1000);
    } catch (Exception e) {
    }

    // Again, this time orphan time must be increased by about 1000 ms
    filter.updateDbScore(url, null, datum, populatedListOfInlinks);
    int secondOrphanTime = getTime(datum);
    Assertions.assertTrue(secondOrphanTime > firstOrphanTime);

    // Act as if no more inlinks, time will not increase, status is still the
    // same
    filter.updateDbScore(url, null, datum, emptyListOfInlinks);
    int thirdOrphanTime = getTime(datum);
    Assertions.assertEquals(thirdOrphanTime, secondOrphanTime);
    Assertions.assertEquals(CrawlDatum.STATUS_DB_NOTMODIFIED, datum.getStatus(),
        "Expected status db_notmodified but got "
            + CrawlDatum.getStatusName(datum.getStatus()));

    // Wait a little bit
    try {
      Thread.sleep(1000);
    } catch (Exception e) {
    }

    // Act as if no more inlinks, time will not increase, status is still the
    // same
    filter.updateDbScore(url, null, datum, emptyListOfInlinks);
    Assertions.assertEquals(CrawlDatum.STATUS_DB_NOTMODIFIED, datum.getStatus(),
        "Expected status db_notmodified but got "
            + CrawlDatum.getStatusName(datum.getStatus()));

    // Wait until scoring.orphan.mark.gone.after
    try {
      Thread.sleep(5000);
    } catch (Exception e) {
    }

    // Again, but now markgoneafter has expired and record should be DB_GONE
    filter.updateDbScore(url, null, datum, emptyListOfInlinks);
    int fourthOrphanTime = getTime(datum);
    Assertions.assertEquals(fourthOrphanTime, thirdOrphanTime);
    Assertions.assertEquals(CrawlDatum.STATUS_DB_GONE, datum.getStatus(),
        "Expected status db_gone but got "
            + CrawlDatum.getStatusName(datum.getStatus()));

    // Wait until scoring.orphan.mark.orphan.after
    try {
      Thread.sleep(5000);
    } catch (Exception e) {
    }

    // Again, but now markgoneafter has expired and record should be DB_ORPHAN
    filter.updateDbScore(url, null, datum, emptyListOfInlinks);
    Assertions.assertEquals(CrawlDatum.STATUS_DB_ORPHAN, datum.getStatus(),
        "Expected status db_orphan but got "
            + CrawlDatum.getStatusName(datum.getStatus()));
  }

  protected int getTime(CrawlDatum datum) {
    IntWritable writable = (IntWritable) datum.getMetaData()
        .get(OrphanScoringFilter.ORPHAN_KEY_WRITABLE);
    return writable.get();
  }
}
