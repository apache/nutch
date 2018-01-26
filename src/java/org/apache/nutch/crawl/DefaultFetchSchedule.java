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

import org.apache.hadoop.io.Text;

/**
 * This class implements the default re-fetch schedule. That is, no matter if
 * the page was changed or not, the <code>fetchInterval</code> remains
 * unchanged, and the updated page fetchTime will always be set to
 * <code>fetchTime + fetchInterval * 1000</code>.
 * 
 * @author Andrzej Bialecki
 */
public class DefaultFetchSchedule extends AbstractFetchSchedule {

  @Override
  public CrawlDatum setFetchSchedule(Text url, CrawlDatum datum,
      long prevFetchTime, long prevModifiedTime, long fetchTime,
      long modifiedTime, int state) {
    datum = super.setFetchSchedule(url, datum, prevFetchTime, prevModifiedTime,
        fetchTime, modifiedTime, state);
    if (datum.getFetchInterval() == 0) {
      datum.setFetchInterval(defaultInterval);
    }
    datum.setFetchTime(fetchTime + (long) datum.getFetchInterval() * 1000);
    if (modifiedTime <= 0 || state == FetchSchedule.STATUS_MODIFIED) {
      // Set modifiedTime to fetchTime on first successful fetch
      modifiedTime = fetchTime;
    }
    datum.setModifiedTime(modifiedTime);
    return datum;
  }
}
