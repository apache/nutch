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
package org.apache.nutch.webui.service.impl;

import java.sql.SQLException;
import java.util.List;

import javax.annotation.Resource;

import org.apache.nutch.webui.client.NutchClient;
import org.apache.nutch.webui.client.NutchClientFactory;
import org.apache.nutch.webui.client.impl.CrawlingCycle;
import org.apache.nutch.webui.client.impl.RemoteCommandsBatchFactory;
import org.apache.nutch.webui.client.impl.CrawlingCycleListener;
import org.apache.nutch.webui.client.impl.RemoteCommand;
import org.apache.nutch.webui.client.impl.RemoteCommandExecutor;
import org.apache.nutch.webui.client.model.Crawl;
import org.apache.nutch.webui.client.model.Crawl.CrawlStatus;
import org.apache.nutch.webui.model.NutchInstance;
import org.apache.nutch.webui.service.CrawlService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import com.j256.ormlite.dao.Dao;

@Service
public class CrawlServiceImpl implements CrawlService, CrawlingCycleListener {
  private Logger log = LoggerFactory.getLogger(CrawlServiceImpl.class);

  @Resource
  private Dao<Crawl, Long> crawlDao;

  @Resource
  private NutchClientFactory nutchClientFactory;

  @Resource
  private RemoteCommandsBatchFactory commandFactory;

  @Override
  @Async
  public void startCrawl(Long crawlId, NutchInstance instance) {
    Crawl crawl = null;
    try {
      crawl = crawlDao.queryForId(crawlId);
      NutchClient client = nutchClientFactory.getClient(instance);
      String seedDirectory = client.createSeed(crawl.getSeedList());
      crawl.setSeedDirectory(seedDirectory);

      List<RemoteCommand> commands = commandFactory.createCommands(crawl);
      RemoteCommandExecutor executor = new RemoteCommandExecutor(client);

      CrawlingCycle cycle = new CrawlingCycle(this, executor, crawl, commands);
      cycle.executeCrawlCycle();

    } catch (Exception e) {
      crawl.setStatus(CrawlStatus.ERROR);
      saveCrawl(crawl);
      log.error("exception occured", e);
    }
  }

  @Override
  public List<Crawl> getCrawls() {
    try {
      return crawlDao.queryForAll();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void saveCrawl(Crawl crawl) {
    try {
      crawlDao.createOrUpdate(crawl);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void deleteCrawl(Long crawlId) {
    try {
      crawlDao.deleteById(crawlId);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void crawlingStarted(Crawl crawl) {
    crawl.setStatus(CrawlStatus.CRAWLING);
    crawl.setProgress(0);
    saveCrawl(crawl);
  }

  @Override
  public void onCrawlError(Crawl crawl, String msg) {
    crawl.setStatus(CrawlStatus.ERROR);
    saveCrawl(crawl);
  }

  @Override
  public void commandExecuted(Crawl crawl, RemoteCommand command, int progress) {
    crawl.setProgress(progress);
    saveCrawl(crawl);
  }

  @Override
  public void crawlingFinished(Crawl crawl) {
    crawl.setStatus(CrawlStatus.FINISHED);
    saveCrawl(crawl);
  }
}
