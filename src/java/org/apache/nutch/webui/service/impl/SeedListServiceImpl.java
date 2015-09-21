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

import org.apache.nutch.webui.model.SeedList;
import org.apache.nutch.webui.model.SeedUrl;
import org.apache.nutch.webui.service.SeedListService;
import org.springframework.stereotype.Service;

import com.j256.ormlite.dao.Dao;

@Service
public class SeedListServiceImpl implements SeedListService {

  @Resource
  private Dao<SeedList, Long> seedListDao;

  @Resource
  private Dao<SeedUrl, Long> seedUrlDao;

  @Override
  public void save(SeedList seedList) {
    try {
      seedListDao.createOrUpdate(seedList);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void delete(Long seedListId) {
    try {
      seedListDao.deleteById(seedListId);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }

  }

  @Override
  public List<SeedList> findAll() {
    try {
      return seedListDao.queryForAll();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public SeedList getSeedList(Long seedListId) {
    try {
      return seedListDao.queryForId(seedListId);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

}
