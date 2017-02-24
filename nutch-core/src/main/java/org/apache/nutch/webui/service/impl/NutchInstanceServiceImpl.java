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

import org.apache.nutch.webui.client.NutchClientFactory;
import org.apache.nutch.webui.model.NutchInstance;
import org.apache.nutch.webui.service.NutchInstanceService;
import org.springframework.stereotype.Service;

import com.j256.ormlite.dao.Dao;

@Service
public class NutchInstanceServiceImpl implements NutchInstanceService {

  @Resource
  private NutchClientFactory nutchClientFactory;

  @Resource
  private Dao<NutchInstance, Long> instancesDao;

  @Override
  public List<NutchInstance> getInstances() {
    try {
      return instancesDao.queryForAll();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }

  }

  @Override
  public NutchInstance getInstance(Long id) {
    try {
      return instancesDao.queryForId(id);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void saveInstance(NutchInstance instance) {
    try {
      instancesDao.createOrUpdate(instance);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void removeInstance(Long id) {
    try {
      instancesDao.deleteById(id);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}
