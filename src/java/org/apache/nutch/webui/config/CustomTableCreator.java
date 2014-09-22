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
package org.apache.nutch.webui.config;

import java.sql.SQLException;
import java.util.List;

import com.j256.ormlite.dao.BaseDaoImpl;
import com.j256.ormlite.dao.Dao;
import com.j256.ormlite.support.ConnectionSource;
import com.j256.ormlite.table.DatabaseTableConfig;
import com.j256.ormlite.table.TableUtils;

public class CustomTableCreator {

  private ConnectionSource connectionSource;
  private List<Dao<?, ?>> configuredDaos;

  public CustomTableCreator(ConnectionSource connectionSource, List<Dao<?, ?>> configuredDaos) {
    this.connectionSource = connectionSource;
    this.configuredDaos = configuredDaos;
    initialize();
  }

  private void initialize() {
    if (configuredDaos == null) {
      throw new IllegalStateException("configuredDaos was not set in " + getClass().getSimpleName());
    }

    for (Dao<?, ?> dao : configuredDaos) {
      createTableForDao(dao);
    }
  }

  private void createTableForDao(Dao<?, ?> dao) {
    DatabaseTableConfig<?> tableConfig = getTableConfig(dao);
    createTableIfNotExists(tableConfig);
  }

  private DatabaseTableConfig<?> getTableConfig(Dao<?, ?> dao) {
    Class<?> clazz = dao.getDataClass();
    DatabaseTableConfig<?> tableConfig = null;
    if (dao instanceof BaseDaoImpl) {
      tableConfig = ((BaseDaoImpl<?, ?>) dao).getTableConfig();
    }
    if (tableConfig == null) {
      return getConfigFromClass(clazz);
    }
    return tableConfig;
  }

  private DatabaseTableConfig<?> getConfigFromClass(Class<?> clazz) {
    try {
      return DatabaseTableConfig.fromClass(connectionSource, clazz);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private void createTableIfNotExists(DatabaseTableConfig<?> tableConfig) {
    try {
      TableUtils.createTableIfNotExists(connectionSource, tableConfig);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}
