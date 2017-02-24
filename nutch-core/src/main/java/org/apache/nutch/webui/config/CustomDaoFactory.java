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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.j256.ormlite.dao.Dao;
import com.j256.ormlite.spring.DaoFactory;
import com.j256.ormlite.support.ConnectionSource;

public class CustomDaoFactory {
  private ConnectionSource connectionSource;
  private List<Dao<?, ?>> registredDaos = Collections
      .synchronizedList(new ArrayList<Dao<?, ?>>());

  public CustomDaoFactory(ConnectionSource connectionSource) {
    this.connectionSource = connectionSource;
  }

  public <T, ID> Dao<T, ID> createDao(Class<T> clazz) {
    try {
      Dao<T, ID> dao = DaoFactory.createDao(connectionSource, clazz);
      register(dao);
      return dao;
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private <T, ID> void register(Dao<T, ID> dao) {
    synchronized (registredDaos) {
      registredDaos.add(dao);
    }
  }

  public List<Dao<?, ?>> getCreatedDaos() {
    synchronized (registredDaos) {
      return Collections.unmodifiableList(registredDaos);
    }
  }
}
