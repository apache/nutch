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
package org.apache.nutch.webui.model;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;

import org.apache.nutch.webui.client.model.ConnectionStatus;

@Entity
public class NutchInstance implements Serializable {

  @Id
  @GeneratedValue
  private Long id;

  @Column
  private String name = "localhost";

  @Column
  private String host = "localhost";

  @Column
  private Integer port = 8081;

  @Column
  private String username;

  @Column
  private String password;

  private ConnectionStatus connectionStatus;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getHost() {
    return host;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public String getUsername() {
    return username;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public Integer getPort() {
    return port;
  }

  public void setPort(Integer port) {
    this.port = port;
  }

  public ConnectionStatus getConnectionStatus() {
    return connectionStatus;
  }

  public void setConnectionStatus(ConnectionStatus connectionStatus) {
    this.connectionStatus = connectionStatus;
  }

  public URI getUrl() {
    try {
      return new URI("http", null, host, port, null, null, null);
    } catch (URISyntaxException e) {
      throw new IllegalStateException("Cannot parse url parameters", e);
    }
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

}
