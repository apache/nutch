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
package org.apache.nutch.webui.client.model;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;

import org.apache.nutch.webui.model.SeedList;

import com.j256.ormlite.field.DatabaseField;

@Entity
public class Crawl implements Serializable {
  public enum CrawlStatus {
    NEW, CRAWLING, FINISHED, ERROR
  }

  @Id
  @GeneratedValue
  private Long id;

  @Column
  private String crawlId;

  @Column
  private String crawlName;

  @Column
  private CrawlStatus status = CrawlStatus.NEW;

  @Column
  private Integer numberOfRounds = 1;

  @Column
  @DatabaseField(foreign = true, foreignAutoRefresh = true)
  private SeedList seedList;

  @Column
  private String seedDirectory;

  @Column
  private int progress;

  public Integer getNumberOfRounds() {
    return numberOfRounds;
  }

  public void setNumberOfRounds(Integer numberOfRounds) {
    this.numberOfRounds = numberOfRounds;
  }

  public String getCrawlId() {
    return crawlId;
  }

  public void setCrawlId(String crawlId) {
    this.crawlId = crawlId;
  }

  public CrawlStatus getStatus() {
    return status;
  }

  public void setStatus(CrawlStatus status) {
    this.status = status;
  }

  public String getCrawlName() {
    return crawlName;
  }

  public void setCrawlName(String crawlName) {
    this.crawlName = crawlName;
  }

  public SeedList getSeedList() {
    return seedList;
  }

  public void setSeedList(SeedList seedList) {
    this.seedList = seedList;
  }

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public String getSeedDirectory() {
    return seedDirectory;
  }

  public void setSeedDirectory(String seedDirectory) {
    this.seedDirectory = seedDirectory;
  }

  public int getProgress() {
    return progress;
  }

  public void setProgress(int progress) {
    this.progress = progress;
  }

}
