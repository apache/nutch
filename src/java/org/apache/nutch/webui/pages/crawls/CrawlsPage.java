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
package org.apache.nutch.webui.pages.crawls;

import static de.agilecoders.wicket.core.markup.html.bootstrap.block.LabelType.Danger;
import static de.agilecoders.wicket.core.markup.html.bootstrap.block.LabelType.Default;
import static de.agilecoders.wicket.core.markup.html.bootstrap.block.LabelType.Info;
import static de.agilecoders.wicket.core.markup.html.bootstrap.block.LabelType.Success;
import static org.apache.nutch.webui.client.model.Crawl.CrawlStatus.CRAWLING;
import static org.apache.nutch.webui.client.model.Crawl.CrawlStatus.ERROR;
import static org.apache.nutch.webui.client.model.Crawl.CrawlStatus.FINISHED;
import static org.apache.nutch.webui.client.model.Crawl.CrawlStatus.NEW;

import java.util.Iterator;

import org.apache.nutch.webui.client.model.Crawl;
import org.apache.nutch.webui.client.model.Crawl.CrawlStatus;
import org.apache.nutch.webui.pages.AbstractBasePage;
import org.apache.nutch.webui.pages.components.ColorEnumLabelBuilder;
import org.apache.nutch.webui.pages.components.CpmIteratorAdapter;
import org.apache.nutch.webui.service.CrawlService;
import org.apache.wicket.ajax.AjaxRequestTarget;
import org.apache.wicket.ajax.AjaxSelfUpdatingTimerBehavior;
import org.apache.wicket.ajax.markup.html.AjaxLink;
import org.apache.wicket.markup.html.WebMarkupContainer;
import org.apache.wicket.markup.html.basic.EnumLabel;
import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.link.Link;
import org.apache.wicket.markup.repeater.Item;
import org.apache.wicket.markup.repeater.RefreshingView;
import org.apache.wicket.model.CompoundPropertyModel;
import org.apache.wicket.model.IModel;
import org.apache.wicket.spring.injection.annot.SpringBean;
import org.apache.wicket.util.time.Duration;

/**
 * This page is for crawls management
 * 
 * @author feodor
 * 
 */
public class CrawlsPage extends AbstractBasePage<Void> {

  private static final Duration UPDATE_TIMEOUT = Duration.seconds(2);

  @SpringBean
  private CrawlService crawlService;

  private WebMarkupContainer crawlsTable;
  private CrawlPanel crawlPanel;

  public CrawlsPage() {
    crawlsTable = new WebMarkupContainer("crawlsTable");
    crawlsTable.setOutputMarkupId(true);
    crawlsTable.add(new AjaxSelfUpdatingTimerBehavior(UPDATE_TIMEOUT));

    RefreshingView<Crawl> crawls = new RefreshingView<Crawl>("crawls") {

      @Override
      protected Iterator<IModel<Crawl>> getItemModels() {
        return new CpmIteratorAdapter<Crawl>(crawlService.getCrawls());
      }

      @Override
      protected void populateItem(Item<Crawl> item) {
        populateCrawlRow(item);
      }
    };

    crawlsTable.add(crawls);
    add(crawlsTable);

    crawlPanel = new CrawlPanel("crawl");
    add(crawlPanel);

    add(new AjaxLink<Crawl>("newCrawl") {
      @Override
      public void onClick(AjaxRequestTarget target) {
        editCrawl(target, new CompoundPropertyModel<Crawl>(createNewCrawl()));
      }
    });
  }

  private void populateCrawlRow(Item<Crawl> item) {
    item.add(new AjaxLink<Crawl>("edit", item.getModel()) {
      @Override
      public void onClick(AjaxRequestTarget target) {
        editCrawl(target, getModel());
      }
    }.add(new Label("crawlName")));
    item.add(new Label("seedList.name"));

    item.add(new Label("progress"));
    item.add(createStatusLabel());
    item.add(new Link<Crawl>("start", item.getModel()) {
      @Override
      public void onClick() {
        crawlService.startCrawl(getModelObject().getId(), getCurrentInstance());
      }
    });

    item.add(new Link<Crawl>("delete", item.getModel()) {
      @Override
      public void onClick() {
        crawlService.deleteCrawl(getModelObject().getId());
      }
    });
  }

  private void editCrawl(AjaxRequestTarget target, IModel<Crawl> model) {
    crawlPanel.setModel(model);
    target.add(crawlPanel);
    crawlPanel.appendShowDialogJavaScript(target);
  }

  private Crawl createNewCrawl() {
    return new Crawl();
  }

  private EnumLabel<CrawlStatus> createStatusLabel() {
    return new ColorEnumLabelBuilder<CrawlStatus>("status")
        .withEnumColor(NEW, Default).withEnumColor(ERROR, Danger)
        .withEnumColor(FINISHED, Success).withEnumColor(CRAWLING, Info).build();
  }
}
