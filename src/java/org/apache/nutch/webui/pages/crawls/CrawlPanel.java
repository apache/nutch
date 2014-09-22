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

import java.util.List;

import org.apache.nutch.webui.client.model.Crawl;
import org.apache.nutch.webui.model.SeedList;
import org.apache.nutch.webui.service.CrawlService;
import org.apache.nutch.webui.service.SeedListService;
import org.apache.wicket.ajax.AjaxRequestTarget;
import org.apache.wicket.ajax.markup.html.form.AjaxSubmitLink;
import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.form.ChoiceRenderer;
import org.apache.wicket.markup.html.form.DropDownChoice;
import org.apache.wicket.markup.html.form.Form;
import org.apache.wicket.markup.html.form.TextField;
import org.apache.wicket.model.IModel;
import org.apache.wicket.model.Model;
import org.apache.wicket.spring.injection.annot.SpringBean;

import com.google.common.collect.Lists;

import de.agilecoders.wicket.core.markup.html.bootstrap.common.NotificationPanel;
import de.agilecoders.wicket.core.markup.html.bootstrap.dialog.Modal;
import de.agilecoders.wicket.core.markup.html.bootstrap.form.BootstrapForm;

public class CrawlPanel extends Modal {
  private static final int MAX_ROUNDS = 10;

  private BootstrapForm<Crawl> form;

  @SpringBean
  private CrawlService crawlService;

  @SpringBean
  private SeedListService seedListService;

  private NotificationPanel notificationPanel;

  public CrawlPanel(String markupId) {
    super(markupId);
    header(Model.of("Crawl"));

    notificationPanel = new NotificationPanel("notificationPanel");
    notificationPanel.setOutputMarkupId(true);
    add(notificationPanel);

    form = new BootstrapForm<Crawl>("crawlForm");
    form.add(new Label("crawlId"));
    form.add(new TextField<String>("crawlName").setRequired(true));

    form.add(new DropDownChoice<Integer>("numberOfRounds", getNumbersOfRounds()));
    form.add(new DropDownChoice<SeedList>("seedList", seedListService.findAll(),
        new ChoiceRenderer<SeedList>("name")).setRequired(true));

    addButton(new AjaxSubmitLink("button", form) {
      @Override
      protected void onSubmit(AjaxRequestTarget target, Form<?> ajaxForm) {
        crawlService.saveCrawl(form.getModelObject());
        target.add(this.getPage());
      }

      protected void onError(AjaxRequestTarget target, Form<?> form) {
        target.add(notificationPanel);
      };
    }.setBody(Model.of("Save")));
    add(form);
  }

  public void setModel(IModel<Crawl> model) {
    form.setModel(model);
  }

  private List<Integer> getNumbersOfRounds() {
    List<Integer> numbers = Lists.newArrayList();
    for (int i = 1; i <= MAX_ROUNDS; i++) {
      numbers.add(i);
    }
    return numbers;
  }

}
