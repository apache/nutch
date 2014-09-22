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
package org.apache.nutch.webui.pages.seed;

import java.util.Iterator;

import org.apache.nutch.webui.model.SeedList;
import org.apache.nutch.webui.model.SeedUrl;
import org.apache.nutch.webui.pages.AbstractBasePage;
import org.apache.nutch.webui.pages.components.CpmIteratorAdapter;
import org.apache.nutch.webui.service.SeedListService;
import org.apache.wicket.ajax.AjaxRequestTarget;
import org.apache.wicket.ajax.markup.html.AjaxLink;
import org.apache.wicket.ajax.markup.html.form.AjaxSubmitLink;
import org.apache.wicket.markup.html.WebMarkupContainer;
import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.form.Form;
import org.apache.wicket.markup.html.form.TextField;
import org.apache.wicket.markup.repeater.Item;
import org.apache.wicket.markup.repeater.RefreshingView;
import org.apache.wicket.model.CompoundPropertyModel;
import org.apache.wicket.model.IModel;
import org.apache.wicket.model.LoadableDetachableModel;
import org.apache.wicket.model.Model;
import org.apache.wicket.request.mapper.parameter.PageParameters;
import org.apache.wicket.spring.injection.annot.SpringBean;

import com.google.common.collect.Lists;

/**
 * This page is for seed urls management
 * 
 * @author feodor
 * 
 */
public class SeedPage extends AbstractBasePage<SeedList> {

  @SpringBean
  private SeedListService seedListService;

  private Form<SeedUrl> urlForm;

  private WebMarkupContainer seedUrlsTable;

  public SeedPage() {
    SeedList list = new SeedList();
    list.setSeedUrls(Lists.<SeedUrl> newArrayList());
    initPage(Model.of(list));
  }

  public SeedPage(final PageParameters parameters) {
    initPage(new LoadableDetachableModel<SeedList>() {

      @Override
      protected SeedList load() {
        Long seedListId = parameters.get("id").toLongObject();
        return seedListService.getSeedList(seedListId);
      }
    });
  }

  public void initPage(IModel<SeedList> model) {
    setModel(new CompoundPropertyModel<SeedList>(model));

    addBaseForm();
    addSeedUrlsList();
    addUrlForm();
  }

  private void addBaseForm() {
    Form<SeedList> form = new Form<SeedList>("seedList", getModel()) {
      @Override
      protected void onSubmit() {
        seedListService.save(getModelObject());
        setResponsePage(SeedListsPage.class);
      }
    };
    form.add(new TextField<String>("name"));
    add(form);
  }

  private void addSeedUrlsList() {
    seedUrlsTable = new WebMarkupContainer("seedUrlsTable");
    seedUrlsTable.setOutputMarkupId(true);

    RefreshingView<SeedUrl> seedUrls = new RefreshingView<SeedUrl>("seedUrls") {

      @Override
      protected Iterator<IModel<SeedUrl>> getItemModels() {
        return new CpmIteratorAdapter<SeedUrl>(getModelObject().getSeedUrls());
      }

      @Override
      protected void populateItem(Item<SeedUrl> item) {
        item.add(new Label("url"));
        item.add(new AjaxLink<SeedUrl>("delete", item.getModel()) {

          @Override
          public void onClick(AjaxRequestTarget target) {
            deleteSeedUrl(getModelObject());
            target.add(seedUrlsTable);
          }
        });
      }
    };
    seedUrlsTable.add(seedUrls);
    add(seedUrlsTable);
  }

  private void addUrlForm() {
    urlForm = new Form<SeedUrl>("urlForm", CompoundPropertyModel.of(Model.of(new SeedUrl())));
    urlForm.setOutputMarkupId(true);
    urlForm.add(new TextField<String>("url"));
    urlForm.add(new AjaxSubmitLink("addUrl", urlForm) {
      @Override
      protected void onSubmit(AjaxRequestTarget target, Form<?> form) {
        addSeedUrl();
        urlForm.setModelObject(new SeedUrl());
        target.add(urlForm);
        target.add(seedUrlsTable);
      }
    });
    add(urlForm);
  }

  private void addSeedUrl() {
    SeedUrl url = urlForm.getModelObject();
    SeedList seedList = getModelObject();
    url.setSeedList(seedList);
    seedList.getSeedUrls().add(url);
  }

  private void deleteSeedUrl(SeedUrl url) {
    SeedList seedList = getModelObject();
    seedList.getSeedUrls().remove(url);
  }

}
