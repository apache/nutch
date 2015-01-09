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
import org.apache.nutch.webui.pages.AbstractBasePage;
import org.apache.nutch.webui.pages.components.CpmIteratorAdapter;
import org.apache.nutch.webui.service.SeedListService;
import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.link.BookmarkablePageLink;
import org.apache.wicket.markup.html.link.Link;
import org.apache.wicket.markup.repeater.Item;
import org.apache.wicket.markup.repeater.RefreshingView;
import org.apache.wicket.model.IModel;
import org.apache.wicket.request.mapper.parameter.PageParameters;
import org.apache.wicket.spring.injection.annot.SpringBean;

/**
 * This page is for seed lists management
 * 
 * @author feodor
 * 
 */
public class SeedListsPage extends AbstractBasePage<Void> {

  @SpringBean
  private SeedListService seedListService;

  public SeedListsPage() {

    RefreshingView<SeedList> seedLists = new RefreshingView<SeedList>(
        "seedLists") {

      @Override
      protected Iterator<IModel<SeedList>> getItemModels() {
        return new CpmIteratorAdapter<SeedList>(seedListService.findAll());
      }

      @Override
      protected void populateItem(final Item<SeedList> item) {
        PageParameters params = new PageParameters();
        params.add("id", item.getModelObject().getId());

        Link<Void> edit = new BookmarkablePageLink<Void>("edit",
            SeedPage.class, params);
        edit.add(new Label("name"));
        item.add(edit);

        item.add(new Label("seedUrlsCount"));

        item.add(new Link<SeedList>("delete", item.getModel()) {
          @Override
          public void onClick() {
            seedListService.delete(item.getModelObject().getId());
          }
        });
      }
    };

    add(seedLists);
    add(new BookmarkablePageLink<Void>("newSeedList", SeedPage.class));
  }
}
