/*
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
package org.apache.nutch.webui.pages.settings;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.nutch.webui.model.NutchConfig;
import org.apache.nutch.webui.pages.AbstractBasePage;
import org.apache.nutch.webui.pages.components.CpmIteratorAdapter;
import org.apache.nutch.webui.service.NutchService;
import org.apache.wicket.markup.html.WebMarkupContainer;
import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.form.TextField;
import org.apache.wicket.markup.repeater.Item;
import org.apache.wicket.markup.repeater.RefreshingView;
import org.apache.wicket.model.IModel;
import org.apache.wicket.spring.injection.annot.SpringBean;

public class SettingsPage extends AbstractBasePage<Void> {
  @SpringBean
  private NutchService nutchService;

  private WebMarkupContainer settingsTable;

  public SettingsPage() {
    settingsTable = new WebMarkupContainer("settingsTable");
    settingsTable.setOutputMarkupId(true);
    RefreshingView<NutchConfig> nutchConfig = new RefreshingView<NutchConfig>(
        "settings") {

      @Override
      protected Iterator<IModel<NutchConfig>> getItemModels() {
        return new CpmIteratorAdapter<>(
            convertNutchConfig(nutchService.getNutchConfig(getCurrentInstance()
                .getId())));
      }

      @Override
      protected void populateItem(Item<NutchConfig> item) {
        item.add(new Label("name"));
        item.add(new TextField<String>("value"));
      }
    };
    settingsTable.add(nutchConfig);
    add(settingsTable);
  }

  private List<NutchConfig> convertNutchConfig(Map<String, String> map) {
    List<NutchConfig> listNutchConfigs = new LinkedList<>();
    for (String key : map.keySet()) {
      NutchConfig conf = new NutchConfig();
      conf.setName(key);
      conf.setValue(map.get(key));
      listNutchConfigs.add(conf);
    }
    return listNutchConfigs;
  }
}
