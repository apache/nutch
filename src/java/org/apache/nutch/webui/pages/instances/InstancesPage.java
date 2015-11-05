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
package org.apache.nutch.webui.pages.instances;

import static de.agilecoders.wicket.core.markup.html.bootstrap.block.LabelType.Danger;
import static de.agilecoders.wicket.core.markup.html.bootstrap.block.LabelType.Info;
import static de.agilecoders.wicket.core.markup.html.bootstrap.block.LabelType.Success;
import static org.apache.nutch.webui.client.model.ConnectionStatus.CONNECTED;
import static org.apache.nutch.webui.client.model.ConnectionStatus.CONNECTING;
import static org.apache.nutch.webui.client.model.ConnectionStatus.DISCONNECTED;

import java.util.Iterator;

import org.apache.nutch.webui.client.model.ConnectionStatus;
import org.apache.nutch.webui.model.NutchInstance;
import org.apache.nutch.webui.pages.AbstractBasePage;
import org.apache.nutch.webui.pages.components.ColorEnumLabel;
import org.apache.nutch.webui.pages.components.ColorEnumLabelBuilder;
import org.apache.nutch.webui.pages.components.CpmIteratorAdapter;
import org.apache.nutch.webui.service.NutchInstanceService;
import org.apache.wicket.ajax.AjaxRequestTarget;
import org.apache.wicket.ajax.AjaxSelfUpdatingTimerBehavior;
import org.apache.wicket.ajax.markup.html.AjaxLink;
import org.apache.wicket.markup.html.WebMarkupContainer;
import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.repeater.Item;
import org.apache.wicket.markup.repeater.RefreshingView;
import org.apache.wicket.model.CompoundPropertyModel;
import org.apache.wicket.model.IModel;
import org.apache.wicket.spring.injection.annot.SpringBean;
import org.apache.wicket.util.time.Duration;

public class InstancesPage extends AbstractBasePage<Void> {
  @SpringBean
  private NutchInstanceService instanceService;

  private InstancePanel instancePanel;

  private WebMarkupContainer instancesTable;
  private static final Duration UPDATE_TIMEOUT = Duration.seconds(1);

  public InstancesPage() {

    instancesTable = new WebMarkupContainer("instancesTable");
    instancesTable.setOutputMarkupId(true);
    instancesTable.add(new AjaxSelfUpdatingTimerBehavior(UPDATE_TIMEOUT));

    instancePanel = new InstancePanel("instanceForm");

    RefreshingView<NutchInstance> instances = refreshingView();
    instancesTable.add(instances);
    add(instancesTable);
    add(instancePanel);
    add(addInstanceButton());
  }

  private RefreshingView<NutchInstance> refreshingView() {
    RefreshingView<NutchInstance> instances = new RefreshingView<NutchInstance>(
        "instances") {

      @Override
      protected Iterator<IModel<NutchInstance>> getItemModels() {
        return new CpmIteratorAdapter<NutchInstance>(
            instanceService.getInstances());
      }

      @Override
      protected void populateItem(Item<NutchInstance> item) {
        populateInstanceRow(item);
      }
    };
    return instances;
  }

  private AjaxLink<NutchInstance> addInstanceButton() {
    return new AjaxLink<NutchInstance>("addInstance") {
      @Override
      public void onClick(AjaxRequestTarget target) {
        instancePanel.setModel(new CompoundPropertyModel<NutchInstance>(
            new NutchInstance()));
        target.add(instancePanel);
        instancePanel.appendShowDialogJavaScript(target);
      }
    };
  }

  private void populateInstanceRow(final Item<NutchInstance> item) {
    item.add(new AjaxLink<NutchInstance>("editInstance") {
      @Override
      public void onClick(AjaxRequestTarget target) {
        instancePanel.setModel(item.getModel());
        target.add(instancePanel);
        instancePanel.appendShowDialogJavaScript(target);
      }
    }.add(new Label("name")));
    item.add(new Label("host"));
    item.add(new Label("username"));
    item.add(createStatusLabel());
    item.add(new AjaxLink<NutchInstance>("instanceDelete", item.getModel()) {
      @Override
      public void onClick(AjaxRequestTarget target) {
        instanceService.removeInstance(getModelObject().getId());
        target.add(instancesTable);
      }
    });
  }

  private ColorEnumLabel<ConnectionStatus> createStatusLabel() {
    return new ColorEnumLabelBuilder<ConnectionStatus>("connectionStatus")
        .withEnumColor(CONNECTED, Success).withEnumColor(CONNECTING, Info)
        .withEnumColor(DISCONNECTED, Danger).build();
  }
}
