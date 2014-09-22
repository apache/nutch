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
package org.apache.nutch.webui.pages;

import org.apache.nutch.webui.client.model.NutchStatus;
import org.apache.nutch.webui.model.NutchInstance;
import org.apache.nutch.webui.pages.instances.InstancesPage;
import org.apache.nutch.webui.service.NutchService;
import org.apache.wicket.ajax.AjaxSelfUpdatingTimerBehavior;
import org.apache.wicket.markup.html.WebMarkupContainer;
import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.link.BookmarkablePageLink;
import org.apache.wicket.model.LoadableDetachableModel;
import org.apache.wicket.spring.injection.annot.SpringBean;
import org.apache.wicket.util.time.Duration;

public class DashboardPage extends AbstractBasePage<Object> {
  @SpringBean
  private NutchService nutchService;

  private WebMarkupContainer panel;

  public DashboardPage() {
    panel = new WebMarkupContainer("panel");
    panel.setOutputMarkupId(true);
    panel.add(new AjaxSelfUpdatingTimerBehavior(Duration.ONE_SECOND));
    panel.add(new Label("jobsRunning", new JobsModel()));
    add(panel);
    add(new BookmarkablePageLink<Void>("viewInstances", InstancesPage.class));

  }

  private class JobsModel extends LoadableDetachableModel<Integer> {
    @Override
    protected Integer load() {
      NutchInstance currentInstance = getCurrentInstance();
      Long id = currentInstance.getId();
      NutchStatus nutchStatus = nutchService.getNutchStatus(id);
      return nutchStatus.getRunningJobs().size();
    }
  }
}
