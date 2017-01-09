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

import static de.agilecoders.wicket.core.markup.html.bootstrap.navbar.Navbar.ComponentPosition.LEFT;
import static de.agilecoders.wicket.core.markup.html.bootstrap.navbar.NavbarComponents.transform;

import java.util.List;

import org.apache.nutch.webui.model.NutchInstance;
import org.apache.nutch.webui.pages.crawls.CrawlsPage;
import org.apache.nutch.webui.pages.instances.InstancesPage;
import org.apache.nutch.webui.pages.menu.VerticalMenu;
import org.apache.nutch.webui.pages.seed.SeedListsPage;
import org.apache.nutch.webui.pages.settings.SettingsPage;
import org.apache.nutch.webui.service.NutchInstanceService;
import org.apache.nutch.webui.service.NutchService;
import org.apache.wicket.Component;
import org.apache.wicket.Page;
import org.apache.wicket.markup.html.GenericWebPage;
import org.apache.wicket.markup.html.link.AbstractLink;
import org.apache.wicket.markup.html.link.Link;
import org.apache.wicket.model.IModel;
import org.apache.wicket.model.LoadableDetachableModel;
import org.apache.wicket.model.Model;
import org.apache.wicket.model.PropertyModel;
import org.apache.wicket.model.ResourceModel;
import org.apache.wicket.spring.injection.annot.SpringBean;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import de.agilecoders.wicket.core.markup.html.bootstrap.button.dropdown.DropDownButton;
import de.agilecoders.wicket.core.markup.html.bootstrap.button.dropdown.MenuBookmarkablePageLink;
import de.agilecoders.wicket.core.markup.html.bootstrap.button.dropdown.MenuDivider;
import de.agilecoders.wicket.core.markup.html.bootstrap.common.NotificationPanel;
import de.agilecoders.wicket.core.markup.html.bootstrap.image.IconType;
import de.agilecoders.wicket.core.markup.html.bootstrap.navbar.Navbar.ComponentPosition;
import de.agilecoders.wicket.core.markup.html.bootstrap.navbar.Navbar.Position;
import de.agilecoders.wicket.core.markup.html.bootstrap.navbar.NavbarButton;
import de.agilecoders.wicket.core.markup.html.bootstrap.navbar.NavbarComponents;
import de.agilecoders.wicket.core.markup.html.bootstrap.navbar.NavbarDropDownButton;
import de.agilecoders.wicket.extensions.markup.html.bootstrap.icon.FontAwesomeIconType;

public abstract class AbstractBasePage<T> extends GenericWebPage<T> {
  @SpringBean
  private NutchService service;

  @SpringBean
  private NutchInstanceService instanceService;

  private VerticalMenu navbar;

  protected IModel<NutchInstance> currentInstance = new InstanceModel();

  public AbstractBasePage() {
    navbar = new VerticalMenu("navigation");
    navbar.brandName(Model.of("Apache Nutch GUI"));
    navbar.setInverted(true);
    navbar.setPosition(Position.TOP);
    add(navbar);

    addMenuItem(DashboardPage.class, "navbar.menu.dashboard",
        FontAwesomeIconType.dashboard);
    addMenuItem(StatisticsPage.class, "navbar.menu.statistics",
        FontAwesomeIconType.bar_chart_o);
    addMenuItem(InstancesPage.class, "navbar.menu.instances",
        FontAwesomeIconType.gears);
    addMenuItem(SettingsPage.class, "navbar.menu.settings",
        FontAwesomeIconType.wrench);
    addMenuItem(CrawlsPage.class, "navbar.menu.crawls",
        FontAwesomeIconType.refresh);
    addMenuItem(SchedulingPage.class, "navbar.menu.scheduling",
        FontAwesomeIconType.clock_o);
    addMenuItem(SearchPage.class, "navbar.menu.search",
        FontAwesomeIconType.search);
    addMenuItem(SeedListsPage.class, "navbar.menu.seedLists",
        FontAwesomeIconType.file);

    navbar.addComponents(transform(ComponentPosition.RIGHT,
        addInstancesMenuMenu()));
    navbar.addComponents(transform(ComponentPosition.RIGHT, addUserMenu()));

    add(new NotificationPanel("globalNotificationPanel"));

    if (currentInstance.getObject() == null && !(this instanceof InstancesPage)) {
      getSession().error("No running instances found!");
      setResponsePage(InstancesPage.class);
    }
  }

  protected Component addUserMenu() {
    DropDownButton userMenu = new NavbarDropDownButton(Model.of("Username")) {
      @Override
      protected List<AbstractLink> newSubMenuButtons(final String buttonMarkupId) {
        List<AbstractLink> subMenu = Lists.newArrayList();
        subMenu.add(new MenuBookmarkablePageLink<Void>(UserSettingsPage.class,
            new ResourceModel("navbar.userMenu.settings"))
            .setIconType(FontAwesomeIconType.gear));
        subMenu.add(new MenuDivider());
        subMenu.add(new MenuBookmarkablePageLink<Void>(LogOutPage.class,
            new ResourceModel("navbar.userMenu.logout"))
            .setIconType(FontAwesomeIconType.power_off));
        return subMenu;
      }
    }.setIconType(FontAwesomeIconType.user);
    return userMenu;
  }

  protected Component addInstancesMenuMenu() {
    IModel<String> instanceName = PropertyModel.of(currentInstance, "name");
    DropDownButton instancesMenu = new NavbarDropDownButton(instanceName) {

      @Override
      protected List<AbstractLink> newSubMenuButtons(String buttonMarkupId) {
        List<NutchInstance> instances = instanceService.getInstances();
        List<AbstractLink> subMenu = Lists.newArrayList();
        for (NutchInstance instance : instances) {
          subMenu.add(new Link<NutchInstance>(buttonMarkupId, Model
              .of(instance)) {
            @Override
            public void onClick() {
              currentInstance.setObject(getModelObject());
              setResponsePage(DashboardPage.class);
            }
          }.setBody(Model.of(instance.getName())));
        }
        return subMenu;
      }
    }.setIconType(FontAwesomeIconType.gears);

    return instancesMenu;
  }

  private <P extends Page> void addMenuItem(Class<P> page, String label,
      IconType icon) {
    Component button = new NavbarButton<Void>(page, Model.of(getString(label)))
        .setIconType(icon);
    navbar.addComponents(NavbarComponents.transform(LEFT, button));
  }

  protected NutchInstance getCurrentInstance() {
    return currentInstance.getObject();
  }

  private class InstanceModel extends LoadableDetachableModel<NutchInstance> {

    @Override
    public void setObject(NutchInstance instance) {
      super.setObject(instance);
      getSession().setAttribute("instanceId", instance.getId());
    }

    @Override
    protected NutchInstance load() {
      Long instanceId = (Long) getSession().getAttribute("instanceId");
      if (instanceId == null) {
        return getFirstInstance();
      }
      return instanceService.getInstance(instanceId);
    }

    private NutchInstance getFirstInstance() {
      return Iterables.getFirst(instanceService.getInstances(), null);
    }
  }
}
