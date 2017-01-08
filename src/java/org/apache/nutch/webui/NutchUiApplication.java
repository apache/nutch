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
package org.apache.nutch.webui;

import org.apache.nutch.api.ConfManager;
import org.apache.nutch.api.impl.RAMConfManager;
import org.apache.nutch.api.resources.ConfigResource;
import org.apache.nutch.webui.pages.DashboardPage;
import org.apache.nutch.webui.pages.assets.NutchUiCssReference;
import org.apache.nutch.webui.pages.auth.AuthorizationStrategy;
import org.apache.nutch.webui.pages.auth.SignInSession;
import org.apache.nutch.webui.pages.auth.User;
import org.apache.wicket.Session;
import org.apache.wicket.markup.html.WebPage;
import org.apache.wicket.protocol.http.WebApplication;
import org.apache.wicket.request.Request;
import org.apache.wicket.request.Response;
import org.apache.wicket.spring.injection.annot.SpringComponentInjector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

import de.agilecoders.wicket.core.Bootstrap;
import de.agilecoders.wicket.core.markup.html.themes.bootstrap.BootstrapCssReference;
import de.agilecoders.wicket.core.settings.BootstrapSettings;
import de.agilecoders.wicket.core.settings.SingleThemeProvider;
import de.agilecoders.wicket.core.settings.Theme;
import de.agilecoders.wicket.extensions.markup.html.bootstrap.icon.FontAwesomeCssReference;

import java.util.HashMap;
import java.util.Map;

@Component
public class NutchUiApplication extends WebApplication implements
    ApplicationContextAware {
  private static final String THEME_NAME = "bootstrap";
  private ApplicationContext context;

  private Map<String, User> userMap = new HashMap<>();
  private ConfManager configManager = new RAMConfManager();

  private static final Logger LOG = LoggerFactory.getLogger(NutchUiApplication.class);

  /**
   * @see org.apache.wicket.Application#getHomePage()
   */
  @Override
  public Class<? extends WebPage> getHomePage() {
    return DashboardPage.class;
  }

  /**
   * @see org.apache.wicket.Application#init()
   */
  @Override
  public void init() {
    super.init();
    initUsers();
    getSecuritySettings().setAuthorizationStrategy(new AuthorizationStrategy());

    BootstrapSettings settings = new BootstrapSettings();
    Bootstrap.install(this, settings);
    configureTheme(settings);

    getComponentInstantiationListeners().add(
        new SpringComponentInjector(this, context));
  }

  private void configureTheme(BootstrapSettings settings) {
    Theme theme = new Theme(THEME_NAME, BootstrapCssReference.instance(),
        FontAwesomeCssReference.instance(), NutchUiCssReference.instance());
    settings.setThemeProvider(new SingleThemeProvider(theme));
  }

  @Override
  public void setApplicationContext(ApplicationContext applicationContext)
      throws BeansException {
    this.context = applicationContext;
  }

  @Override
  public Session newSession(Request request, Response response) {
    super.newSession(request, response);
    return new SignInSession(request);
  }

  private void initUsers() {
    String[] users = configManager.get(ConfigResource.DEFAULT).getTrimmedStrings("webgui.auth.users", "admin|admin,user|user");

    for (String userDetailStr : users) {
      String[] userDetail = userDetailStr.split("\\|");
      if(userDetail.length != 2) {
        LOG.error("Check user definition of webgui.auth.users at nutch-site.xml");
        throw new IllegalStateException("Check user definition of webgui.auth.users at nutch-site.xml ");
      }

      User user = new User(userDetail[0], userDetail[1]);
      userMap.put(userDetail[0], user);
      LOG.info("User added: {}", userDetail[0]);
    }
  }

  public User getUser(String username, String password) {
    if (!userMap.containsKey(username)) {
      return null;
    }

    User user = userMap.get(username);
    if (!user.getPassword().equals(password)) {
      return null;
    }

    return user;
  }
}
