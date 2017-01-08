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
package org.apache.nutch.webui.pages.auth;

import org.apache.wicket.markup.html.WebPage;
import org.apache.wicket.markup.html.form.Form;
import org.apache.wicket.markup.html.form.PasswordTextField;
import org.apache.wicket.markup.html.form.TextField;
import org.apache.wicket.markup.html.panel.FeedbackPanel;
import org.apache.wicket.model.PropertyModel;

/**
 * Sign in page implementation.
 */
public class SignInPage extends WebPage {

  private User user = new User();

  public SignInPage() {
    add(new LoginForm("form"));
    add(new FeedbackPanel("feedback"));
  }

  private class LoginForm extends Form<Void> {

    public LoginForm(String id) {
      super(id);

      add(new TextField<String>("username", new PropertyModel<String>(
              user, "username")));
      add(new PasswordTextField("password", new PropertyModel<String>(
              user, "password")));
    }

    @Override
    protected void onSubmit() {
      ((SignInSession) getSession()).signIn(user.getUsername(), user.getPassword());

      if (((SignInSession) getSession()).getUser() != null) {
        continueToOriginalDestination();
        setResponsePage(getApplication().getHomePage());
      } else
        // Pass error message to feedback panel
        error("Invalid username or password");
    }

  }

  public User getUser() {
    return user;
  }

  public void setUser(User user) {
    this.user = user;
  }
}