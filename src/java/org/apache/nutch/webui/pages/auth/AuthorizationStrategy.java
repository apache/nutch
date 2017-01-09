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

import org.apache.wicket.Component;
import org.apache.wicket.RestartResponseAtInterceptPageException;
import org.apache.wicket.Session;
import org.apache.wicket.authorization.Action;
import org.apache.wicket.authorization.IAuthorizationStrategy;

/**
 * Authorization strategy to check whether to allow a page or not.
 */
public class AuthorizationStrategy implements IAuthorizationStrategy {
  @Override
  public boolean isInstantiationAuthorized(Class componentClass) {

    // Check if it needs authentication or not
    if (!AuthenticatedWebPage.class.isAssignableFrom(componentClass)) {
      return true;
    }

    // Allow if user signed in
    if (((SignInSession) Session.get()).isSignedIn()) {
      return true;
    }

    // Redirect to sign in page due to page requires authentication and user not signed in
    throw new RestartResponseAtInterceptPageException(SignInPage.class);

  }

  @Override
  public boolean isActionAuthorized(Component component, Action action) {
    // Authorize all actions
    return true;
  }
}