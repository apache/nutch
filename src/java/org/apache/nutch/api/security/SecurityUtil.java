/*******************************************************************************
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
 ******************************************************************************/
package org.apache.nutch.api.security;

import org.apache.nutch.api.ConfManager;
import org.apache.nutch.api.resources.ConfigResource;
import org.restlet.ext.jaxrs.JaxRsApplication;
import org.restlet.security.MapVerifier;
import org.restlet.security.MemoryRealm;
import org.restlet.security.Role;
import org.restlet.security.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for security operations for NutchServer REST API.
 *
 */
public final class SecurityUtil {

  private static final Logger LOG = LoggerFactory.getLogger(SecurityUtil.class);

  /**
   * Private constructor to prevent instantiation
   */
  private SecurityUtil() {
  }

  /**
   * Returns roles defined at {@link org.apache.nutch.api.security.AuthorizationRoleEnum} associated with
   * {@link org.restlet.ext.jaxrs.JaxRsApplication} type application
   *
   * @param application {@link org.restlet.ext.jaxrs.JaxRsApplication} type application
   * @return roles associated with given {@link org.restlet.ext.jaxrs.JaxRsApplication} type application
   */
  public static List<Role> getRoles(JaxRsApplication application) {
    List<Role> roles = new ArrayList<>();
    for (AuthorizationRoleEnum authorizationRole : AuthorizationRoleEnum.values()) {
      roles.add(new Role(application, authorizationRole.toString()));
    }
    return roles;
  }

  /**
   * Constructs realm
   *
   * @param application {@link org.restlet.ext.jaxrs.JaxRsApplication }application
   * @param configManager {@link org.apache.nutch.api.ConfManager} type config manager
   * @return realm
   */
  public static MemoryRealm constructRealm(JaxRsApplication application, ConfManager configManager){
    MemoryRealm realm = new MemoryRealm();
    MapVerifier mapVerifier = new MapVerifier();
    String[] users = configManager.get(ConfigResource.DEFAULT).getTrimmedStrings("restapi.auth.users", "admin|admin|admin,user|user|user");
    if (users.length <= 1) {
      throw new IllegalStateException("Check users definition of restapi.auth.users at nutch-site.xml ");
    }
    for (String userconf : users) {
      String[] userDetail = userconf.split("\\|");
      if(userDetail.length != 3) {
        LOG.error("Check user definition of restapi.auth.users at nutch-site.xml");
        throw new IllegalStateException("Check user definition of restapi.auth.users at nutch-site.xml ");
      }
      User user = new User(userDetail[0], userDetail[1]);
      mapVerifier.getLocalSecrets().put(user.getIdentifier(), user.getSecret());
      realm.getUsers().add(user);
      realm.map(user, Role.get(application, userDetail[2]));
      LOG.info("User added: {}", userDetail[0]);
    }
      realm.setVerifier(mapVerifier);
      return realm;
  }

  /**
   * Check for allowing only admin role
   *
   * @param securityContext to check role of logged-in user
   */
  public static void allowOnlyAdmin(SecurityContext securityContext) {
    if (securityContext.getAuthenticationScheme() != null
        && !securityContext.isUserInRole(AuthorizationRoleEnum.ADMIN.toString())) {
      throw new WebApplicationException(Response.status(Response.Status.FORBIDDEN)
          .entity("User does not have required " + AuthorizationRoleEnum.ADMIN + " role!").build());
    }
  }

}
