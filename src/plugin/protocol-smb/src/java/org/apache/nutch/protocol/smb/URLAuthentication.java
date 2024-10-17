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
package org.apache.nutch.protocol.smb;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

public class URLAuthentication {
    protected static final Logger LOG = LoggerFactory.getLogger(URLAuthentication.class);

    public static class Authentication {
        protected static final Logger LOG = LoggerFactory.getLogger(Authentication.class);

        private Pattern pattern;
        private String user;
        private String domain;
        private char[] password;

        protected Authentication(String pattern, String user, String domain, char[] password) {
            LOG.debug("Authentication({}, {}, {}, *****)", pattern, user, domain);
            if (pattern == null || pattern.isEmpty()) {
                throw new IllegalArgumentException("pattern must not be null");
            }
            if (user == null || user.isEmpty()) {
                throw new IllegalArgumentException("user must not be null");
            }
            if (password == null) {
                throw new IllegalArgumentException("password must not be null");
            }
            this.pattern = Pattern.compile(pattern);
            this.user = user;
            this.domain = domain;
            this.password = password;
        }

        public boolean matches(String url) {
            LOG.debug("matches({})", url);
            return pattern.matcher(url).matches();
        }

        protected Pattern getPattern() {
            return pattern;
        }

        public String getUser() {
            return user;
        }

        public char[] getPassword() {
            return password;
        }

        public String getDomain() {
            return domain;
        }
    }

    private List<Authentication> authentications;
    
    public static URLAuthentication loadAuthentication(InputSource inputSource) {
        LOG.debug("loadAuthentication(...)");

        URLAuthentication result = new URLAuthentication();

        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document document = builder.parse(inputSource);
            Element rootElement = document.getDocumentElement();
            NodeList authList = rootElement.getElementsByTagName("authentication");
            for (int i = 0; i<authList.getLength(); i++) {
                Element auth = (Element)authList.item(i);
                String pattern = auth.getAttribute("pattern");
                String user = auth.getAttribute("user");
                String domain = auth.getAttribute("domain");
                String password = auth.getAttribute("password");
                result.addAuthentication(new Authentication(pattern, user, domain, password.toCharArray()));
            }
        } catch (Exception e) {
            LOG.error("Could not load authentication data", e);
            return null;
        }

        return result;
    }

    private URLAuthentication() {
        LOG.debug("URLAuthentication()");
        this.authentications = new ArrayList<>();
    }

    private void addAuthentication(Authentication auth) {
        LOG.debug("addAuthentication({})", auth);
        authentications.add(auth);
    }

    public Authentication getAuthenticationFor(String url) {
        LOG.debug("getAuthenticationFor({})", url);

        for (Authentication auth: authentications) {
            if (auth.matches(url)) {
                LOG.trace("matched pattern {}", auth.getPattern());
                return auth;
            } else {
                LOG.trace("missed pattern {}", auth.getPattern());
            }
        }

        LOG.trace("Nothing found in {} entries", authentications.size());
        return null;
    }

}
