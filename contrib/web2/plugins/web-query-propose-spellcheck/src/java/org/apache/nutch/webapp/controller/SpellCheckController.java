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
package org.apache.nutch.webapp.controller;

import java.io.IOException;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.spell.SpellCheckerBean;
import org.apache.nutch.spell.SpellCheckerTerms;
import org.apache.nutch.webapp.common.SearchForm;
import org.apache.nutch.webapp.common.ServiceLocator;
import org.apache.nutch.webapp.common.Startable;
import org.apache.nutch.webapp.controller.NutchController;
import org.apache.struts.tiles.ComponentContext;

public class SpellCheckController extends NutchController implements Startable {

  SpellCheckerBean spellCheckerBean=null;
  
  public static final String ATTR_SPELL_TERMS="spellCheckerTerms";
  public static final String ATTR_SPELL_QUERY="spellCheckerQuery";
  
  public void nutchPerform(ComponentContext tileContext,
      HttpServletRequest request, HttpServletResponse response,
      ServletContext servletContext) throws ServletException, IOException {
    ServiceLocator serviceLocator=getServiceLocator(request);

    SpellCheckerTerms spellCheckerTerms = null;
    if (spellCheckerBean != null) {
            spellCheckerTerms = spellCheckerBean.checkSpelling(serviceLocator.getSearch().getQuery(), serviceLocator.getSearch().getQueryString());
    }

    SearchForm form=(SearchForm)serviceLocator.getSearchForm().clone();
    form.setValue(SearchForm.NAME_QUERYSTRING,spellCheckerTerms.getSpellCheckedQuery());
    String spellQuery = form.getParameterString("utf-8");
    
    request.setAttribute(ATTR_SPELL_TERMS, spellCheckerTerms);
    request.setAttribute(ATTR_SPELL_QUERY, spellQuery);
  }

  public void start(ServletContext servletContext) {
    Configuration conf=getServiceLocator(servletContext).getConfiguration();
    LOG.info("Initializing spellchecker");
    spellCheckerBean = SpellCheckerBean.get(conf);
  }
}
