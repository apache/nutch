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
