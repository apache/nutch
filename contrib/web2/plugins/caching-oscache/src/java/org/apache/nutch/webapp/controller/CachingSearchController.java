package org.apache.nutch.webapp.controller;

import java.io.IOException;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.nutch.webapp.CacheManager;
import org.apache.nutch.webapp.common.Search;
import org.apache.nutch.webapp.common.ServiceLocator;
import org.apache.struts.tiles.ComponentContext;

import com.opensymphony.oscache.base.NeedsRefreshException;

/**
 * This naive search result caching implementation is just an example of
 * extending the web ui.
 */
public class CachingSearchController extends SearchController {

  public void nutchPerform(ComponentContext tileContext,
      HttpServletRequest request, HttpServletResponse response,
      ServletContext servletContext) throws ServletException, IOException {

    Search search = null;
    boolean requiresUpdate = false;

    // key used for caching
    String key = request.getQueryString();

    ServiceLocator locator = getServiceLocator(request);

    try {
      search = CacheManager.getInstance(locator.getConfiguration()).getSearch(
          key);
      request.setAttribute("resultInfo", search.getResultInfo());
      request.setAttribute("nutchSearch", search);

      LOG.fine("Using cached");
    } catch (NeedsRefreshException e) {
      requiresUpdate = true;
      LOG.fine("Cache update required");
    }

    if (search == null || requiresUpdate) {
      LOG.fine("Cache miss");
      super.nutchPerform(tileContext, request, response, servletContext);
      search = (Search) request.getAttribute(SearchController.REQ_ATTR_SEARCH);
      CacheManager.getInstance(locator.getConfiguration()).putSearch(key,
          search);
    }

  }
}
