package org.apache.nutch.webapp.common;

import java.io.IOException;
import java.util.Locale;

import javax.servlet.ServletContext;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.plugin.PluginRepository;
import org.apache.nutch.searcher.NutchBean;
import org.apache.nutch.util.NutchConfiguration;

public abstract class ServletContextServiceLocator implements ServiceLocator {

  /**
   * ServiceLocator will be saved in ServletContext under this key
   */
  private static final String KEY = ServletContextServiceLocator.class
      .getName();

  /**
   * Logger
   */
  public static final Log LOG = LogFactory.getLog(ServletContextServiceLocator.class);

  /**
   * ServletContext this ServiceLocator is bound to
   */
  protected ServletContext servletContext;

  /**
   * Active configuration
   */
  protected Configuration config;

  /**
   * Active PluginRepository
   */
  private PluginRepository repository;

  /**
   * Active NutchBean
   */
  private NutchBean bean;

  /**
   * Private Constructor used to create new ServiceLocator when needed
   * 
   * @param servletContext
   */
  private ServletContextServiceLocator(ServletContext servletContext) {
    this.servletContext = servletContext;
    config = NutchConfiguration.get(servletContext);
    repository = PluginRepository.get(config);
    try {
      bean = new NutchBean(config);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Factory method to get handle to ServiceLocator, or if none exists in
   * ServletContext then one is created.
   * 
   * @param context
   * @return ServiceLocator instance bound to ServletContext
   */
  public static ServiceLocator getInstance(ServletContext context) {
    ServiceLocator locator = (ServiceLocator) context.getAttribute(KEY);
    if (locator == null) {
      LOG.info("creating new serviceLocator for context:" + context);
      locator = new ServletContextServiceLocator(context) {
        public SearchForm getSearchForm() {
          return null;
        }

        public Preferences getPreferences() {
          return null;
        }

        public Search getSearch() {
          return null;
        }

        public Locale getLocale() {
          return null;
        }
      };
      context.setAttribute(KEY, locator);
    }
    return locator;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.nutch.webapp.common.ServiceLocator#getConfiguration()
   */
  public Configuration getConfiguration() {
    return config;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.nutch.webapp.common.ServiceLocator#getPluginRepository()
   */public PluginRepository getPluginRepository() {
    return repository;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.nutch.webapp.common.ServiceLocator#getNutchBean()
   */
  public NutchBean getNutchBean() {
    return bean;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.nutch.webapp.common.ServiceLocator#getPluginResourceLoader(java.lang.ClassLoader)
   */
  public PluginResourceLoader getPluginResourceLoader(ClassLoader loader) {
    return PluginResourceLoader.getInstance(this, loader);
  }

}
