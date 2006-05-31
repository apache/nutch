package org.apache.nutch.webapp.common;

import javax.servlet.ServletContext;

/** Controllers wishing to be initialized must extend this interface
 * 
 */
public interface Startable {
  public void start(ServletContext servletContext);
}
