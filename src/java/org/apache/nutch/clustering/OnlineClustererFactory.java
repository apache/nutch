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

package org.apache.nutch.clustering;

// Commons Logging imports
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.nutch.plugin.*;
import org.apache.hadoop.conf.Configuration;

/**
 * A factory for retrieving {@link OnlineClusterer} extensions.
 *
 * @author Dawid Weiss
 * @version $Id: OnlineClustererFactory.java,v 1.2 2005/02/07 19:09:26 cutting Exp $
 */
public class OnlineClustererFactory {
  public static final Log LOG = LogFactory.getLog(OnlineClustererFactory.class);
  
  /**
   * Nutch configuration key specifying a particular clustering extension
   * to use. 
   */
  private final static String CONFIG_FIELD_NAME = "extension.clustering.extension-name";

  /**
   * An {@link ExtensionPoint} pointing to {@link OnlineClusterer}. 
   */
  private ExtensionPoint extensionPoint;
  
  /**
   * Default clustering extension implementation retrieved from the
   * configuration file or <code>null</code> if the default (first encountered extension)
   * is to be used.
   */
  private String extensionName;

  /**
   * Create an instance of the clustering factory bound to
   * a given configuration.
   */
  public OnlineClustererFactory(Configuration conf) {
      this.extensionPoint = PluginRepository.get(conf).getExtensionPoint(OnlineClusterer.X_POINT_ID);
      this.extensionName = conf.get(CONFIG_FIELD_NAME);
  }

  /**
  * @return Returns the online clustering extension specified
  * in nutch configuration (key name is <code>extension.clustering.extension-name</code>). 
  * If the name is empty (no preference), the first available clustering extension is
  * returned.
  */
  public OnlineClusterer getOnlineClusterer()
    throws PluginRuntimeException {

    if (this.extensionPoint == null) {
      // not even an extension point defined.
      return null;
    }
    
    if (extensionName != null) {
      Extension extension = findExtension(extensionName);
      if (extension != null) {
        if (LOG.isInfoEnabled()) {
          LOG.info("Using clustering extension: " + extensionName);
        }
        return (OnlineClusterer) extension.getExtensionInstance();
      }
      if (LOG.isWarnEnabled()) {
        LOG.warn("Clustering extension not found: '" + extensionName +
                 "', trying the default");
      }
      // not found, fallback to the default, if available.
    }

    final Extension[] extensions = this.extensionPoint.getExtensions();
    if (extensions.length > 0) {
      if (LOG.isInfoEnabled()) {
        LOG.info("Using the first clustering extension found: " +
                 extensions[0].getId());
      }
      return (OnlineClusterer) extensions[0].getExtensionInstance();
    } else {
      return null;
    }
  }

  private Extension findExtension(String name)
    throws PluginRuntimeException {

    final Extension[] extensions = this.extensionPoint.getExtensions();
    for (int i = 0; i < extensions.length; i++) {
      final Extension extension = extensions[i];
      if (name.equals(extension.getId()))
        return extension;
    }
    return null;
  }

} 
