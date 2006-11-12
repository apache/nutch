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

package org.apache.nutch.ontology;

// Commons Logging imports
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.nutch.plugin.*;
import org.apache.hadoop.conf.Configuration;

/**
 * A factory for retrieving {@link Ontology} extensions.
 *
 * @author Michael Pan
 * @version $Id: OntologyFactory.java,v 1.2 2005/02/07 19:09:58 cutting Exp $
 */
public class OntologyFactory {
  public final static Log LOG = LogFactory.getLog(OntologyFactory.class);

  private ExtensionPoint extensionPoint;
  private Configuration conf;
  
  public OntologyFactory(Configuration conf) {
    this.conf = conf;
    this.extensionPoint = PluginRepository.get(conf).getExtensionPoint(Ontology.X_POINT_ID);  
  }

  /**
  * @return Returns the online ontology extension specified
  * in nutch configuration's key
  * <code>extension.ontology.extension-name</code>.
  * If the name is  empty (no preference),
  * the first available ontology extension is returned.
  */
  public Ontology getOntology() throws PluginRuntimeException {
     
    if (this.extensionPoint == null) {
      // not even an extension point defined.
      return null;
    }

    String extensionName = this.conf.get("extension.ontology.extension-name");
    if (extensionName != null) {
      Extension extension = findExtension(extensionName);
      if (extension != null) {
        if (LOG.isInfoEnabled()) {
          LOG.info("Using ontology extension: " + extensionName);
        }
        return (Ontology) extension.getExtensionInstance();
      }
      if (LOG.isWarnEnabled()) {
        LOG.warn("Ontology extension not found: '" + extensionName +
                 "', trying the default");
      }
      // not found, fallback to the default, if available.
    }

    Extension[] extensions = this.extensionPoint.getExtensions();
    if (extensions.length > 0) {
      if (LOG.isInfoEnabled()) {
        LOG.info("Using the first ontology extension found: " +
                 extensions[0].getId());
      }
      return (Ontology) extensions[0].getExtensionInstance();
    } else {
      return null;
    }

  }

  private Extension findExtension(String name)
    throws PluginRuntimeException {

    Extension[] extensions = this.extensionPoint.getExtensions();

    for (int i = 0; i < extensions.length; i++) {
      Extension extension = extensions[i];

      if (name.equals(extension.getId()))
        return extension;
    }

    return null;
  }

} 
