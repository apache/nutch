/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nutch.protocol;

import java.net.URL;
import java.net.MalformedURLException;

// Commons Logging imports
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.nutch.plugin.*;

import org.apache.hadoop.conf.Configuration;

/** Creates and caches {@link Protocol} plugins.  Protocol plugins should
 * define the attribute "protocolName" with the name of the protocol that they
 * implement. */
public class ProtocolFactory {

  public static final Log LOG = LogFactory.getLog(ProtocolFactory.class);

  private ExtensionPoint extensionPoint;
  private Configuration conf;

  public ProtocolFactory(Configuration conf) {
      this.conf = conf;
      this.extensionPoint = PluginRepository.get(conf)
      .getExtensionPoint(Protocol.X_POINT_ID);
      if (this.extensionPoint == null) {
          throw new RuntimeException("x-point " + Protocol.X_POINT_ID + " not found.");
        }
  }                      

  /** Returns the appropriate {@link Protocol} implementation for a url. */
  public Protocol getProtocol(String urlString)
    throws ProtocolNotFound {
    try {
      URL url = new URL(urlString);
      String protocolName = url.getProtocol();
      if (protocolName == null)
        throw new ProtocolNotFound(urlString);
      Extension extension = getExtension(protocolName);
      if (extension == null)
        throw new ProtocolNotFound(protocolName);
      Protocol protocol = (Protocol) extension.getExtensionInstance();
      protocol.setConf(this.conf);
      return protocol;

    } catch (MalformedURLException e) {
      throw new ProtocolNotFound(urlString, e.toString());
    } catch (PluginRuntimeException e) {
      throw new ProtocolNotFound(urlString, e.toString());
    }
  }

  private Extension getExtension(String name)
    throws PluginRuntimeException {

    if (this.conf.getObject(name) != null)
      return (Extension)this.conf.getObject(name);
    
    Extension extension = findExtension(name);
    
    if (extension != null) this.conf.setObject(name, extension);
    
    return extension;
  }

  private Extension findExtension(String name)
    throws PluginRuntimeException {

    Extension[] extensions = this.extensionPoint.getExtensions();

    for (int i = 0; i < extensions.length; i++) {
      Extension extension = extensions[i];

      if (name.equals(extension.getAttribute("protocolName")))
        return extension;
    }
    return null;
  }
}
