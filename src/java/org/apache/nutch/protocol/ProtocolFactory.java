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

import java.util.Hashtable;
import java.net.URL;
import java.net.MalformedURLException;

import org.apache.nutch.plugin.*;

import java.util.logging.Logger;
import org.apache.nutch.util.LogFormatter;

/** Creates and caches {@link Protocol} plugins.  Protocol plugins should
 * define the attribute "protocolName" with the name of the protocol that they
 * implement. */
public class ProtocolFactory {

  public static final Logger LOG = LogFormatter
    .getLogger(ProtocolFactory.class.getName());

  private final static ExtensionPoint X_POINT = PluginRepository.getInstance()
      .getExtensionPoint(Protocol.X_POINT_ID);

  static {
    if (X_POINT == null) {
      throw new RuntimeException("x-point "+Protocol.X_POINT_ID+" not found.");
    }
  }

  private static final Hashtable CACHE = new Hashtable();

  private ProtocolFactory() {}                      // no public ctor

  /** Returns the appropriate {@link Protocol} implementation for a url. */
  public static Protocol getProtocol(String urlString)
    throws ProtocolNotFound {
    try {
      URL url = new URL(urlString);
      String protocolName = url.getProtocol();
      if (protocolName == null)
        throw new ProtocolNotFound(urlString);
      Extension extension = getExtension(protocolName);
      if (extension == null)
        throw new ProtocolNotFound(protocolName);

      return (Protocol)extension.getExtensionInstance();

    } catch (MalformedURLException e) {
      throw new ProtocolNotFound(urlString, e.toString());
    } catch (PluginRuntimeException e) {
      throw new ProtocolNotFound(urlString, e.toString());
    }
  }

  private static Extension getExtension(String name)
    throws PluginRuntimeException {

    if (CACHE.containsKey(name))
      return (Extension)CACHE.get(name);
    
    Extension extension = findExtension(name);
    
    if (extension != null) CACHE.put(name, extension);
    
    return extension;
  }

  private static Extension findExtension(String name)
    throws PluginRuntimeException {

    Extension[] extensions = X_POINT.getExtensions();

    for (int i = 0; i < extensions.length; i++) {
      Extension extension = extensions[i];

      if (name.equals(extension.getAttribute("protocolName")))
        return extension;
    }
    return null;
  }
}
