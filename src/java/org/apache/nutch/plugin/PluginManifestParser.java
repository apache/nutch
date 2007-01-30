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
package org.apache.nutch.plugin;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.logging.Log;

import org.apache.hadoop.conf.Configuration;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * The <code>PluginManifestParser</code> parser just parse the manifest file
 * in all plugin directories.
 * 
 * @author joa23
 */
public class PluginManifestParser {
  private static final String ATTR_NAME = "name";
  private static final String ATTR_CLASS = "class";
  private static final String ATTR_ID = "id";

  public static final Log LOG = PluginRepository.LOG;

  private static final boolean WINDOWS = System.getProperty("os.name")
      .startsWith("Windows");

  private Configuration conf;

  private PluginRepository pluginRepository;

  public PluginManifestParser(Configuration conf,
      PluginRepository pluginRepository) {
    this.conf = conf;
    this.pluginRepository = pluginRepository;
  }

  /**
   * Returns a list of all found plugin descriptors.
   * 
   * @param pluginFolders
   *          folders to search plugins from
   * @return A {@link Map} of all found {@link PluginDescriptor}s.
   */
  public Map<String, PluginDescriptor> parsePluginFolder(String[] pluginFolders) {
    Map<String, PluginDescriptor> map = new HashMap<String, PluginDescriptor>();

    if (pluginFolders == null) {
      throw new IllegalArgumentException("plugin.folders is not defined");
    }

    for (String name : pluginFolders) {
      File directory = getPluginFolder(name);
      if (directory == null) {
        continue;
      }
      LOG.info("Plugins: looking in: " + directory.getAbsolutePath());
      for (File oneSubFolder : directory.listFiles()) {
        if (oneSubFolder.isDirectory()) {
          String manifestPath = oneSubFolder.getAbsolutePath() + File.separator
              + "plugin.xml";
          try {
            LOG.debug("parsing: " + manifestPath);
            PluginDescriptor p = parseManifestFile(manifestPath);
            map.put(p.getPluginId(), p);
          } catch (MalformedURLException e) {
            LOG.warn(e.toString());
          } catch (SAXException e) {
            LOG.warn(e.toString());
          } catch (IOException e) {
            LOG.warn(e.toString());
          } catch (ParserConfigurationException e) {
            LOG.warn(e.toString());
          }
        }
      }
    }
    return map;
  }

  /**
   * Return the named plugin folder. If the name is absolute then it is
   * returned. Otherwise, for relative names, the classpath is scanned.
   */
  public File getPluginFolder(String name) {
    File directory = new File(name);
    if (!directory.isAbsolute()) {
      URL url = PluginManifestParser.class.getClassLoader().getResource(name);
      if (url == null && directory.exists() && directory.isDirectory()
          && directory.listFiles().length > 0) {
        return directory; // relative path that is not in the classpath
      } else if (url == null) {
        LOG.warn("Plugins: directory not found: " + name);
        return null;
      } else if (!"file".equals(url.getProtocol())) {
        LOG.warn("Plugins: not a file: url. Can't load plugins from: " + url);
        return null;
      }
      String path = url.getPath();
      if (WINDOWS && path.startsWith("/")) // patch a windows bug
        path = path.substring(1);
      try {
        path = URLDecoder.decode(path, "UTF-8"); // decode the url path
      } catch (UnsupportedEncodingException e) {
      }
      directory = new File(path);
    }
    return directory;
  }

  /**
   * @param manifestPath
   * @throws ParserConfigurationException
   * @throws IOException
   * @throws SAXException
   * @throws MalformedURLException
   */
  private PluginDescriptor parseManifestFile(String pManifestPath)
      throws MalformedURLException, SAXException, IOException,
      ParserConfigurationException {
    Document document = parseXML(new File(pManifestPath).toURL());
    String pPath = new File(pManifestPath).getParent();
    return parsePlugin(document, pPath);
  }

  /**
   * @param url
   * @return Document
   * @throws IOException
   * @throws SAXException
   * @throws ParserConfigurationException
   * @throws DocumentException
   */
  private Document parseXML(URL url) throws SAXException, IOException,
      ParserConfigurationException {
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    DocumentBuilder builder = factory.newDocumentBuilder();
    return builder.parse(url.openStream());
  }

  /**
   * @param pDocument
   * @throws MalformedURLException
   */
  private PluginDescriptor parsePlugin(Document pDocument, String pPath)
      throws MalformedURLException {
    Element rootElement = pDocument.getDocumentElement();
    String id = rootElement.getAttribute(ATTR_ID);
    String name = rootElement.getAttribute(ATTR_NAME);
    String version = rootElement.getAttribute("version");
    String providerName = rootElement.getAttribute("provider-name");
    String pluginClazz = null;
    if (rootElement.getAttribute(ATTR_CLASS).trim().length() > 0) {
      pluginClazz = rootElement.getAttribute(ATTR_CLASS);
    }
    PluginDescriptor pluginDescriptor = new PluginDescriptor(id, version, name,
        providerName, pluginClazz, pPath, this.conf);
    LOG.debug("plugin: id=" + id + " name=" + name + " version=" + version
          + " provider=" + providerName + "class=" + pluginClazz);
    parseExtension(rootElement, pluginDescriptor);
    parseExtensionPoints(rootElement, pluginDescriptor);
    parseLibraries(rootElement, pluginDescriptor);
    parseRequires(rootElement, pluginDescriptor);
    return pluginDescriptor;
  }

  /**
   * @param pRootElement
   * @param pDescriptor
   * @throws MalformedURLException
   */
  private void parseRequires(Element pRootElement, PluginDescriptor pDescriptor)
      throws MalformedURLException {

    NodeList nodelist = pRootElement.getElementsByTagName("requires");
    if (nodelist.getLength() > 0) {

      Element requires = (Element) nodelist.item(0);

      NodeList imports = requires.getElementsByTagName("import");
      for (int i = 0; i < imports.getLength(); i++) {
        Element anImport = (Element) imports.item(i);
        String plugin = anImport.getAttribute("plugin");
        if (plugin != null) {
          pDescriptor.addDependency(plugin);
        }
      }
    }
  }

  /**
   * @param pRootElement
   * @param pDescriptor
   * @throws MalformedURLException
   */
  private void parseLibraries(Element pRootElement, PluginDescriptor pDescriptor)
      throws MalformedURLException {
    NodeList nodelist = pRootElement.getElementsByTagName("runtime");
    if (nodelist.getLength() > 0) {

      Element runtime = (Element) nodelist.item(0);

      NodeList libraries = runtime.getElementsByTagName("library");
      for (int i = 0; i < libraries.getLength(); i++) {
        Element library = (Element) libraries.item(i);
        String libName = library.getAttribute(ATTR_NAME);
        NodeList list = library.getElementsByTagName("export");
        Element exportElement = (Element) list.item(0);
        if (exportElement != null)
          pDescriptor.addExportedLibRelative(libName);
        else
          pDescriptor.addNotExportedLibRelative(libName);
      }
    }
  }

  /**
   * @param rootElement
   * @param pluginDescriptor
   */
  private void parseExtensionPoints(Element pRootElement,
      PluginDescriptor pPluginDescriptor) {
    NodeList list = pRootElement.getElementsByTagName("extension-point");
    if (list != null) {
      for (int i = 0; i < list.getLength(); i++) {
        Element oneExtensionPoint = (Element) list.item(i);
        String id = oneExtensionPoint.getAttribute(ATTR_ID);
        String name = oneExtensionPoint.getAttribute(ATTR_NAME);
        String schema = oneExtensionPoint.getAttribute("schema");
        ExtensionPoint extensionPoint = new ExtensionPoint(id, name, schema);
        pPluginDescriptor.addExtensionPoint(extensionPoint);
      }
    }
  }

  /**
   * @param rootElement
   * @param pluginDescriptor
   */
  private void parseExtension(Element pRootElement,
      PluginDescriptor pPluginDescriptor) {
    NodeList extensions = pRootElement.getElementsByTagName("extension");
    if (extensions != null) {
      for (int i = 0; i < extensions.getLength(); i++) {
        Element oneExtension = (Element) extensions.item(i);
        String pointId = oneExtension.getAttribute("point");

        NodeList extensionImplementations = oneExtension.getChildNodes();
        if (extensionImplementations != null) {
          for (int j = 0; j < extensionImplementations.getLength(); j++) {
            Node node = extensionImplementations.item(j);
            if (!node.getNodeName().equals("implementation")) {
              continue;
            }
            Element oneImplementation = (Element) node;
            String id = oneImplementation.getAttribute(ATTR_ID);
            String extensionClass = oneImplementation.getAttribute(ATTR_CLASS);
            LOG.debug("impl: point=" + pointId + " class=" + extensionClass);
            Extension extension = new Extension(pPluginDescriptor, pointId, id,
                extensionClass, this.conf, this.pluginRepository);
            NodeList parameters = oneImplementation
                .getElementsByTagName("parameter");
            if (parameters != null) {
              for (int k = 0; k < parameters.getLength(); k++) {
                Element param = (Element) parameters.item(k);
                extension.addAttribute(param.getAttribute(ATTR_NAME), param
                    .getAttribute("value"));
              }
            }
            pPluginDescriptor.addExtension(extension);
          }
        }
      }
    }
  }
}
