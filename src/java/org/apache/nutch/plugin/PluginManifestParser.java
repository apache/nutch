/*
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
package org.apache.nutch.plugin;
import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import org.apache.nutch.util.NutchConf;
import org.dom4j.Attribute;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import java.util.regex.Pattern;

/**
 * The <code>PluginManifestParser</code> parser just parse the manifest file in all plugin
 * directories.
 * 
 * @author joa23
 */
public class PluginManifestParser {
  public static final Logger LOG = PluginRepository.LOG;

  private static final boolean WINDOWS
    = System.getProperty("os.name").startsWith("Windows");

  /**
   * Returns a list with plugin descriptors.
   * 
   * @return ArrayList
   * @throws IOException
   * @throws MalformedURLException
   * @throws DocumentException
   */
  public static ArrayList parsePluginFolder() {
    ArrayList list = new ArrayList();
    String[] pluginFolders = NutchConf.get().getStrings("plugin.folders");
    Pattern excludes = Pattern.compile(NutchConf.get().get("plugin.excludes", ""));
    Pattern includes = Pattern.compile(NutchConf.get().get("plugin.includes", ""));
    if (pluginFolders == null)
      throw new IllegalArgumentException("plugin.folders is not defined");
    for (int i = 0; i < pluginFolders.length; i++) {
      String name = pluginFolders[i];
      File directory = getPluginFolder(name);
      if (directory == null)
        continue;
      LOG.info("Plugins: looking in: "+directory);
      File[] files = directory.listFiles();
      if (files == null)
        continue;
      for (int j = 0; j < files.length; j++) {
        File oneSubFolder = files[j];
        if (oneSubFolder.isDirectory()) {
          
          if (!includes.matcher(oneSubFolder.getName()).matches()) {
            LOG.info("not including: "+oneSubFolder);
            continue;
          }

          if (excludes.matcher(oneSubFolder.getName()).matches()) {
            LOG.info("excluding: "+oneSubFolder);
            continue;
          }

          String manifestPath = oneSubFolder.getAbsolutePath()
            + File.separator + "plugin.xml";
          try {
            LOG.info("parsing: "+manifestPath);
            list.add(parseManifestFile(manifestPath));
          } catch (MalformedURLException e) {
            LOG.info(e.toString());
          } catch (DocumentException e) {
            LOG.info(e.toString());
          }
        }
      }
    }
    return list;
  }

  /** Return the named plugin folder.  If the name is absolute then it is
   * returned.  Otherwise, for relative names, the classpath is scanned.*/
  static File getPluginFolder(String name) {
    File directory = new File(name);
    if (!directory.isAbsolute()) {
      URL url =PluginManifestParser.class.getClassLoader().getResource(name);
      if (url == null) {
        LOG.info("Plugins: directory not found: "+name);
        return null;
      } else if (!"file".equals(url.getProtocol())) {
        LOG.info("Plugins: not a file: url. Can't load plugins from: "+url);
        return null;
      }
      String path = url.getPath();
      if (WINDOWS && path.startsWith("/"))        // patch a windows bug
        path = path.substring(1);
      try {
        path = URLDecoder.decode(path, "UTF-8");    // decode the url path
      } catch (UnsupportedEncodingException e) {}
      directory = new File(path);
    }
    return directory;
  }

  /**
   * @param manifestPath
   */
  private static PluginDescriptor parseManifestFile(String pManifestPath)
    throws MalformedURLException, DocumentException {
    Document document = parseXML(new File(pManifestPath).toURL());
    String pPath = new File(pManifestPath).getParent();
    return parsePlugin(document, pPath);
  }
  /**
   * @param url
   * @return Document
   * @throws DocumentException
   */
  private static Document parseXML(URL url) throws DocumentException {
    SAXReader reader = new SAXReader();
    Document document = reader.read(url);
    return document;
  }
  /**
   * @param document
   */
  private static PluginDescriptor parsePlugin(Document pDocument, String pPath)
    throws MalformedURLException {
    Element rootElement = pDocument.getRootElement();
    String id = rootElement.attributeValue("id");
    String name = rootElement.attributeValue("name");
    String version = rootElement.attributeValue("version");
    String providerName = rootElement.attributeValue("provider-name");
    String pluginClazz = rootElement.attributeValue("class");
    PluginDescriptor pluginDescriptor = new PluginDescriptor(id, version,
                                                             name, providerName, pluginClazz, pPath);
//     LOG.fine("plugin: id="+id+" name="+name+" version="+version
//              +" provider="+providerName+"class="+pluginClazz);
    parseExtension(rootElement, pluginDescriptor);
    parseExtensionPoints(rootElement, pluginDescriptor);
    parseLibraries(rootElement, pluginDescriptor);
    return pluginDescriptor;
  }
  /**
   * @param rootElement
   * @param pluginDescriptor
   */
  private static void parseLibraries(Element pRootElement,
                                     PluginDescriptor pDescriptor) throws MalformedURLException {
    Element runtime = pRootElement.element("runtime");
    if (runtime == null)
      return;
    List libraries = runtime.elements("library");
    for (int i = 0; i < libraries.size(); i++) {
      Element library = (Element) libraries.get(i);
      String libName = library.attributeValue("name");
      Element exportElement = library.element("extport");
      if (exportElement != null)
        pDescriptor.addExportedLibRelative(libName);
      else
        pDescriptor.addNotExportedLibRelative(libName);
    }
  }
  /**
   * @param rootElement
   * @param pluginDescriptor
   */
  private static void parseExtensionPoints(Element pRootElement,
                                           PluginDescriptor pPluginDescriptor) {
    List list = pRootElement.elements("extension-point");
    if (list != null) {
      for (int i = 0; i < list.size(); i++) {
        Element oneExtensionPoint = (Element) list.get(i);
        String id = oneExtensionPoint.attributeValue("id");
        String name = oneExtensionPoint.attributeValue("name");
        String schema = oneExtensionPoint.attributeValue("schema");
        ExtensionPoint extensionPoint = new ExtensionPoint(id, name,
                                                           schema);
        //LOG.fine("plugin: point="+id);
        pPluginDescriptor.addExtensionPoint(extensionPoint);
      }
    }
  }
  /**
   * @param rootElement
   * @param pluginDescriptor
   */
  private static void parseExtension(Element pRootElement,
                                     PluginDescriptor pPluginDescriptor) {
    List extensions = pRootElement.elements("extension");
    if (extensions != null) {
      for (int i = 0; i < extensions.size(); i++) {
        Element oneExtension = (Element) extensions.get(i);
        String pointId = oneExtension.attributeValue("point");
        List extensionImplementations = oneExtension.elements();
        if (extensionImplementations != null) {
          for (int j = 0; j < extensionImplementations.size(); j++) {
            Element oneImplementation = (Element) extensionImplementations
              .get(j);
            String id = oneImplementation.attributeValue("id");
            String extensionClass = oneImplementation.attributeValue("class");
            LOG.fine("impl: point="+pointId+" class="+extensionClass);
            Extension extension = new Extension(pPluginDescriptor,
                                                pointId, id, extensionClass);
            List list = oneImplementation.attributes();
            for (int k = 0; k < list.size(); k++) {
              Attribute attribute = (Attribute) list.get(k);
              String name = attribute.getName();
              if (name.equals("id") || name.equals("class"))
                continue;
              String value = attribute.getValue();
              extension.addAttribute(name, value);
            }
            pPluginDescriptor.addExtension(extension);
          }
        }
      }
    }
  }
}
