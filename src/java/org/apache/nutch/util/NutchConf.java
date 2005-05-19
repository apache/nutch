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

package org.apache.nutch.util;

import java.util.*;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.MalformedURLException;
import java.io.*;
import java.util.logging.Logger;
import javax.xml.parsers.*;
import org.w3c.dom.*;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

/** Provides access to Nutch configuration parameters.
 *
 * <p>Default values for all parameters are specified in a file named
 * <tt>nutch-default.xml</tt> located on the classpath.  Overrides for these
 * defaults should be in an optional file named <tt>nutch-site.xml</tt>, also
 * located on the classpath.  Typically these files reside in the
 * <tt>conf/</tt> subdirectory at the top-level of a Nutch installation.
 */
    
public class NutchConf {
  private static final Logger LOG =
    LogFormatter.getLogger("org.apache.nutch.util.NutchConf");

  private static final NutchConf DEFAULT = new NutchConf();

  /** Return the default configuration. */
  public static NutchConf get() { return DEFAULT; }

  private ArrayList resourceNames = new ArrayList();
  private Properties properties;
  private ClassLoader classLoader = NutchConf.class.getClassLoader();

  /** A new configuration. */
  public NutchConf() {
    resourceNames.add("nutch-default.xml");
    resourceNames.add("nutch-site.xml");
  }

  /** A new configuration with the same settings as another. */
  public NutchConf(NutchConf other) {
    this.resourceNames = (ArrayList)other.resourceNames.clone();
    if (other.properties != null)
      this.properties = (Properties)other.properties.clone();
  }

  /** Adds a resource name to the chain of resources read.  Such resources are
   * located on the CLASSPATH.  The first resource is always
   * <tt>nutch-default.xml</tt>, and the last is always
   * <tt>nutch-site.xml</tt>.  New resources are inserted between these, so
   * they can override defaults, but not site-specifics. */
  public synchronized void addConfResource(String name) {
    addConfResourceInternal(name);
  }

  /** Adds a file to the chain of resources read.  The first resource is always
   * <tt>nutch-default.xml</tt>, and the last is always
   * <tt>nutch-site.xml</tt>.  New resources are inserted between these, so
   * they can override defaults, but not site-specifics. */
  public synchronized void addConfResource(File file) {
    addConfResourceInternal(file);
  }

  private synchronized void addConfResourceInternal(Object name) {
    resourceNames.add(resourceNames.size()-1, name); // add second to last
    properties = null;                            // trigger reload
  }

  /** Returns the value of the <code>name</code> property, or null if no
   * such property exists. */
  public String get(String name) { return getProps().getProperty(name);}

  /** Sets the value of the <code>name</code> property. */
  public void set(String name, Object value) {
    getProps().setProperty(name, value.toString());
  }
  
  /** Returns the value of the <code>name</code> property.  If no such property
   * exists, then <code>defaultValue</code> is returned.
   */
  public String get(String name, String defaultValue) {
     return getProps().getProperty(name, defaultValue);
  }
  
  /** Returns the value of the <code>name</code> property as an integer.  If no
   * such property is specified, or if the specified value is not a valid
   * integer, then <code>defaultValue</code> is returned.
   */
  public int getInt(String name, int defaultValue) {
    String valueString = get(name);
    if (valueString == null)
      return defaultValue;
    try {
      return Integer.parseInt(valueString);
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  /** Sets the value of the <code>name</code> property to an integer. */
  public void setInt(String name, int value) {
    set(name, Integer.toString(value));
  }


  /** Returns the value of the <code>name</code> property as a long.  If no
   * such property is specified, or if the specified value is not a valid
   * long, then <code>defaultValue</code> is returned.
   */
  public long getLong(String name, long defaultValue) {
    String valueString = get(name);
    if (valueString == null)
      return defaultValue;
    try {
      return Long.parseLong(valueString);
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  /** Returns the value of the <code>name</code> property as a float.  If no
   * such property is specified, or if the specified value is not a valid
   * float, then <code>defaultValue</code> is returned.
   */
  public float getFloat(String name, float defaultValue) {
    String valueString = get(name);
    if (valueString == null)
      return defaultValue;
    try {
      return Float.parseFloat(valueString);
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  /** Returns the value of the <code>name</code> property as an boolean.  If no
   * such property is specified, or if the specified value is not a valid
   * boolean, then <code>defaultValue</code> is returned.  Valid boolean values
   * are "true" and "false".
   */
  public boolean getBoolean(String name, boolean defaultValue) {
    String valueString = get(name);
    if ("true".equals(valueString))
      return true;
    else if ("false".equals(valueString))
      return false;
    else return defaultValue;
  }

  /** Returns the value of the <code>name</code> property as an array of
   * strings.  If no such property is specified, then <code>null</code>
   * is returned.  Values are whitespace or comma delimted.
   */
  public String[] getStrings(String name) {
    String valueString = get(name);
    if (valueString == null)
      return null;
    StringTokenizer tokenizer = new StringTokenizer (valueString,", \t\n\r\f");
    List values = new ArrayList();
    while (tokenizer.hasMoreTokens()) {
      values.add(tokenizer.nextToken());
    }
    return (String[])values.toArray(new String[values.size()]);
  }

  /** Returns the value of the <code>name</code> property as a Class.  If no
   * such property is specified, then <code>defaultValue</code> is returned.
   */
  public Class getClass(String name, Class defaultValue) {
    String valueString = get(name);
    if (valueString == null)
      return defaultValue;
    try {
      return Class.forName(valueString);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  /** Returns the value of the <code>name</code> property as a Class.  If no
   * such property is specified, then <code>defaultValue</code> is returned.
   * An error is thrown if the returned class does not implement the named
   * interface. 
   */
  public Class getClass(String propertyName, Class defaultValue,Class xface) {
    try {
      Class theClass = getClass(propertyName, defaultValue);
      if (theClass != null && !xface.isAssignableFrom(theClass))
        throw new RuntimeException(theClass+" not "+xface.getName());
      return theClass;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /** Sets the value of the <code>name</code> property to the name of a class.
   * First checks that the class implements the named interface. 
   */
  public void setClass(String propertyName, Class theClass, Class xface) {
    if (!xface.isAssignableFrom(theClass))
      throw new RuntimeException(theClass+" not "+xface.getName());
    set(propertyName, theClass.getName());
  }

  /** Returns the URL for the named resource. */
  public URL getResource(String name) {
    return classLoader.getResource(name);
  }
  /** Returns an input stream attached to the configuration resource with the
   * given <code>name</code>.
   */
  public InputStream getConfResourceAsInputStream(String name) {
    try {
      URL url= getResource(name);

      if (url == null) {
        LOG.info(name + " not found");
        return null;
      } else {
        LOG.info("found resource " + name + " at " + url);
      }

      return url.openStream();
    } catch (Exception e) {
      return null;
    }
  }

  /** Returns a reader attached to the configuration resource with the
   * given <code>name</code>.
   */
  public Reader getConfResourceAsReader(String name) {
    try {
      URL url= getResource(name);

      if (url == null) {
        LOG.info(name + " not found");
        return null;
      } else {
        LOG.info("found resource " + name + " at " + url);
      }

      return new InputStreamReader(url.openStream());
    } catch (Exception e) {
      return null;
    }
  }

  private synchronized Properties getProps() {
    if (properties == null) {
      Properties defaults = new Properties();
      Properties newProps = new Properties(defaults);
      ListIterator i = resourceNames.listIterator();
      while (i.hasNext()) {
        if (i.nextIndex() == 0) {                 // load defaults
          loadResource(defaults, i.next(), false);
        } else if (i.nextIndex()==resourceNames.size()-1) { // load site
          loadResource(newProps, i.next(), true);
        } else {                                  // load intermediate
          loadResource(newProps, i.next(), false);
        }
      }
      properties = newProps;
    }
    return properties;
  }

  private void loadResource(Properties properties,
                            Object name, boolean quietFail) {
    try {
      DocumentBuilder builder =
        DocumentBuilderFactory.newInstance().newDocumentBuilder();
      Document doc = null;

      if (name instanceof String) {               // a CLASSPATH resource
        URL url = getResource((String)name);
        if (url != null) {
          LOG.info("parsing " + url);
          doc = builder.parse(url.toString());
        }
      } else if (name instanceof File) {          // a file resource
        File file = (File)name;
        if (file.exists()) {
          LOG.info("parsing " + file);
          doc = builder.parse(file);
        }
      }

      if (doc == null) {
        if (quietFail)
          return;
        throw new RuntimeException(name + " not found");
      }

      Element root = doc.getDocumentElement();
      if (!"nutch-conf".equals(root.getTagName()))
        LOG.severe("bad conf file: top-level element not <nutch-conf>");
      NodeList props = root.getChildNodes();
      for (int i = 0; i < props.getLength(); i++) {
        Node propNode = props.item(i);
        if (!(propNode instanceof Element))
          continue;
        Element prop = (Element)propNode;
        if (!"property".equals(prop.getTagName()))
          LOG.warning("bad conf file: element not <property>");
        NodeList fields = prop.getChildNodes();
        String attr = null;
        String value = null;
        for (int j = 0; j < fields.getLength(); j++) {
          Node fieldNode = fields.item(j);
          if (!(fieldNode instanceof Element))
            continue;
          Element field = (Element)fieldNode;
          if ("name".equals(field.getTagName()))
            attr = ((Text)field.getFirstChild()).getData();
          if ("value".equals(field.getTagName()) && field.hasChildNodes())
            value = ((Text)field.getFirstChild()).getData();
        }
        if (attr != null && value != null)
          properties.setProperty(attr, value);
      }
        
    } catch (Exception e) {
      LOG.severe("error parsing conf file: " + e);
      throw new RuntimeException(e);
    }
    
  }

  /** Writes non-default properties in this configuration.*/
  public void write(OutputStream out) throws IOException {
    Properties properties = getProps();
    try {
      Document doc =
        DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
      Element conf = doc.createElement("nutch-conf");
      doc.appendChild(conf);
      conf.appendChild(doc.createTextNode("\n"));
      for (Enumeration e = properties.keys(); e.hasMoreElements();) {
        String name = (String)e.nextElement();
        String value = (String)properties.get(name);
      
        Element propNode = doc.createElement("property");
        conf.appendChild(propNode);
      
        Element nameNode = doc.createElement("name");
        nameNode.appendChild(doc.createTextNode(name));
        propNode.appendChild(nameNode);
      
        Element valueNode = doc.createElement("value");
        valueNode.appendChild(doc.createTextNode(value));
        propNode.appendChild(valueNode);

        conf.appendChild(doc.createTextNode("\n"));
      }
    
      DOMSource source = new DOMSource(doc);
      StreamResult result = new StreamResult(out);
      TransformerFactory transFactory = TransformerFactory.newInstance();
      Transformer transformer = transFactory.newTransformer();
      transformer.transform(source, result);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /** For debugging.  List non-default properties to the terminal and exit. */
  public static void main(String[] args) throws Exception {
    get().write(System.out);
  }

}
