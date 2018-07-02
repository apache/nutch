/*
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
package org.apache.nutch.plugin;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Locale;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for the plugin system
 */
public class TestPluginSystem {
  private int fPluginCount;

  private LinkedList<File> fFolders = new LinkedList<File>();
  private Configuration conf;
  private PluginRepository repository;

  @Before
  public void setUp() throws Exception {
    this.conf = NutchConfiguration.create();
    conf.set("plugin.includes", ".*");
    // String string = this.conf.get("plugin.includes", "");
    // conf.set("plugin.includes", string + "|Dummy*");
    fPluginCount = 5;
    createDummyPlugins(fPluginCount);
    this.repository = PluginRepository.get(conf);
  }

  /*
   * (non-Javadoc)
   * 
   * @see junit.framework.TestCase#tearDown()
   */
  @After
  public void tearDown() throws Exception {
    for (int i = 0; i < fFolders.size(); i++) {
      File folder = fFolders.get(i);
      delete(folder);
      folder.delete();
    }
  }

  /**
   */
  @Test
  public void testPluginConfiguration() {
    String string = getPluginFolder();
    File file = new File(string);
    if (!file.exists()) {
      file.mkdir();
    }
    Assert.assertTrue(file.exists());
  }

  /**
   */
  @Test
  public void testLoadPlugins() {
    PluginDescriptor[] descriptors = repository.getPluginDescriptors();
    int k = descriptors.length;
    Assert.assertTrue(fPluginCount <= k);
    for (int i = 0; i < descriptors.length; i++) {
      PluginDescriptor descriptor = descriptors[i];
      if (!descriptor.getPluginId().startsWith("getPluginFolder()")) {
        continue;
      }
      Assert.assertEquals(1, descriptor.getExportedLibUrls().length);
      Assert.assertEquals(1, descriptor.getNotExportedLibUrls().length);
    }
  }

  @Test
  public void testRepositoryCache() throws IOException {
    Configuration config = NutchConfiguration.create();
    PluginRepository repo = PluginRepository.get(config);
    Job job = NutchJob.getInstance(config);
    config = job.getConfiguration();
    PluginRepository repo1 = PluginRepository.get(config);
    Assert.assertTrue(repo == repo1);
    // now construct a config without UUID
    config = new Configuration();
    config.addResource("nutch-default.xml");
    config.addResource("nutch-site.xml");
    repo = PluginRepository.get(config);
    job = NutchJob.getInstance(config);
    config = job.getConfiguration();
    repo1 = PluginRepository.get(config);
    Assert.assertTrue(repo1 != repo);
  }

  /**
   *  
   */
  @Test
  public void testGetExtensionAndAttributes() {
    String xpId = " sdsdsd";
    ExtensionPoint extensionPoint = repository.getExtensionPoint(xpId);
    Assert.assertEquals(extensionPoint, null);
    Extension[] extension1 = repository.getExtensionPoint(getGetExtensionId())
        .getExtensions();
    Assert.assertEquals(extension1.length, fPluginCount);
    for (int i = 0; i < extension1.length; i++) {
      Extension extension2 = extension1[i];
      String string = extension2.getAttribute(getGetConfigElementName());
      Assert.assertEquals(string, getParameterValue());
    }
  }

  /**
   * @throws PluginRuntimeException
   */
  @Test
  public void testGetExtensionInstances() throws PluginRuntimeException {
    Extension[] extensions = repository.getExtensionPoint(getGetExtensionId())
        .getExtensions();
    Assert.assertEquals(extensions.length, fPluginCount);
    for (int i = 0; i < extensions.length; i++) {
      Extension extension = extensions[i];
      Object object = extension.getExtensionInstance();
      if (!(object instanceof HelloWorldExtension))
        Assert.fail(" object is not a instance of HelloWorldExtension");
      ((ITestExtension) object).testGetExtension("Bla ");
      String string = ((ITestExtension) object).testGetExtension("Hello");
      Assert.assertEquals("Hello World", string);
    }
  }

  /**
   * 
   *  
   */
  @Test
  public void testGetClassLoader() {
    PluginDescriptor[] descriptors = repository.getPluginDescriptors();
    for (int i = 0; i < descriptors.length; i++) {
      PluginDescriptor descriptor = descriptors[i];
      Assert.assertNotNull(descriptor.getClassLoader());
    }
  }

  /**
   * @throws IOException
   */
  @Test
  public void testGetResources() throws IOException {
    PluginDescriptor[] descriptors = repository.getPluginDescriptors();
    for (int i = 0; i < descriptors.length; i++) {
      PluginDescriptor descriptor = descriptors[i];
      if (!descriptor.getPluginId().startsWith("getPluginFolder()")) {
        continue;
      }
      String value = descriptor.getResourceString("key", Locale.UK);
      Assert.assertEquals("value", value);
      value = descriptor.getResourceString("key", Locale.TRADITIONAL_CHINESE);
      Assert.assertEquals("value", value);

    }
  }

  /**
   * @return a PluginFolderPath
   */
  private String getPluginFolder() {
    String[] strings = conf.getStrings("plugin.folders");
    if (strings == null || strings.length == 0)
      Assert.fail("no plugin directory setuped..");

    String name = strings[0];
    return new PluginManifestParser(conf, this.repository)
        .getPluginFolder(name).toString();
  }

  /**
   * Creates some Dummy Plugins
   * 
   * @param pCount
   */
  private void createDummyPlugins(int pCount) {
    String string = getPluginFolder();
    try {
      File folder = new File(string);
      folder.mkdir();
      for (int i = 0; i < pCount; i++) {
        String pluginFolder = string + File.separator + "DummyPlugin" + i;
        File file = new File(pluginFolder);
        file.mkdir();
        fFolders.add(file);
        createPluginManifest(i, file.getAbsolutePath());
        createResourceFile(file.getAbsolutePath());
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Creates an ResourceFile
   * 
   * @param pFolderPath
   * @throws FileNotFoundException
   * @throws IOException
   */
  private void createResourceFile(String pFolderPath)
      throws FileNotFoundException, IOException {
    Properties properties = new Properties();
    properties.setProperty("key", "value");
    properties.store(new FileOutputStream(pFolderPath + File.separator
        + "messages" + ".properties"), "");
  }

  /**
   * Deletes files in path
   * 
   * @param path
   * @throws IOException
   */
  private void delete(File path) throws IOException {
    File[] files = path.listFiles();
    for (int i = 0; i < files.length; ++i) {
      if (files[i].isDirectory())
        delete(files[i]);
      files[i].delete();
    }
  }

  /**
   * Creates an Plugin Manifest File
   * 
   * @param i
   * @param pFolderPath
   * @throws IOException
   */
  private void createPluginManifest(int i, String pFolderPath)
      throws IOException {
    FileWriter out = new FileWriter(pFolderPath + File.separator + "plugin.xml");
    String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        + "<!--this is just a simple plugin for testing issues.-->"
        + "<plugin id=\"org.apache.nutch.plugin."
        + i
        + "\" name=\""
        + i
        + "\" version=\"1.0\" provider-name=\"joa23\" "
        + "class=\"org.apache.nutch.plugin.SimpleTestPlugin\">"
        + "<extension-point id=\"aExtensioID\" "
        + "name=\"simple Parser Extension\" "
        + "schema=\"schema/testExtensionPoint.exsd\"/>"
        + "<runtime><library name=\"libs/exported.jar\"><extport/></library>"
        + "<library name=\"libs/not_exported.jar\"/></runtime>"
        + "<extension point=\"aExtensioID\">"
        + "<implementation name=\"simple Parser Extension\" "
        + "id=\"aExtensionId.\" class=\"org.apache.nutch.plugin.HelloWorldExtension\">"
        + "<parameter name=\"dummy-name\" value=\"a simple param value\"/>"
        + "</implementation></extension></plugin>";
    out.write(xml);
    out.flush();
    out.close();
  }

  private String getParameterValue() {
    return "a simple param value";
  }

  private static String getGetExtensionId() {
    return "aExtensioID";
  }

  private static String getGetConfigElementName() {
    return "dummy-name";
  }

  public static void main(String[] args) throws IOException {
    new TestPluginSystem().createPluginManifest(1, "/");
  }
}
