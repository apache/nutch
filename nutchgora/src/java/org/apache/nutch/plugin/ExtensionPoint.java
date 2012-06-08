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
import java.util.ArrayList;

/**
 * The <code>ExtensionPoint</code> provide meta information of a extension
 * point.
 * 
 * @author joa23
 */
public class ExtensionPoint {
  private String ftId;
  private String fName;
  private String fSchema;
  private ArrayList<Extension> fExtensions;

  /**
   * Constructor
   * 
   * @param pId
   *          unique extension point Id
   * @param pName
   *          name of the extension poin
   * @param pSchema
   *          xml schema of the extension point
   */
  public ExtensionPoint(String pId, String pName, String pSchema) {
    setId(pId);
    setName(pName);
    setSchema(pSchema);
    fExtensions = new ArrayList<Extension>();
  }

  /**
   * Returns the unique id of the extension point.
   * 
   * @return String
   */
  public String getId() {
    return ftId;
  }

  /**
   * Returns the name of the extension point.
   * 
   * @return String
   */
  public String getName() {
    return fName;
  }

  /**
   * Returns a path to the xml schema of a extension point.
   * 
   * @return String
   */
  public String getSchema() {
    return fSchema;
  }

  /**
   * Sets the extensionPointId.
   * 
   * @param pId extension point id
   */
  private void setId(String pId) {
    ftId = pId;
  }

  /**
   * Sets the extension point name.
   * 
   * @param pName
   */
  private void setName(String pName) {
    fName = pName;
  }

  /**
   * Sets the schema.
   * 
   * @param pSchema
   */
  private void setSchema(String pSchema) {
    fSchema = pSchema;
  }

  /**
   * Install a corresponding extension to this extension point.
   * 
   * @param extension
   */
  public void addExtension(Extension extension) {
    fExtensions.add(extension);
  }

  /**
   * Returns a array of extensions that listen to this extension point
   * 
   * @return Extension[]
   */
  public Extension[] getExtensions() {
    return fExtensions.toArray(new Extension[fExtensions.size()]);
  }

}
