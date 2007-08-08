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
package org.apache.nutch.parse;

// JDK imports
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * This class represents a natural ordering for which parsing plugin should get
 * called for a particular mimeType. It provides methods to store the
 * parse-plugins.xml data, and methods to retreive the name of the appropriate
 * parsing plugin for a contentType.
 *
 * @author mattmann
 * @version 1.0
 */
class ParsePluginList {
  
  /* a map to link mimeType to an ordered list of parsing plugins */
  private Map<String, List<String>> fMimeTypeToPluginMap = null;
  
  /* A list of aliases */
  private Map<String, String> aliases = null;
  
  
  /**
   * Constructs a new ParsePluginList
   */
  ParsePluginList() {
    fMimeTypeToPluginMap = new HashMap<String, List<String>>();
    aliases = new HashMap<String, String>();
  }
  
  List<String> getPluginList(String mimeType) {
    return fMimeTypeToPluginMap.get(mimeType);
  }

  void setAliases(Map<String, String> aliases) {
    this.aliases = aliases;
  }
  
  Map<String, String> getAliases() {
    return aliases;
  }
  
  void setPluginList(String mimeType, List<String> l) {
    fMimeTypeToPluginMap.put(mimeType, l);
  }
  
  List<String> getSupportedMimeTypes() {
    return Arrays.asList(fMimeTypeToPluginMap.keySet().toArray(
            new String[] {}));
  }
  
}
