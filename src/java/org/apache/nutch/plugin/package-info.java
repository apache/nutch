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

/** 
 * The Nutch {@link org.apache.nutch.plugin.Pluggable Plugin} System.
 * <p><b>The Nutch Plugin System provides a way to extend nutch functionality</b>.
 * A large part of the functionality of Nutch are provided by plugins:
 * All of the parsing, indexing and searching that nutch does is actually 
 * accomplished by various plugins.</p>
 * <p>In writing a plugin, you're actually providing one or more extensions 
 * of the existing extension-points (<i>hooks</i>). The core Nutch extension-points 
 * are themselves defined in a plugin, the <code>nutch-extensionpoints</code> plugin.
 * Each extension-point defines an interface that must be implemented by the 
 * extension. The core extension-points and extensions available in Nutch are
 * listed in the {@link org.apache.nutch.plugin.Pluggable} interface.</p>
 * @see <a href="./doc-files/plugin.dtd">Nutch plugin manifest DTD</a>
 * @see <a href="https://cwiki.apache.org/confluence/display/NUTCH/PluginCentral">Plugin Central</a>
 * @see <a href="https://cwiki.apache.org/confluence/display/NUTCH/AboutPlugins">About Plugins</a>
 * @see <a href="https://cwiki.apache.org/confluence/display/NUTCH/WhyNutchHasAPluginSystem">
 * Why Nutch has a Plugin System?</a>
 * @see <a href="https://cwiki.apache.org/confluence/display/NUTCH/WhichTechnicalConceptsAreBehindTheNutchPluginSystem">
 * Which technical concepts are behind the nutch plugin system?</a>
 * @see <a href="https://cwiki.apache.org/confluence/display/NUTCH/WhatsTheProblemWithPluginsAndClass-loading">
 * What's the problem with Plugins and Class loading?</a>
 * @see <a href="https://cwiki.apache.org/confluence/display/NUTCH/WritingPluginExample">
 * Writing Plugin Example</a>
 */
package org.apache.nutch.plugin;
