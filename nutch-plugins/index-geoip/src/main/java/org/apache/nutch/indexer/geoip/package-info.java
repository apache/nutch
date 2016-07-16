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
/**
 * <p>This plugin implements an indexing filter which takes 
 * advantage of the 
 * <a href="https://github.com/maxmind/GeoIP2-java">GeoIP2-java API</a>.</p>
 * <p>The third party library distribution provides an API for the GeoIP2 
 * <a href="http://dev.maxmind.com/geoip/geoip2/web-services">Precision web services</a> 
 * and <a href="http://dev.maxmind.com/geoip/geoip2/downloadable">databases</a>. 
 * The API also works with the free 
 * <a href="http://dev.maxmind.com/geoip/geoip2/geolite2/">GeoLite2 databases</a>.
 *
 */
package org.apache.nutch.indexer.geoip;