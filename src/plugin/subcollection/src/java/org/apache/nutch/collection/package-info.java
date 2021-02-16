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
 * <p>Subcollection is a subset of an index. Subcollections are 
 * defined by urlpatterns in form of white/blacklist. So to get the 
 * page into subcollection it must match the whitelist and not 
 * the blacklist.</p>
 * <p> Subcollection definitions are read from a file 
 * <code>subcollections.xml</code> and the format is as follows 
 * (imagine here that you are crawling all the virtualhosts from 
 * apache.org and you want to tag pages with url pattern 
 * "https://nutch.apache.org" and 
 * "https://cwiki.apache.org/confluence/display/nutch" to be part of 
 * subcollection "nutch", this allows you to later search specifically 
 * from this subcollection)</p>
 * <pre>
 * {@code
 * <xml version="1.0" encoding="UTF-8"?>
 * <subcollections>
 *  <subcollection>
 *   <name>nutch</name>
 *   <id>nutch</id>
 *   <whitelist>https://nutch.apache.org</whitelist>
 *   <whitelist>https://cwiki.apache.org/confluence/display/nutch</whitelist>
 *   <blacklist />
 *  </subcollection>
 * </subcollections>
 * }
 * </pre>
 * <p>Despite of this configuration you still can crawl any urls 
 * as long as they pass through your global url filters. (note that 
 * you must also seed your urls in normal nutch way)</p>
 */
package org.apache.nutch.collection;
