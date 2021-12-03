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
 * URL normalizer performing basic normalizations:
 * <ul>
 * <li>remove default ports, e.g., port 80 for <code>http://</code> URLs</li>
 * <li>remove needless slashes and dot segments in the path component</li>
 * <li>remove anchors</li>
 * <li>use percent-encoding (only) where needed</li>
 * </ul>
 * 
 * E.g.,
 * <code>https://www.example.org/a/../b//./select%2Dlang.php?lang=español#anchor</code>
 * is normalized to <code>https://www.example.org/b/select-lang.php?lang=espa%C3%B1ol</code>
 * 
 * Optional and configurable normalizations are:
 * <ul>
 * <li>convert Internationalized Domain Names (IDNs) uniquely either to the
 * ASCII (Punycode) or Unicode representation, see property
 * <code>urlnormalizer.basic.host.idn</code></li>
 * <li>remove a trailing dot from host names, see property
 * <code>urlnormalizer.basic.host.trim-trailing-dot</code></li>
 * </ul>
 */
package org.apache.nutch.net.urlnormalizer.basic;

