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
 * URL normalizer to normalize the protocol for all URLs of a given host or
 * domain.
 * 
 * E.g., normalize <code>http://nutch.apache.org/path/</code> to
 * <code>https://www.apache.org/path/</code> if it's known that the host
 * <code>nutch.apache.org</code> supports https and http-URLs either cause
 * duplicate content or are redirected to https.
 *
 * The configuration of rules follows the schema:
 * 
 * <pre>
 * &lt;host&gt; \t &lt;protcol&gt;
 * </pre>
 * 
 * for example
 * 
 * <pre>
 * nutch.apache.org \t https
 * *.example.com \t http
 * </pre>
 * 
 * These rules will normalize all URLs of the host <code>nutch.apache.org</code>
 * to use https while every URL from <code>example.com</code> and its subdomains
 * is normalized to be based on http.
 *
 * A "host" pattern which starts with <code>*.</code> will match all hosts
 * (subdomains) of the given domain, or more generally matches domain suffixes
 * separated by a dot.
 * 
 * Rules are usually configured via the configuration file "protocols.txt". The
 * filename is specified by the property
 * <code>urlnormalizer.protocols.file</code>. Alternatively, if the property
 * <code>urlnormalizer.protocols.rules</code> defines a non-empty string, these
 * rules take precedence of those specified in the rule file.
 */
package org.apache.nutch.net.urlnormalizer.protocol;

