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

rootProject.name = "apache-nutch"

// Plugin subprojects - all located under src/plugin/
val pluginDir = file("src/plugin")

// Library plugins (must be built first as other plugins depend on them)
include("lib-htmlunit")
include("lib-http")
include("lib-nekohtml")
include("lib-rabbitmq")
include("lib-regex-filter")
include("lib-selenium")
include("lib-xml")

// Nutch extension points
include("nutch-extensionpoints")

// Protocol plugins
include("protocol-file")
include("protocol-foo")
include("protocol-ftp")
include("protocol-htmlunit")
include("protocol-http")
include("protocol-httpclient")
include("protocol-interactiveselenium")
include("protocol-okhttp")
include("protocol-selenium")

// Parse plugins
include("parse-ext")
include("parse-html")
include("parse-js")
include("parse-metatags")
include("parse-tika")
include("parse-zip")

// Parse filter plugins
include("parsefilter-debug")
include("parsefilter-naivebayes")
include("parsefilter-regex")

// URL filter plugins
include("urlfilter-automaton")
include("urlfilter-domain")
include("urlfilter-domaindenylist")
include("urlfilter-fast")
include("urlfilter-ignoreexempt")
include("urlfilter-prefix")
include("urlfilter-regex")
include("urlfilter-suffix")
include("urlfilter-validator")

// URL normalizer plugins
include("urlnormalizer-ajax")
include("urlnormalizer-basic")
include("urlnormalizer-host")
include("urlnormalizer-pass")
include("urlnormalizer-protocol")
include("urlnormalizer-querystring")
include("urlnormalizer-regex")
include("urlnormalizer-slash")

// Scoring plugins
include("scoring-depth")
include("scoring-link")
include("scoring-metadata")
include("scoring-opic")
include("scoring-orphan")
include("scoring-similarity")

// Index filter plugins
include("index-anchor")
include("index-arbitrary")
include("index-basic")
include("index-geoip")
include("index-jexl-filter")
include("index-links")
include("index-metadata")
include("index-more")
include("index-replace")
include("index-static")

// Indexer backend plugins
include("indexer-cloudsearch")
include("indexer-csv")
include("indexer-dummy")
include("indexer-elastic")
include("indexer-kafka")
include("indexer-opensearch-1x")
include("indexer-rabbit")
include("indexer-solr")

// Exchange plugins
include("exchange-jexl")

// Publisher plugins
include("publish-rabbitmq")

// Miscellaneous plugins
include("creativecommons")
include("feed")
include("headings")
include("language-identifier")
include("microformats-reltag")
include("mimetype-filter")
include("subcollection")
include("tld")
include("urlmeta")

// Set project directories for all plugin subprojects
rootProject.children.forEach { project ->
    project.projectDir = file("src/plugin/${project.name}")
}

