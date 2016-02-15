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
package org.apache.nutch.filter.lang;

// JDK imports
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Iterator;
import java.util.Map;

import org.apache.avro.util.Utf8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.parse.HTMLMetaTags;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.HtmlParseFilter;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseResult;
import org.apache.nutch.protocol.Content;
import org.w3c.dom.DocumentFragment;

/**
 * Users can specify crawling language(s) arbitrarily.
 * if user set "language.filter.type" to accept and set "language.filter.languages" any ISO-639 language codes in nutch.site.xml,
 * this class remove outlinks except "language.filter.languages" value(s) for not crawl pages which are other languages.   
 * if user set "language.filter.type" to filter and set "language.filter.languages" any ISO-639 language codes in nutch.site.xml,
 * this class remove outlinks of "language.filter.languages" value(s) for not crawl pages which specify languages.
 */
public class LanguageFilter implements HtmlParseFilter {

   public static final Logger LOG = LoggerFactory
           .getLogger(LanguageFilter.class);

   public static final String LANG_FILTER_TYPE_ACCEPTING = "accept";
   public static final String LANG_FILTER_TYPE_FILTERING = "filter";
   private boolean filterLangOnly = false;
   private boolean acceptLangOnly = false; 

   public static final String LANG_FILTER_TYPE ="language.filter.type";
   public static final String LANG_FILTER_LANGUAGES ="language.filter.languages";


   /* A static Map of ISO-639 language codes */
   private List<String> LANGUAGES;

   private Configuration conf;

   public ParseResult filter(Content content, ParseResult parseResult,
           HTMLMetaTags metaTags, DocumentFragment doc) {
       String lang = parseResult.get(content.getUrl()).getData().getParseMeta().get(Metadata.LANGUAGE); 
       if(lang!=null){
          String[] ietfLang = lang.split("-");
          if(ietfLang.length > 1){
               lang = ietfLang[0];
            }
       }
       return getParse(parseResult, lang);
   }
   
   public ParseResult getParse(ParseResult parseResult, String lang){     
       if(filterLangOnly && LANGUAGES.contains(lang)){     
           return null;
       }
       if(acceptLangOnly && !LANGUAGES.contains(lang)){
           return null;
       }
       return parseResult;
   }
   
   public boolean isFilterLangOnly() {
       return filterLangOnly;
   }

   public boolean isAcceptLangOnly() {
       return acceptLangOnly;
   }

   public void setConf(Configuration conf) {
       this.conf = conf;
       String type = conf.get(LANG_FILTER_TYPE, "accept");     
       if (type.equals(LANG_FILTER_TYPE_ACCEPTING))
           acceptLangOnly = true;
       else if(type.equals(LANG_FILTER_TYPE_FILTERING))
           filterLangOnly = true;
       
       String languages = conf.get(LANG_FILTER_LANGUAGES,"en");
       LANGUAGES = Arrays.asList(languages.split(" "));
   }

   public Configuration getConf() {
       return this.conf;
   }
}
