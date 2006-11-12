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
package org.apache.nutch.keymatch;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.nutch.searcher.Query;
import org.apache.nutch.util.DomUtil;
import org.apache.xerces.dom.DocumentImpl;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/**
 * <p>SimpleKeyMatcher is responsible for targetting predefined links for defined
 * keywords for example to promote some urls that are not yet part of
 * production index.</p>
 * <p>SimpleKeyMatcher is not a textadd targetting system</p>
 * <p>KeyMatcher is configured with xml configuration file:
 * <br><pre>
 * &lt;?xml version="1.0"?>
 * &lt;keymatches>
 *   &lt;keymatch type="keyword|phrase|exact">
 *     &lt;term>search engine&lt;/term>
 *     &lt;url>http://lucene.apache.org/nutch&lt;/url>
 *     &lt;title>Your favourite search engine!&lt;/title>
 *   &lt;/keymatch>
 * &lt;/keymatches></pre>
 * By default Keymatcher expects the file be named keymatches.xml
 * </p>
 * <p>Match type can be one of the following: keyword, phrase, exact match.
 * Terms of a query are produced by the Query object and none of the
 * matches is case sensitive</p>
 * <b>keyword</b><br>
 * Matches on keyword level, for example query "search engine" would match both
 * keywords search and engine<br>
 * <br>
 * <b>phrase</b><br>
 * Matches phrase, for example: query "open source search engine" "search engine watch"
 * would match "search engine", but query "search from engine" would not.<br>
 * <br>
 * <b>exact</b><br>
 * Query "open source search engine" would match "open source search engine", but not
 * "search engine" nor "best open source engine"<br>
 * 
 */
public class SimpleKeyMatcher extends Configured {

  static final char PREFIX_KEYWORD='k';
  static final char PREFIX_PHRASE='p';
  static final char PREFIX_EXACT='e';
  
  class KeyMatcherStats {
    int terms[];

    void addStats(int numTerms) {
      if (numTerms <= terms.length) {
        terms[numTerms]++;
      }
    }

    public KeyMatcherStats(int size) {
      terms = new int[size];
      for (int i = 0; i < size; i++) {
        terms[i] = 0;
      }
    }
  }

  public static final Log LOG = LogFactory.getLog(SimpleKeyMatcher.class);

  public static final String TAG_KEYMATCH = "keymatch";

  public static final String TAG_KEYMATCHES = "keymatches";

  static final String DEFAULT_CONFIG_FILE = "keymatches.xml";

  static final int MAX_TERMS = 5;

  KeyMatcherStats stats;
  KeyMatchFilter currentFilter;

  HashMap matches = new HashMap();
  private String configName;

  public SimpleKeyMatcher(Configuration conf) {
    this(DEFAULT_CONFIG_FILE,conf);
  }
  
  /**
   * Sets currentFilter
   * @param filter the filter to set
   */
  public void setFilter(KeyMatchFilter filter) {
    this.currentFilter=filter;
  }

  /**
   * Construct new SimpleKeyMatcher with provided filename and configuration
   * @param resourceName
   * @param conf
   */
  public SimpleKeyMatcher(String resourceName, Configuration conf) {
    super(conf);
    configName=resourceName;
    stats = new KeyMatcherStats(MAX_TERMS);
    currentFilter=new ViewCountSorter();
    init();
  }

  /**
   * Initialize keyword matcher
   * 
   */
  protected void init() {
    final HashMap tempMap = new HashMap();
    final InputStream input = getConf().getConfResourceAsInputStream(
        configName);

    if (input != null) {
      final Element root = DomUtil.getDom(input);
      try {
        input.close();
      } catch (IOException e1) {
        e1.printStackTrace();
      }

      final NodeList nodeList = root.getElementsByTagName(TAG_KEYMATCH);

      LOG.debug("Configuration file has " + nodeList.getLength()
          + " KeyMatch entries.");
      for (int i = 0; i < nodeList.getLength(); i++) {
        final Element element = (Element) nodeList.item(i);
        final KeyMatch keyMatch = new KeyMatch();
        keyMatch.initialize(element);
        addKeyMatch(tempMap, keyMatch);
      }

      matches=tempMap;
    }
  }

  /**
   * Get keymatches for query
   * @param query parsed query
   * @param context evaluation context
   * @return array of keymatches
   */
  public KeyMatch[] getMatches(final Query query, Map context) {
    
    final ArrayList currentMatches=new ArrayList();
    
    final String terms[]=query.getTerms();
      
    //"keyword"
    for(int i=0;i<terms.length;i++){
      if(LOG.isDebugEnabled()){
        LOG.debug("keyword: '" + terms[i] + "'");
      }

      addMatches(currentMatches, matches.get(PREFIX_KEYWORD + terms[i]));
    }
    
    //"phrase"
    for(int l=2;l<=terms.length;l++){
      if(stats.terms[l]>0) {
      for(int p=0;p<=terms.length-l;p++){
        String key="";
          for(int i=p;i<p+l;i++){
            key+=terms[i];
            if(i!=p+l-1) key+=" ";
          }
          if(LOG.isDebugEnabled()){
            LOG.debug("phrase key: '" + key + "'");
          }
          addMatches(currentMatches, matches.get(PREFIX_PHRASE + key));
        }
      }
    }

    //"exact"
    String key=query.toString();
    if(LOG.isDebugEnabled()){
      LOG.debug("exact key: '" + key + "'");
    }

    addMatches(currentMatches, matches.get(PREFIX_EXACT + key));

    return currentFilter.filter(currentMatches, context);  
  }

  void addMatches(ArrayList currentMatches, Object match){
    if(match!=null) {
    if(match instanceof ArrayList) {
      currentMatches.addAll(((ArrayList)match));
    } else {
      currentMatches.add(match);
    }
    }
  }
  
  /** Get tokens of a string with nutch Query parser
   * 
   * @param string
   * @return
   */
  private String[] getTokens(final String string){
      org.apache.nutch.searcher.Query q;
      try {
        q = org.apache.nutch.searcher.Query.parse(string, getConf());
        return q.getTerms();
      } catch (IOException e) {
        LOG.info("Error getting terms from query:" + e);
      }
      return new String[0];
  }

  /**
   * add new keymatch
   * 
   * @param keymatch
   */
  protected void addKeyMatch(Map map, final KeyMatch keymatch) {
    String key="";

    LOG.info("Adding keymatch: MATCHTYPE=" + KeyMatch.TYPES[keymatch.type] + ", TERM='" + keymatch.term + "', TITLE='"
        + keymatch.title + "' ,URL='" + keymatch.url + "'");

    keymatch.term=keymatch.term.toLowerCase();
    switch (keymatch.type) {
      case KeyMatch.TYPE_EXACT: key+=PREFIX_EXACT;break;
      case KeyMatch.TYPE_PHRASE: key+=PREFIX_PHRASE;break;
      default: key+=PREFIX_KEYWORD;break;
    }
    
    //add info obout kw count for optimization
    if(keymatch.type==KeyMatch.TYPE_PHRASE) {
      stats.addStats(getTokens(keymatch.term).length);
    }
    
    key+=keymatch.term;
    
    if(map.containsKey(key)) {
      ArrayList l;

      Object o = matches.get(key);
      if(o instanceof ArrayList) {
        l=(ArrayList) o;
      } else {
        KeyMatch temp=(KeyMatch)o;
        l=new ArrayList();
        l.add(temp);
      }
      l.add(keymatch);
      map.put(key,l);
    } else {
      map.put(key, keymatch);
    }
  }
  
  /**
   * Add Keymatch
   *
   */
  public void addKeyMatch(KeyMatch match){
    addKeyMatch(matches, match);
  }

  /**
   * Saves keymatch configuration into file.
   * 
   * @throws IOException
   */
  public void save() throws IOException {
    try {
      final URL url = getConf().getResource(configName);
      if (url == null) {
        throw new IOException("Resource not found: " + configName);
      }
      final FileOutputStream fos = new FileOutputStream(new File(url.getFile()));
      final DocumentImpl doc = new DocumentImpl();
      final Element keymatches = doc.createElement(TAG_KEYMATCHES);
      final Iterator iterator = matches.values().iterator();

      while (iterator.hasNext()) {
        final Element keymatch = doc.createElement(TAG_KEYMATCH);
        final KeyMatch keyMatch = (KeyMatch) iterator.next();
        keyMatch.populateElement(keymatch);
        keymatches.appendChild(keymatch);
      }

      DomUtil.saveDom(fos, keymatches);
      fos.flush();
      fos.close();
    } catch (FileNotFoundException e) {
      throw new IOException(e.toString());
    }
  }

  /** 
   * Clear keymatches from this SimpleKeyMatcher instance
   *
   */
  public void clear(){
    matches=new HashMap();
  }

  public void setKeyMatches(List keymatches){
    HashMap hm=new HashMap();
    Iterator i=keymatches.iterator();
    while(i.hasNext()) {
      KeyMatch km=(KeyMatch)i.next();
      addKeyMatch(hm,km);
    }
    matches=hm;
  }

}
