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
package org.apache.nutch.urlfilter.ignoreexempt.bidirectional;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.net.URLExemptionFilter;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.URLUtil;
import org.apache.nutch.urlfilter.api.RegexRule;
import org.apache.nutch.urlfilter.regex.RegexURLFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.net.MalformedURLException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.Arrays;
import java.util.regex.Pattern;
import java.util.List;
import java.util.ArrayList;


/**
 * This implementation of {@link org.apache.nutch.net.URLExemptionFilter} uses regex configuration for both fromUrl and toUrl
 * to check if URL is eligible for exemption from 'db.ignore.external'.
 * When this filter is enabled, the external urls will be checked against configured sequence of regex rules.
 *<p>
 * The exemption rule file defaults to db-ignore-external-exemptions-bidirectional.txt in the classpath but can be
 * overridden using the property  <code>"db.ignore.external.exemptions.bidirectional.file" in ./conf/nutch-*.xml</code>
 *</p>
 * @since Mar 1, 2018
 * @version 1
 * @see org.apache.nutch.net.URLExemptionFilter
 * @see org.apache.nutch.urlfilter.regex.RegexURLFilter
 */
public class BidirectionalExemptionUrlFilter extends RegexURLFilter
    implements URLExemptionFilter {

  public static final String DB_IGNORE_EXTERNAL_EXEMPTIONS_BIDIRECTIONAL_FILE
      = "db.ignore.external.exemptions.bidirectional.file";
  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  @Override
  //This implementation checks rules exceptions for two arbitrary urls. True if reg_ex(toUrl) = fromUrl
  public boolean filter(String fromUrl, String toUrl) {
	
	    String sourceHost = URLUtil.getHost(fromUrl).toLowerCase();
	    String sourceDestination = URLUtil.getHost(toUrl).toLowerCase();
	    
	    if (LOG.isDebugEnabled()) {
	      LOG.debug("BidirectionalExemptionUrlFilter. Source url: " + fromUrl + " and destination url " + toUrl);
	    }

	    String modifiedSourceHost = sourceHost; 
	    String modifiedDestinationHost = sourceDestination;
	    for (RegexRule rule : super.getRules()) {
	    	
	      if (LOG.isDebugEnabled()) {
	        LOG.debug("Applying rule [" + rule.regex() + "]");
	      }

	      modifiedSourceHost = rule.replace(modifiedSourceHost, "");
	      modifiedDestinationHost = rule.replace(modifiedDestinationHost, "");
	    };
	   
	    return modifiedSourceHost.equals(modifiedDestinationHost);
  }

 
  protected Reader getRulesReader(Configuration conf)
      throws IOException {
    String fileRules = conf.get(DB_IGNORE_EXTERNAL_EXEMPTIONS_BIDIRECTIONAL_FILE);
    this.getConf();
    return conf.getConfResourceAsReader(fileRules);
  }
  public static void main(String[] args) {

    if (args.length != 2) {
      System.out.println("Error: Invalid Args");
      System.out.println("Usage: " +
          BidirectionalExemptionUrlFilter.class.getName() + " <url_source, url_destination>");
      return;
    }
    String sourceUrl = args[0];
    String destinationUrl = args[1];
    BidirectionalExemptionUrlFilter instance = new BidirectionalExemptionUrlFilter();
    instance.setConf(NutchConfiguration.create());
    System.out.println(instance.filter(sourceUrl, destinationUrl));
  }
}
