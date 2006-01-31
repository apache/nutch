/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nutch.net;

import java.net.URL;
import java.net.MalformedURLException;
import java.io.IOException;
// import java.net.URI;
// import java.net.URISyntaxException;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.logging.Logger;
import org.apache.nutch.util.LogFormatter;
import org.apache.nutch.util.NutchConf;

import javax.xml.parsers.*;
import org.w3c.dom.*;
import org.apache.oro.text.regex.*;

import org.apache.nutch.util.*;

/** Allows users to do regex substitutions on all/any URLs that are encountered, which
 * is useful for stripping session IDs from URLs.
 *
 * <p>This class must be specified as the URL normalizer to be used in <tt>nutch-site.xml</tt>
 * or <tt>nutch-default.xml</tt>.  To do this specify the <tt>urlnormalizer.class</tt> property to
 * have the value:  <tt>org.apache.nutch.net.RegexUrlNormalizer</tt>.  The <tt>urlnormalizer.regex.file</tt>
 * property should also be set to the file name of an xml file which should contain the patterns
 * and substitutions to be done on encountered URLs.</p>
 *
 * @author Luke Baker
 */
public class RegexUrlNormalizer extends BasicUrlNormalizer
  implements UrlNormalizer {

    /** Class which holds a compiled pattern and its corresponding substition string. */
    private static class Rule {
      public Perl5Pattern pattern;
      public String substitution;	
    }
    
    private List rules;
    private PatternMatcher matcher = new Perl5Matcher();
    
    /** The default constructor which is called from UrlNormalizerFactory (normalizerClass.newInstance()) in method: getNormalizer()**/
    public RegexUrlNormalizer()  {}
    
    /** Constructor which can be passed the file name, so it doesn't look in the configuration files for it. */
    public RegexUrlNormalizer(String filename)
      throws IOException, MalformedPatternException {
      //URL url= NutchConf.get().getResource(filename);
      rules = readConfigurationFile(filename);
    }
    
    
    /** This function does the replacements by iterating through all the regex patterns.
      * It accepts a string url as input and returns the altered string. */
    public synchronized String regexNormalize(String urlString) {
      Iterator i=rules.iterator();
      while(i.hasNext()) {
        Rule r=(Rule) i.next();
        urlString = Util.substitute(matcher, r.pattern, 
          new Perl5Substitution(r.substitution), urlString, Util.SUBSTITUTE_ALL); // actual substitution
      }
      return urlString;
    }
   
    /** Normalizes any URLs by calling super.basicNormalize()
      * and regexSub(). This is the function that gets called
      * elsewhere in Nutch. */
    public synchronized String normalize(String urlString)
      throws MalformedURLException {
        urlString = super.normalize(urlString); // run basicNormalize first to ready for regexNormalize
        urlString = regexNormalize(urlString);
        urlString = super.normalize(urlString); // make sure regexNormalize didn't screw up the URL
        return urlString;
  }
  
  
  
  /** Reads the configuration file and populates a List of Rules. */
  private List readConfigurationFile(String filename)
    throws IOException, MalformedPatternException {

    Perl5Compiler compiler=new Perl5Compiler();
    List rules=new ArrayList();
    try {
      
      LOG.info("loading " + filename);
      // borrowed heavily from code in NutchConf.java
      Document doc =
        DocumentBuilderFactory.newInstance().newDocumentBuilder()
        .parse(filename);
      Element root = doc.getDocumentElement();
      if (!"regex-normalize".equals(root.getTagName()))
        LOG.severe("bad conf file: top-level element not <regex-normalize>");
      NodeList regexes = root.getChildNodes();
      for (int i = 0; i < regexes.getLength(); i++) {
        Node regexNode = regexes.item(i);
        if (!(regexNode instanceof Element))
          continue;
        Element regex = (Element)regexNode;
        if (!"regex".equals(regex.getTagName()))
          LOG.warning("bad conf file: element not <regex>");
        NodeList fields = regex.getChildNodes();
        String patternValue = null;
        String subValue = null;
        for (int j = 0; j < fields.getLength(); j++) {
          Node fieldNode = fields.item(j);
          if (!(fieldNode instanceof Element))
            continue;
          Element field = (Element)fieldNode;
          if ("pattern".equals(field.getTagName()) && field.hasChildNodes())
            patternValue = ((Text)field.getFirstChild()).getData();
          if ("substitution".equals(field.getTagName()) && field.hasChildNodes())
            subValue = ((Text)field.getFirstChild()).getData();
          if (!field.hasChildNodes())
            subValue = "";
        }
        if (patternValue != null && subValue != null) {
          Rule rule=new Rule();
          rule.pattern=(Perl5Pattern) compiler.compile(patternValue);
          rule.substitution=subValue;
          rules.add(rule);
        }
      }
        
    } catch (Exception e) {
      LOG.severe("error parsing " + filename +" conf file: " + e);
    }
    return rules;
  }
  
  public void setConf(NutchConf conf) {
    super.setConf(conf);
    // the default constructor was called
    if (this.rules == null) {
      String filename = getConf().get("urlnormalizer.regex.file");
      URL url = getConf().getResource(filename);
      try {
        this.rules = readConfigurationFile(url.toString());
      } catch (IOException e) {
        // TODO mb@media-style.com: throw Exception? Because broken api.
        throw new RuntimeException(e.getMessage(), e);
      } catch (MalformedPatternException e) {
        // TODO mb@media-style.com: throw Exception? Because broken api.
        throw new RuntimeException(e.getMessage(), e);
      }
    }

  }
    
  /** Spits out patterns and substitutions that are in the configuration file. */
  public static void main(String args[])
    throws MalformedPatternException, IOException {
      RegexUrlNormalizer normalizer = new RegexUrlNormalizer();
      normalizer.setConf(new NutchConf());
      Iterator i=normalizer.rules.iterator();
      while(i.hasNext()) {
        Rule r=(Rule) i.next();
        System.out.print(r.pattern.getPattern() + "  ");
        System.out.println(r.substitution);
      }
    }
  
}
