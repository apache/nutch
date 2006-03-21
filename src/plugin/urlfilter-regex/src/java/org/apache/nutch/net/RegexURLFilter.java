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

// JDK imports
import java.io.Reader;
import java.io.IOException;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

// Hadoop imports
import org.apache.hadoop.conf.Configuration;


/**
 * Filters URLs based on a file of regular expressions using the
 * {@link java.util.regex Java Regex implementation}.
 */
public class RegexURLFilter extends RegexURLFilterBase {

  public RegexURLFilter() {
    super();
  }

  public RegexURLFilter(String filename)
    throws IOException, PatternSyntaxException {
    super(filename);
  }

  RegexURLFilter(Reader reader)
    throws IOException, IllegalArgumentException {
    super(reader);
  }

  
  /* ----------------------------------- *
   * <implementation:RegexURLFilterBase> *
   * ----------------------------------- */
  
  // Inherited Javadoc
  protected String getRulesFile(Configuration conf) {
    return conf.get("urlfilter.regex.file");
  }

  // Inherited Javadoc
  protected RegexRule createRule(boolean sign, String regex) {
    return new Rule(sign, regex);
  }
  
  /* ------------------------------------ *
   * </implementation:RegexURLFilterBase> *
   * ------------------------------------ */

  
  public static void main(String args[]) throws IOException {
    main(new RegexURLFilter(), args);
  }


  private class Rule extends RegexRule {
    
    private Pattern pattern;
    
    Rule(boolean sign, String regex) {
      super(sign, regex);
      pattern = Pattern.compile(regex);
    }

    protected boolean match(String url) {
      return pattern.matcher(url).find();
    }
  }
  
}
