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

package org.apache.nutch.util;

import org.junit.Test;
import static org.junit.Assert.*;

/** Unit tests for SuffixStringMatcher. */
public class TestSuffixStringMatcher {

  private final static int NUM_TEST_ROUNDS= 20;
  private final static int MAX_TEST_SUFFIXES= 100;
  private final static int MAX_SUFFIX_LEN= 10;
  private final static int NUM_TEST_INPUTS_PER_ROUND= 100;
  private final static int MAX_INPUT_LEN= 20;

  private final static char[] alphabet= 
    new char[] {
      'a', 'b', 'c', 'd',
//      'e', 'f', 'g', 'h', 'i', 'j',
//      'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't',
//      'u', 'v', 'w', 'x', 'y', 'z', '1', '2', '3', '4',
//      '5', '6', '7', '8', '9', '0'
    };

  private String makeRandString(int minLen, int maxLen) {
    int len= minLen + (int) (Math.random() * (maxLen - minLen));
    char[] chars= new char[len];
    
    for (int pos= 0; pos < len; pos++) {
      chars[pos]= alphabet[(int) (Math.random() * alphabet.length)];
    }
    
    return new String(chars);
  }
  
  @Test
  public void testSuffixMatcher() {
    int numMatches= 0;
    int numInputsTested= 0;

    for (int round= 0; round < NUM_TEST_ROUNDS; round++) {

      // build list of suffixes
      int numSuffixes= (int) (Math.random() * MAX_TEST_SUFFIXES);
      String[] suffixes= new String[numSuffixes];
      for (int i= 0; i < numSuffixes; i++) {
        suffixes[i]= makeRandString(0, MAX_SUFFIX_LEN);
      }

      SuffixStringMatcher sufmatcher= new SuffixStringMatcher(suffixes);

      // test random strings for suffix matches
      for (int i= 0; i < NUM_TEST_INPUTS_PER_ROUND; i++) {
        String input= makeRandString(0, MAX_INPUT_LEN);
        boolean matches= false;
        int longestMatch= -1;
        int shortestMatch= -1;

        for (int j= 0; j < suffixes.length; j++) {

          if ((suffixes[j].length() > 0) 
              && input.endsWith(suffixes[j])) {

            matches= true;
            int matchSize= suffixes[j].length();

            if (matchSize > longestMatch) 
              longestMatch= matchSize;

            if ( (matchSize < shortestMatch)
                 || (shortestMatch == -1) )
              shortestMatch= matchSize;
          }

        }

        if (matches) 
          numMatches++;

        numInputsTested++;

        assertTrue( "'" + input + "' should " + (matches ? "" : "not ") 
                    + "match!",
                    matches == sufmatcher.matches(input) );
        if (matches) {
          assertTrue( shortestMatch 
                      == sufmatcher.shortestMatch(input).length());
          assertTrue( input.substring(input.length() - shortestMatch).equals(
                        sufmatcher.shortestMatch(input)) );

          assertTrue( longestMatch 
                      == sufmatcher.longestMatch(input).length());
          assertTrue( input.substring(input.length() - longestMatch).equals(
                        sufmatcher.longestMatch(input)) );

        }
      }
    }

    System.out.println("got " + numMatches + " matches out of " 
                       + numInputsTested + " tests");
  }

}
