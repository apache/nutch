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
package org.apache.nutch.util;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * A class for efficiently matching <code>String</code>s against a set of
 * prefixes.
 */
public class PrefixStringMatcher extends TrieStringMatcher {

  /**
   * Creates a new <code>PrefixStringMatcher</code> which will match
   * <code>String</code>s with any prefix in the supplied array. Zero-length
   * <code>Strings</code> are ignored.
   */
  public PrefixStringMatcher(String[] prefixes) {
    super();
    for (int i = 0; i < prefixes.length; i++)
      addPatternForward(prefixes[i]);
  }

  /**
   * Creates a new <code>PrefixStringMatcher</code> which will match
   * <code>String</code>s with any prefix in the supplied
   * <code>Collection</code>.
   * 
   * @throws ClassCastException
   *           if any <code>Object</code>s in the collection are not
   *           <code>String</code>s
   */
  public PrefixStringMatcher(Collection<String> prefixes) {
    super();
    Iterator<String> iter = prefixes.iterator();
    while (iter.hasNext())
      addPatternForward(iter.next());
  }

  /**
   * Returns true if the given <code>String</code> is matched by a prefix in the
   * trie
   */
  public boolean matches(String input) {
    TrieNode node = root;
    for (int i = 0; i < input.length(); i++) {
      node = node.getChild(input.charAt(i));
      if (node == null)
        return false;
      if (node.isTerminal())
        return true;
    }
    return false;
  }

  /**
   * Returns the shortest prefix of <code>input</code> that is matched,
   * or <code>null</code> if no match exists.
   */
  public String shortestMatch(String input) {
    TrieNode node = root;
    for (int i = 0; i < input.length(); i++) {
      node = node.getChild(input.charAt(i));
      if (node == null)
        return null;
      if (node.isTerminal())
        return input.substring(0, i + 1);
    }
    return null;
  }

  /**
   * Returns the longest prefix of <code>input</code> that is matched,
   * or <code>null</code> if no match exists.
   */
  public String longestMatch(String input) {
    TrieNode node = root;
    String result = null;
    for (int i = 0; i < input.length(); i++) {
      node = node.getChild(input.charAt(i));
      if (node == null)
        break;
      if (node.isTerminal())
        result = input.substring(0, i + 1);
    }
    return result;
  }

  public static final void main(String[] argv) {
    String[] prefixes = new String[] { "abcd", "abc", "aac", "baz", "foo",
        "foobar" };
    PrefixStringMatcher matcher = new PrefixStringMatcher(prefixes);

    String[] tests = { "a", "ab", "abc", "abcdefg", "apple", "aa", "aac",
        "aaccca", "abaz", "baz", "bazooka", "fo", "foobar", "kite", };

    for (int i = 0; i < tests.length; i++) {
      System.out.println("testing: " + tests[i]);
      System.out.println("   matches: " + matcher.matches(tests[i]));
      System.out.println("  shortest: " + matcher.shortestMatch(tests[i]));
      System.out.println("   longest: " + matcher.longestMatch(tests[i]));
    }

    int iterations = 1000;
    System.out.println("Testing thread-safety (NUTCH-2585) with " + iterations
        + " iterations:");
    List<String> testsList = Arrays.asList(tests);
    for (int i = 0; i < iterations; i++) {
      matcher = new PrefixStringMatcher(prefixes);
      Collections.shuffle(testsList);
      try {
        long count = testsList.parallelStream().filter(matcher::matches).count();
        System.out.print(String.format("Cycle %4d : %d matches\r", i, count));
      } catch (Exception e) {
        // flush output
        System.out.println("");
        throw e;
      }
    }
    System.out.println("");
  }
}
