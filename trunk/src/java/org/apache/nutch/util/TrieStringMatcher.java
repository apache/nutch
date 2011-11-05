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


import java.util.Arrays;
import java.util.LinkedList;
import java.util.ListIterator;

/**
 * TrieStringMatcher is a base class for simple tree-based string
 * matching.
 *
 */
public abstract class TrieStringMatcher {
  protected TrieNode root;

  protected TrieStringMatcher() {
    this.root= new TrieNode('\000', false);
  }

  /**
   * Node class for the character tree.
   */
  protected class TrieNode implements Comparable<TrieNode> {
    protected TrieNode[] children;
    protected LinkedList<TrieNode> childrenList;
    protected char nodeChar;
    protected boolean terminal;

    /**
     * Creates a new TrieNode, which contains the given
     * <code>nodeChar</code>.  If <code>isTerminal</code> is
     * <code>true</code>, the new node is a <em>terminal</em> node in
     * the trie.
     */  
    TrieNode(char nodeChar, boolean isTerminal) {
      this.nodeChar= nodeChar;
      this.terminal= isTerminal;
      this.childrenList= new LinkedList<TrieNode>();
    }

    /**
     * Returns <code>true</code> if this node is a <em>terminal</em>
     * node in the trie.
     */
    boolean isTerminal() {
      return terminal;
    }

    /**
     * Returns the child node of this node whose node-character is
     * <code>nextChar</code>.  If no such node exists, one will be is
     * added.  If <em>isTerminal</em> is <code>true</code>, the node 
     * will be a terminal node in the trie.
     */
    TrieNode getChildAddIfNotPresent(char nextChar, boolean isTerminal) {
      if (childrenList == null) {
        childrenList= new LinkedList<TrieNode>();
        childrenList.addAll(Arrays.asList(children));
        children= null;
      }

      if (childrenList.size() == 0) {
        TrieNode newNode= new TrieNode(nextChar, isTerminal);
        childrenList.add(newNode);
        return newNode;
      }

      ListIterator<TrieNode> iter= childrenList.listIterator();
      TrieNode node= iter.next();
      while ( (node.nodeChar < nextChar) && iter.hasNext() ) 
        node= iter.next();
                        
      if (node.nodeChar == nextChar) {
        node.terminal= node.terminal | isTerminal;
        return node;
      }

      if (node.nodeChar > nextChar) 
        iter.previous();

      TrieNode newNode= new TrieNode(nextChar, isTerminal);
      iter.add(newNode);
      return newNode;                   
    }

    /**
     * Returns the child node of this node whose node-character is
     * <code>nextChar</code>.  If no such node exists,
     * <code>null</code> is returned.
     */
    TrieNode getChild(char nextChar) {
      if (children == null) {
        children= childrenList.toArray(new TrieNode[childrenList.size()]);
        childrenList= null;
        Arrays.sort(children);
      }

      int min= 0;
      int max= children.length - 1;
      int mid= 0;
      while (min < max) {
        mid= (min + max) / 2;
        if (children[mid].nodeChar == nextChar) 
          return children[mid];
        if (children[mid].nodeChar < nextChar)
          min= mid + 1;
        else // if (children[mid].nodeChar > nextChar)
          max= mid - 1;
      }

      if (min == max) 
        if (children[min].nodeChar == nextChar)
          return children[min];

      return null;
    }

    public int compareTo(TrieNode other) {
      if (this.nodeChar < other.nodeChar) 
        return -1;
      if (this.nodeChar == other.nodeChar) 
        return 0;
//    if (this.nodeChar > other.nodeChar) 
      return 1;
    }
  }

  /**
   * Returns the next {@link TrieNode} visited, given that you are at
   * <code>node</code>, and the the next character in the input is 
   * the <code>idx</code>'th character of <code>s</code>.
   */
  protected final TrieNode matchChar(TrieNode node, String s, int idx) {
    return node.getChild(s.charAt(idx));
  }

  /**
   * Adds any necessary nodes to the trie so that the given
   * <code>String</code> can be decoded and the last character is
   * represented by a terminal node.  Zero-length <code>Strings</code>
   * are ignored.
   */
  protected final void addPatternForward(String s) {
    TrieNode node= root;
    int stop= s.length() - 1;
    int i;
    if (s.length() > 0) {
      for (i= 0; i < stop; i++)
        node= node.getChildAddIfNotPresent(s.charAt(i), false);
      node= node.getChildAddIfNotPresent(s.charAt(i), true);
    }
  }

  /**
   * Adds any necessary nodes to the trie so that the given
   * <code>String</code> can be decoded <em>in reverse</em> and the
   * first character is represented by a terminal node.  Zero-length
   * <code>Strings</code> are ignored.
   */
  protected final void addPatternBackward(String s) {
    TrieNode node= root;
    if (s.length() > 0) {
      for (int i= s.length()-1; i > 0; i--) 
        node= node.getChildAddIfNotPresent(s.charAt(i), false);
      node= node.getChildAddIfNotPresent(s.charAt(0), true);
    }
  }

  /**
   * Returns true if the given <code>String</code> is matched by a
   * pattern in the trie
   */
  public abstract boolean matches(String input);

  /**
   * Returns the shortest substring of <code>input<code> that is
   * matched by a pattern in the trie, or <code>null<code> if no match
   * exists.
   */
  public abstract String shortestMatch(String input);

  /**
   * Returns the longest substring of <code>input<code> that is
   * matched by a pattern in the trie, or <code>null<code> if no match
   * exists.
   */
  public abstract String longestMatch(String input);

}
