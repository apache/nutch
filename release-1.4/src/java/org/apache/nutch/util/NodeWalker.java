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

import java.util.Stack;

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * <p>A utility class that allows the walking of any DOM tree using a stack 
 * instead of recursion.  As the node tree is walked the next node is popped
 * off of the stack and all of its children are automatically added to the 
 * stack to be called in tree order.</p>
 * 
 * <p>Currently this class is not thread safe.  It is assumed that only one
 * thread will be accessing the <code>NodeWalker</code> at any given time.</p>
 */
public class NodeWalker {

  // the root node the the stack holding the nodes
  private Node currentNode;
  private NodeList currentChildren;
  private Stack<Node> nodes;
  
  /**
   * Starts the <code>Node</code> tree from the root node.
   * 
   * @param rootNode
   */
  public NodeWalker(Node rootNode) {

    nodes = new Stack<Node>();
    nodes.add(rootNode);
  }
  
  /**
   * <p>Returns the next <code>Node</code> on the stack and pushes all of its
   * children onto the stack, allowing us to walk the node tree without the
   * use of recursion.  If there are no more nodes on the stack then null is
   * returned.</p>
   * 
   * @return Node The next <code>Node</code> on the stack or null if there
   * isn't a next node.
   */
  public Node nextNode() {
    
    // if no next node return null
    if (!hasNext()) {
      return null;
    }
    
    // pop the next node off of the stack and push all of its children onto
    // the stack
    currentNode = nodes.pop();
    currentChildren = currentNode.getChildNodes();
    int childLen = (currentChildren != null) ? currentChildren.getLength() : 0;
    
    // put the children node on the stack in first to last order
    for (int i = childLen - 1; i >= 0; i--) {
      nodes.add(currentChildren.item(i));
    }
    
    return currentNode;
  }
  
  /**
   * <p>Skips over and removes from the node stack the children of the last
   * node.  When getting a next node from the walker, that node's children 
   * are automatically added to the stack.  You can call this method to remove
   * those children from the stack.</p>
   * 
   * <p>This is useful when you don't want to process deeper into the 
   * current path of the node tree but you want to continue processing sibling
   * nodes.</p>
   *
   */
  public void skipChildren() {
    
    int childLen = (currentChildren != null) ? currentChildren.getLength() : 0;
    
    for (int i = 0 ; i < childLen ; i++) {
      Node child = nodes.peek();
      if (child.equals(currentChildren.item(i))) {
        nodes.pop();
      }
    }
  }
  
  /**
   * Returns true if there are more nodes on the current stack.
   * @return
   */
  public boolean hasNext() {
    return (nodes.size() > 0);
  }
}
