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

package org.apache.nutch.util;

import java.util.Arrays;
import java.util.HashMap;

/**
 * A Fibonacci Heap, as described in <i>Introduction to Algorithms</i> by
 * Charles E. Leiserson, Thomas H. Cormen, Ronald L. Rivest.
 *
 * <p>
 *
 * A Fibonacci heap is a very efficient data structure for priority
 * queuing.  
 *
 */
public class FibonacciHeap {
  private FibonacciHeapNode min;
  private HashMap itemsToNodes;

  // private node class
  private static class FibonacciHeapNode {
    private Object userObject;
    private int priority;

    private FibonacciHeapNode parent;
    private FibonacciHeapNode prevSibling;
    private FibonacciHeapNode nextSibling;
    private FibonacciHeapNode child;
    private int degree;
    private boolean mark;

    FibonacciHeapNode(Object userObject, int priority) {
      this.userObject= userObject;
      this.priority= priority;

      this.parent= null;
      this.prevSibling= this;
      this.nextSibling= this;
      this.child= null;
      this.degree= 0;
      this.mark= false;
    }

    public String toString() {
      return "["+userObject+", "+degree+"]";
    }
  }

  /**
   * Creates a new <code>FibonacciHeap</code>.
   */
  public FibonacciHeap() {
    this.min= null;
    this.itemsToNodes= new HashMap();
  }

  /**
   *  Adds the Object <code>item</code>, with the supplied
   *  <code>priority</code>.
   */
  public void add(Object item, int priority) {
    if (itemsToNodes.containsKey(item))
      throw new IllegalStateException("heap already contains item! (item= "
                                      + item + ")");
    FibonacciHeapNode newNode= new FibonacciHeapNode(item, priority);
    itemsToNodes.put(item, newNode);

    if (min == null) {
      min= newNode;
    } else {
      concatenateSiblings(newNode, min);
      if (newNode.priority < min.priority) 
        min= newNode;
    }
  }

  /**
   * Returns <code>true</code> if <code>item</code> exists in this
   * <code>FibonacciHeap</code>, false otherwise.
   */
  public boolean contains(Object item) {
    return itemsToNodes.containsKey(item);
  }

  // makes x's nextSibling and prevSibling point to itself
  private void removeFromSiblings(FibonacciHeapNode x) {
    if (x.nextSibling == x) 
      return;
    x.nextSibling.prevSibling= x.prevSibling;
    x.prevSibling.nextSibling= x.nextSibling;
    x.nextSibling= x;
    x.prevSibling= x;
  }

  // joins siblings lists of a and b
  private void concatenateSiblings(FibonacciHeapNode a, FibonacciHeapNode b) {
    a.nextSibling.prevSibling= b;
    b.nextSibling.prevSibling= a;
    FibonacciHeapNode origAnext= a.nextSibling;
    a.nextSibling= b.nextSibling;
    b.nextSibling= origAnext;
  }

  /**
   * Returns the same Object that {@link #popMin()} would, without
   * removing it.
   */ 
  public Object peekMin() {
    if (min == null) 
      return null;
    return min.userObject;
  }

  /**
   * Returns the number of objects in the heap.
   */
  public int size() {
    return itemsToNodes.size();
  }

  /**
   * Returns the object which has the <em>lowest</em> priority in the
   * heap.  If the heap is empty, <code>null</code> is returned.
   */
  public Object popMin() {
    if (min == null) 
      return null;
    if (min.child != null) {
      FibonacciHeapNode tmp= min.child;
      // rempve parent pointers to min
      while (tmp.parent != null) {
        tmp.parent= null;
        tmp= tmp.nextSibling;
      }
      // add children of min to root list
      concatenateSiblings(tmp, min);
    }
    // remove min from root list
    FibonacciHeapNode oldMin= min;
    if (min.nextSibling == min) {
      min= null;
    } else {
      min= min.nextSibling;
      removeFromSiblings(oldMin);
      consolidate();
    }
    itemsToNodes.remove(oldMin.userObject);
    return oldMin.userObject;
  }

  // consolidates heaps of same degree
  private void consolidate() {
    int size= size();
    FibonacciHeapNode[] newRoots= new FibonacciHeapNode[size];

    FibonacciHeapNode node= min;
    FibonacciHeapNode start= min;
    do {
      FibonacciHeapNode x= node;
      int currDegree= node.degree;
      while (newRoots[currDegree] != null) {
        FibonacciHeapNode y= newRoots[currDegree];
        if (x.priority > y.priority) {
          FibonacciHeapNode tmp= x;
          x= y;
          y= tmp;
        }
        if (y == start) {
          start= start.nextSibling;
        }
        if (y == node) {
          node= node.prevSibling;
        }
        link(y, x);
        newRoots[currDegree++]= null;
      }
      newRoots[currDegree]= x;
      node= node.nextSibling;
    } while (node != start);

    min= null;
    for (int i= 0; i < newRoots.length; i++) 
      if (newRoots[i] != null) {
        if ( (min == null) 
             || (newRoots[i].priority < min.priority) )
          min= newRoots[i];
      }
  }

  // links y under x
  private void link(FibonacciHeapNode y, FibonacciHeapNode x) {
    removeFromSiblings(y);
    y.parent= x;
    if (x.child == null) 
      x.child= y;
    else 
      concatenateSiblings(x.child, y);
    x.degree++;
    y.mark= false;
  }

  /**
   * Decreases the <code>priority</code> value associated with
   * <code>item</code>.
   *
   * <p>
   *
   * <code>item<code> must exist in the heap, and it's current
   * priority must be greater than <code>priority</code>.  
   *
   * @throws IllegalStateException if <code>item</code> does not exist
   * in the heap, or if <code>item</code> already has an equal or
   * lower priority than the supplied<code>priority</code>.
   */
  public void decreaseKey(Object item, int priority) {
    FibonacciHeapNode node= 
      (FibonacciHeapNode) itemsToNodes.get(item);
    if (node == null) 
      throw new IllegalStateException("No such element: " + item);
    if (node.priority < priority) 
      throw new IllegalStateException("decreaseKey(" + item + ", " 
                                      + priority + ") called, but priority="
                                      + node.priority);
    node.priority= priority;
    FibonacciHeapNode parent= node.parent;
    if ( (parent != null) && (node.priority < parent.priority) ) {
      cut(node, parent);
      cascadingCut(parent);
    }
    if (node.priority < min.priority) 
      min= node;

  }

  // cut node x from below y
  private void cut(FibonacciHeapNode x, FibonacciHeapNode y) {
    // remove x from y's children
    if (y.child == x) 
      y.child= x.nextSibling;
    if (y.child == x) 
      y.child= null;

    y.degree--;
    removeFromSiblings(x);
    concatenateSiblings(x, min);
    x.parent= null;
    x.mark= false;
                
  }

  private void cascadingCut(FibonacciHeapNode y) {
    FibonacciHeapNode z= y.parent;
    if (z != null) {
      if (!y.mark) {
        y.mark= true;
      } else {
        cut(y, z);
        cascadingCut(z);
      }
    }
  }

}
