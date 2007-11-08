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

package org.apache.nutch.parse.html;

import java.net.URL;
import java.net.MalformedURLException;
import java.util.Collection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Stack;

import org.apache.nutch.parse.Outlink;
import org.apache.nutch.util.NodeWalker;
import org.apache.hadoop.conf.Configuration;

import org.w3c.dom.*;

/**
 * A collection of methods for extracting content from DOM trees.
 * 
 * This class holds a few utility methods for pulling content out of 
 * DOM nodes, such as getOutlinks, getText, etc.
 *
 */
public class DOMContentUtils {

  public static class LinkParams {
    public String elName;
    public String attrName;
      public int childLen;
      
      public LinkParams(String elName, String attrName, int childLen) {
          this.elName = elName;
          this.attrName = attrName;
          this.childLen = childLen;
      }
      
      public String toString() {
          return "LP[el=" + elName + ",attr=" + attrName + ",len=" + childLen + "]";
      }
  }
  
  private HashMap linkParams = new HashMap();
  private Configuration conf;
  
  public DOMContentUtils(Configuration conf) {
    setConf(conf);
  }
  
  public void setConf(Configuration conf) {
    // forceTags is used to override configurable tag ignoring, later on
    Collection<String> forceTags = new ArrayList<String>(1);

    this.conf = conf;
    linkParams.clear();
    linkParams.put("a", new LinkParams("a", "href", 1));
    linkParams.put("area", new LinkParams("area", "href", 0));
    if (conf.getBoolean("parser.html.form.use_action", true)) {
      linkParams.put("form", new LinkParams("form", "action", 1));
      if (conf.get("parser.html.form.use_action") != null)
        forceTags.add("form");
    }
    linkParams.put("frame", new LinkParams("frame", "src", 0));
    linkParams.put("iframe", new LinkParams("iframe", "src", 0));
    linkParams.put("script", new LinkParams("script", "src", 0));
    linkParams.put("link", new LinkParams("link", "href", 0));
    linkParams.put("img", new LinkParams("img", "src", 0));

    // remove unwanted link tags from the linkParams map
    String[] ignoreTags = conf.getStrings("parser.html.outlinks.ignore_tags");
    for ( int i = 0 ; ignoreTags != null && i < ignoreTags.length ; i++ ) {
      if ( ! forceTags.contains(ignoreTags[i]) )
        linkParams.remove(ignoreTags[i]);
    }
  }
  
  /**
   * This method takes a {@link StringBuffer} and a DOM {@link Node},
   * and will append all the content text found beneath the DOM node to 
   * the <code>StringBuffer</code>.
   *
   * <p>
   *
   * If <code>abortOnNestedAnchors</code> is true, DOM traversal will
   * be aborted and the <code>StringBuffer</code> will not contain
   * any text encountered after a nested anchor is found.
   * 
   * <p>
   *
   * @return true if nested anchors were found
   */
  public boolean getText(StringBuffer sb, Node node, 
                                      boolean abortOnNestedAnchors) {
    if (getTextHelper(sb, node, abortOnNestedAnchors, 0)) {
      return true;
    } 
    return false;
  }


  /**
   * This is a convinience method, equivalent to {@link
   * #getText(StringBuffer,Node,boolean) getText(sb, node, false)}.
   * 
   */
  public void getText(StringBuffer sb, Node node) {
    getText(sb, node, false);
  }

  // returns true if abortOnNestedAnchors is true and we find nested 
  // anchors
  private boolean getTextHelper(StringBuffer sb, Node node, 
                                             boolean abortOnNestedAnchors,
                                             int anchorDepth) {
    boolean abort = false;
    NodeWalker walker = new NodeWalker(node);
    
    while (walker.hasNext()) {
    
      Node currentNode = walker.nextNode();
      String nodeName = currentNode.getNodeName();
      short nodeType = currentNode.getNodeType();
      
      if ("script".equalsIgnoreCase(nodeName)) {
        walker.skipChildren();
      }
      if ("style".equalsIgnoreCase(nodeName)) {
        walker.skipChildren();
      }
      if (abortOnNestedAnchors && "a".equalsIgnoreCase(nodeName)) {
        anchorDepth++;
        if (anchorDepth > 1) {
          abort = true;
          break;
        }        
      }
      if (nodeType == Node.COMMENT_NODE) {
        walker.skipChildren();
      }
      if (nodeType == Node.TEXT_NODE) {
        // cleanup and trim the value
        String text = currentNode.getNodeValue();
        text = text.replaceAll("\\s+", " ");
        text = text.trim();
        if (text.length() > 0) {
          if (sb.length() > 0) sb.append(' ');
        	sb.append(text);
        }
      }
    }
    
    return abort;
  }

  /**
   * This method takes a {@link StringBuffer} and a DOM {@link Node},
   * and will append the content text found beneath the first
   * <code>title</code> node to the <code>StringBuffer</code>.
   *
   * @return true if a title node was found, false otherwise
   */
  public boolean getTitle(StringBuffer sb, Node node) {
    
    NodeWalker walker = new NodeWalker(node);
    
    while (walker.hasNext()) {
  
      Node currentNode = walker.nextNode();
      String nodeName = currentNode.getNodeName();
      short nodeType = currentNode.getNodeType();
      
      if ("body".equalsIgnoreCase(nodeName)) { // stop after HEAD
        return false;
      }
  
      if (nodeType == Node.ELEMENT_NODE) {
        if ("title".equalsIgnoreCase(nodeName)) {
          getText(sb, currentNode);
          return true;
        }
      }
    }      
    
    return false;
  }

  /** If Node contains a BASE tag then it's HREF is returned. */
  public URL getBase(Node node) {

    NodeWalker walker = new NodeWalker(node);
    
    while (walker.hasNext()) {
  
      Node currentNode = walker.nextNode();
      String nodeName = currentNode.getNodeName();
      short nodeType = currentNode.getNodeType();
      
      // is this node a BASE tag?
      if (nodeType == Node.ELEMENT_NODE) {
  
        if ("body".equalsIgnoreCase(nodeName)) { // stop after HEAD
          return null;
        }
  
        if ("base".equalsIgnoreCase(nodeName)) {
          NamedNodeMap attrs = currentNode.getAttributes();
          for (int i= 0; i < attrs.getLength(); i++ ) {
            Node attr = attrs.item(i);
            if ("href".equalsIgnoreCase(attr.getNodeName())) {
              try {
                return new URL(attr.getNodeValue());
              } catch (MalformedURLException e) {}
            }
          }
        }
      }
    }

    // no.
    return null;
  }


  private boolean hasOnlyWhiteSpace(Node node) {
    String val= node.getNodeValue();
    for (int i= 0; i < val.length(); i++) {
      if (!Character.isWhitespace(val.charAt(i)))
        return false;
    }
    return true;
  }

  // this only covers a few cases of empty links that are symptomatic
  // of nekohtml's DOM-fixup process...
  private boolean shouldThrowAwayLink(Node node, NodeList children, 
                                              int childLen, LinkParams params) {
    if (childLen == 0) {
      // this has no inner structure 
      if (params.childLen == 0) return false;
      else return true;
    } else if ((childLen == 1) 
               && (children.item(0).getNodeType() == Node.ELEMENT_NODE)
               && (params.elName.equalsIgnoreCase(children.item(0).getNodeName()))) { 
      // single nested link
      return true;

    } else if (childLen == 2) {

      Node c0= children.item(0);
      Node c1= children.item(1);

      if ((c0.getNodeType() == Node.ELEMENT_NODE)
          && (params.elName.equalsIgnoreCase(c0.getNodeName()))
          && (c1.getNodeType() == Node.TEXT_NODE) 
          && hasOnlyWhiteSpace(c1) ) {
        // single link followed by whitespace node
        return true;
      }

      if ((c1.getNodeType() == Node.ELEMENT_NODE)
          && (params.elName.equalsIgnoreCase(c1.getNodeName()))
          && (c0.getNodeType() == Node.TEXT_NODE) 
          && hasOnlyWhiteSpace(c0) ) {
        // whitespace node followed by single link
        return true;
      }

    } else if (childLen == 3) {
      Node c0= children.item(0);
      Node c1= children.item(1);
      Node c2= children.item(2);
      
      if ((c1.getNodeType() == Node.ELEMENT_NODE)
          && (params.elName.equalsIgnoreCase(c1.getNodeName()))
          && (c0.getNodeType() == Node.TEXT_NODE) 
          && (c2.getNodeType() == Node.TEXT_NODE) 
          && hasOnlyWhiteSpace(c0)
          && hasOnlyWhiteSpace(c2) ) {
        // single link surrounded by whitespace nodes
        return true;
      }
    }

    return false;
  }
  
  /**
   * Handles cases where the url param information is encoded into the base
   * url as opposed to the target.
   * <p>
   * If the taget contains params (i.e. ';xxxx') information then the target 
   * params information is assumed to be correct and any base params information
   * is ignored.  If the base contains params information but the tareget does
   * not, then the params information is moved to the target allowing it to be
   * correctly determined by the java.net.URL class.
   * 
   * @param base The base URL.
   * @param target The target path from the base URL.
   * 
   * @return URL A URL with the params information correctly encoded.
   * 
   * @throws MalformedURLException If the url is not a well formed URL.
   */
  private URL fixEmbeddedParams(URL base, String target) 
    throws MalformedURLException{
    
    // the target contains params information or the base doesn't then no
    // conversion necessary, return regular URL
    if (target.indexOf(';') >= 0 || base.toString().indexOf(';') == -1) {
      return new URL(base, target);
    }
    
    // get the base url and it params information
    String baseURL = base.toString();
    int startParams = baseURL.indexOf(';');
    String params = baseURL.substring(startParams);
    
    // if the target has a query string then put the params information after
    // any path but before the query string, otherwise just append to the path
    int startQS = target.indexOf('?');
    if (startQS >= 0) {
      target = target.substring(0, startQS) + params + 
        target.substring(startQS);
    }
    else {
      target += params;
    }
    
    return new URL(base, target);
  }

  /**
   * This method finds all anchors below the supplied DOM
   * <code>node</code>, and creates appropriate {@link Outlink}
   * records for each (relative to the supplied <code>base</code>
   * URL), and adds them to the <code>outlinks</code> {@link
   * ArrayList}.
   *
   * <p>
   *
   * Links without inner structure (tags, text, etc) are discarded, as
   * are links which contain only single nested links and empty text
   * nodes (this is a common DOM-fixup artifact, at least with
   * nekohtml).
   */
  public void getOutlinks(URL base, ArrayList outlinks, 
                                       Node node) {
    
    NodeWalker walker = new NodeWalker(node);
    while (walker.hasNext()) {
      
      Node currentNode = walker.nextNode();
      String nodeName = currentNode.getNodeName();
      short nodeType = currentNode.getNodeType();      
      NodeList children = currentNode.getChildNodes();
      int childLen = (children != null) ? children.getLength() : 0; 
      
      if (nodeType == Node.ELEMENT_NODE) {
        
        nodeName = nodeName.toLowerCase();
        LinkParams params = (LinkParams)linkParams.get(nodeName);
        if (params != null) {
          if (!shouldThrowAwayLink(currentNode, children, childLen, params)) {
  
            StringBuffer linkText = new StringBuffer();
            getText(linkText, currentNode, true);
  
            NamedNodeMap attrs = currentNode.getAttributes();
            String target = null;
            boolean noFollow = false;
            boolean post = false;
            for (int i= 0; i < attrs.getLength(); i++ ) {
              Node attr = attrs.item(i);
              String attrName = attr.getNodeName();
              if (params.attrName.equalsIgnoreCase(attrName)) {
                target = attr.getNodeValue();
              } else if ("rel".equalsIgnoreCase(attrName) &&
                         "nofollow".equalsIgnoreCase(attr.getNodeValue())) {
                noFollow = true;
              } else if ("method".equalsIgnoreCase(attrName) &&
                         "post".equalsIgnoreCase(attr.getNodeValue())) {
                post = true;
              }
            }
            if (target != null && !noFollow && !post)
              try {
                
                URL url = (base.toString().indexOf(';') > 0) ? 
                  fixEmbeddedParams(base, target) :  new URL(base, target);
                outlinks.add(new Outlink(url.toString(),
                                         linkText.toString().trim()));
              } catch (MalformedURLException e) {
                // don't care
              }
          }
          // this should not have any children, skip them
          if (params.childLen == 0) continue;
        }
      }
    }
  }

}

