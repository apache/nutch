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

package org.apache.nutch.parse.html;

import java.net.URL;

import org.w3c.dom.*;
import org.w3c.dom.html.*;
import org.apache.html.dom.*;

/**
 * Class for parsing META Directives from DOM trees.  This class
 * currently handles Robots META directives (all, none, nofollow,
 * noindex), finding BASE HREF tags, and HTTP-EQUIV no-cache
 * instructions.
 */
public class RobotsMetaProcessor {

  /**
   * Utility class with indicators for the robots directives "noindex"
   * and "nofollow", and HTTP-EQUIV/no-cache
   */
  public static class RobotsMetaIndicator {
    private boolean noIndex= false;
    private boolean noFollow= false;
    private boolean noCache= false;
    private URL baseHref= null;

    /** 
     * Sets <code>noIndex</code>, <code>noFollow</code> and
     * <code>noCache</code> to <code>false</code>.
     */
    public void reset() {
      noIndex= false;
      noFollow= false;
      noCache= false;
      baseHref= null;
    }

    /** 
     * Sets <code>noFollow</code> to <code>true</code>.
     */
    public void setNoFollow() {
      noFollow= true;
    }

    /** 
     * Sets <code>noIndex</code> to <code>true</code>.
     */
    public void setNoIndex() {
      noIndex= true;
    }

    /** 
     * Sets <code>noCache</code> to <code>true</code>.
     */
    public void setNoCache() {
      noCache= true;
    }

    /**
     * Sets the <code>baseHref</code>.
     */
    public void setBaseHref(URL baseHref) {
      this.baseHref= baseHref;
    }

    /** 
     * Returns the current value of <code>noIndex</code>.
     */
    public boolean getNoIndex() {
      return noIndex;
    }

    /** 
     * Returns the current value of <code>noFollow</code>.
     */
    public boolean getNoFollow() {
      return noFollow;
    }

    /** 
     * Returns the current value of <code>noCache</code>.
     */
    public boolean getNoCache() {
      return noCache;
    }

    /**
     * Returns the <code>baseHref</code>, if set, or <code>null</code>
     * otherwise.
     */
    public URL getBaseHref() {
      return baseHref;
    }

  }

  /**
   * Sets the indicators in <code>robotsMeta</code> to appropriate
   * values, based on any META tags found under the given
   * <code>node</code>.
   */
  public static final void getRobotsMetaDirectives(
    RobotsMetaIndicator robotsMeta, Node node, URL currURL) {

    robotsMeta.reset();
    getRobotsMetaDirectivesHelper(robotsMeta, node, currURL);
  }

  private static final void getRobotsMetaDirectivesHelper(
    RobotsMetaIndicator robotsMeta, Node node, URL currURL) {

    if (node.getNodeType() == Node.ELEMENT_NODE) {

      if ("BODY".equals(node.getNodeName())) {
        // META tags should not be under body
        return;
      }

      if ("META".equals(node.getNodeName())) {
        NamedNodeMap attrs= node.getAttributes();
        Node nameNode= attrs.getNamedItem("name");

        if (nameNode != null) {
          if ("robots".equalsIgnoreCase(nameNode.getNodeValue())) {
            Node contentNode= attrs.getNamedItem("content");

            if (contentNode != null) {
              String directives= 
                contentNode.getNodeValue().toLowerCase();
              int index= directives.indexOf("none");

              if (index >= 0) {
                robotsMeta.setNoIndex();
                robotsMeta.setNoFollow();
              }

              index= directives.indexOf("all");
              if (index >= 0) {
                // do nothing...
              }

              index= directives.indexOf("noindex");
              if (index >= 0) {
                robotsMeta.setNoIndex();
              }

              index= directives.indexOf("nofollow");
              if (index >= 0) {
                robotsMeta.setNoFollow();
              }
            } 

          } // end if (name == robots)
        }  // end if (nameNode != null) 
        
        Node HTTPEquivNode= attrs.getNamedItem("http-equiv");

        if ( (HTTPEquivNode != null) 
             && ("Pragma".equalsIgnoreCase(HTTPEquivNode.getNodeValue())) ) {
          Node contentNode= attrs.getNamedItem("content");

          if (contentNode != null) {
            String content= contentNode.getNodeValue().toLowerCase();
            int index= content.indexOf("no-cache");
            if (index >= 0) 
              robotsMeta.setNoCache();
          }

        }

      } else if ("BASE".equalsIgnoreCase(node.getNodeName())) {
        NamedNodeMap attrs= node.getAttributes();
        Node hrefNode= attrs.getNamedItem("href");

        if (hrefNode != null) {
          String urlString= hrefNode.getNodeValue();

          URL url= null;
          try {
            if (currURL == null)
              url= new URL(urlString);
            else 
              url= new URL(currURL, urlString);
          } catch (Exception e) {
            ;
          }

          if (url != null) 
            robotsMeta.setBaseHref(url);
        }

      }

    }

    NodeList children = node.getChildNodes();
    if ( children != null ) {
      int len = children.getLength();
      for ( int i = 0; i < len; i++ ) {
        getRobotsMetaDirectivesHelper(robotsMeta, children.item(i), currURL);
      }
    }
  }

}
