/*
 * Created on Aug 4, 2006
 * Author: Andrzej Bialecki &lt;ab@getopt.org&gt;
 *
 */
package org.apache.nutch.protocol;

import java.net.URL;

public class EmptyRobotRules implements RobotRules {
  
  public static final RobotRules RULES = new EmptyRobotRules();

  public long getCrawlDelay() {
    return -1;
  }

  public long getExpireTime() {
    return -1;
  }

  public boolean isAllowed(URL url) {
    return true;
  }

}
