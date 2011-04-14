/*******************************************************************************
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
 ******************************************************************************/
package org.apache.nutch.tools.proxy;
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

import java.io.IOException;
import java.util.Random;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletResponse;

import org.mortbay.jetty.Request;

public class DelayHandler extends AbstractTestbedHandler {
  
  public static final long DEFAULT_DELAY = 2000;
  
  private int delay;
  private boolean random;
  private Random r;
  
  public DelayHandler(int delay) {
    if (delay < 0) {
      delay = -delay;
      random = true;
      r = new Random(1234567890L); // repeatable random
    }
    this.delay = delay;
  }

  @Override
  public void handle(Request req, HttpServletResponse res, String target,
          int dispatch) throws IOException, ServletException {
    try {
      int del = random ? r.nextInt(delay) : delay;
      Thread.sleep(del);
      addMyHeader(res, "Delay", String.valueOf(del));
    } catch (Exception e) {
      
    }
  }
}
