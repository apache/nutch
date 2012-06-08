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

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.mortbay.jetty.HttpConnection;
import org.mortbay.jetty.Request;
import org.mortbay.jetty.handler.AbstractHandler;

public abstract class AbstractTestbedHandler extends AbstractHandler {
  protected boolean debug = false;

  @Override
  public void handle(String target, HttpServletRequest req,
          HttpServletResponse res, int dispatch) throws IOException,
          ServletException {
    Request base_request = (req instanceof Request) ? (Request)req : HttpConnection.getCurrentConnection().getRequest();
    res.addHeader("X-TestbedHandlers", this.getClass().getSimpleName());
    handle(base_request, res, target, dispatch);
  }
  
  public abstract void handle(Request req, HttpServletResponse res, String target,
          int dispatch) throws IOException, ServletException;
  
  public void addMyHeader(HttpServletResponse res, String name, String value) {
    name = "X-" + this.getClass().getSimpleName() + "-" + name;
    res.addHeader(name, value);
  }
}
