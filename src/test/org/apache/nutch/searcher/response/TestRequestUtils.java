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

package org.apache.nutch.searcher.response;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import junit.framework.TestCase;

public class TestRequestUtils extends TestCase {

  public TestRequestUtils(String name) {
    super(name);
  }

  /**
   * Test getBooleanParameter() - no default
   */
  public void testGetBooleanParameterNoDefault() {
    String param = "foo";
    Map parameters = new HashMap();
    HttpServletRequest request = createMockHttpServletRequest(parameters);

    assertFalse("No param", RequestUtils.getBooleanParameter(request, param));

    parameters.put(param, "0");
    assertFalse("Foo=0",    RequestUtils.getBooleanParameter(request, param));

    parameters.put(param, "no");
    assertFalse("Foo=no",  RequestUtils.getBooleanParameter(request, param));

    parameters.put(param, "false");
    assertFalse("Foo=false", RequestUtils.getBooleanParameter(request, param));

    parameters.put(param, "abcdef");
    assertFalse("Foo=abcdef", RequestUtils.getBooleanParameter(request, param));

    parameters.put(param, "1");
    assertTrue("Foo=1",    RequestUtils.getBooleanParameter(request, param));

    parameters.put(param, "yes");
    assertTrue("Foo=yes",  RequestUtils.getBooleanParameter(request, param));

    parameters.put(param, "YES");
    assertTrue("Foo=YES",  RequestUtils.getBooleanParameter(request, param));

    parameters.put(param, "true");
    assertTrue("Foo=true", RequestUtils.getBooleanParameter(request, param));

    parameters.put(param, "TRUE");
    assertTrue("Foo=TRUE", RequestUtils.getBooleanParameter(request, param));
  }

  /**
   * Test getBooleanParameter() - with default
   */
  public void testGetBooleanParameterWithoDefault() {
    String param = "foo";
    Map parameters = new HashMap();
    HttpServletRequest request = createMockHttpServletRequest(parameters);

    assertTrue("No param - def true", RequestUtils.getBooleanParameter(request, param, true));
    assertFalse("No param - def false", RequestUtils.getBooleanParameter(request, param, false));

    parameters.put(param, "0");
    assertFalse("Foo=0",    RequestUtils.getBooleanParameter(request, param, true));

    parameters.put(param, "no");
    assertFalse("Foo=no",  RequestUtils.getBooleanParameter(request, param, true));

    parameters.put(param, "false");
    assertFalse("Foo=false", RequestUtils.getBooleanParameter(request, param, true));

    parameters.put(param, "abcdef");
    assertFalse("Foo=abcdef", RequestUtils.getBooleanParameter(request, param, true));

    parameters.put(param, "1");
    assertTrue("Foo=1",    RequestUtils.getBooleanParameter(request, param, false));

    parameters.put(param, "yes");
    assertTrue("Foo=yes",  RequestUtils.getBooleanParameter(request, param, false));

    parameters.put(param, "YES");
    assertTrue("Foo=YES",  RequestUtils.getBooleanParameter(request, param, false));

    parameters.put(param, "true");
    assertTrue("Foo=true", RequestUtils.getBooleanParameter(request, param, false));

    parameters.put(param, "TRUE");
    assertTrue("Foo=TRUE", RequestUtils.getBooleanParameter(request, param, false));
  }

  /**
   * Create a mock HttpServletRequest.
   */
  private HttpServletRequest createMockHttpServletRequest(Map parameters) {
    MockHttpServletRequestInvocationHandler handler = new MockHttpServletRequestInvocationHandler();
    handler.setParameterMap(parameters);
    ClassLoader cl = getClass().getClassLoader();
    Class[] interfaces = new Class[] {HttpServletRequest.class};
    return (HttpServletRequest)Proxy.newProxyInstance(cl, interfaces, handler);
  }

  /**
   * InvocationHandler for mock HttpServletRequest proxy.
   */
  private static class MockHttpServletRequestInvocationHandler implements InvocationHandler {
    private Map parameters = new HashMap();
    public Object invoke(Object proxy, Method method, Object[] args)
            throws Throwable {
      if (method.getName().equals("getParameter")) {
        return parameters.get((String)args[0]);
      }
      return null;
    }
    public void setParameterMap(Map parameters) {
      this.parameters = parameters;
    }
  }
}