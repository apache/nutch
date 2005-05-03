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

package org.apache.nutch.ipc;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.lang.reflect.Method;

import junit.framework.TestCase;

import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.Arrays;

import org.apache.nutch.util.LogFormatter;

/** Unit tests for RPC. */
public class TestRPC extends TestCase {
  private static final int PORT = 1234;

  public static final Logger LOG =
    LogFormatter.getLogger("org.apache.nutch.ipc.TestRPC");

  // quiet during testing, since output ends up on console
  static {
    LOG.setLevel(Level.WARNING);
    Client.LOG.setLevel(Level.WARNING);
    Server.LOG.setLevel(Level.WARNING);
  }

  public TestRPC(String name) { super(name); }
	
  public interface TestProtocol {
    void ping() throws IOException;
    String echo(String value) throws IOException;
    String[] echo(String[] value) throws IOException;
    int add(int v1, int v2) throws IOException;
    int add(int[] values) throws IOException;
    int error() throws IOException;
  }

  public class TestImpl implements TestProtocol {

    public void ping() {}

    public String echo(String value) throws IOException { return value; }

    public String[] echo(String[] values) throws IOException { return values; }

    public int add(int v1, int v2) {
      return v1 + v2;
    }

    public int add(int[] values) {
      int sum = 0;
      for (int i = 0; i < values.length; i++) {
        sum += values[i];
      }
      return sum;
    }

    public int error() throws IOException {
      throw new IOException("bobo");
    }

  }

  public void testCalls() throws Exception {
    Server server = RPC.getServer(new TestImpl(), PORT);
    server.start();

    InetSocketAddress addr = new InetSocketAddress(PORT);
    TestProtocol proxy =
      (TestProtocol)RPC.getProxy(TestProtocol.class, addr);
    
    proxy.ping();

    String stringResult = proxy.echo("foo");
    assertEquals(stringResult, "foo");

    String[] stringResults = proxy.echo(new String[]{"foo","bar"});
    assertTrue(Arrays.equals(stringResults, new String[]{"foo","bar"}));

    int intResult = proxy.add(1, 2);
    assertEquals(intResult, 3);

    intResult = proxy.add(new int[] {1, 2});
    assertEquals(intResult, 3);

    boolean caught = false;
    try {
      proxy.error();
    } catch (IOException e) {
      LOG.fine("Caught " + e);
      caught = true;
    }
    assertTrue(caught);

    // try a multi-call
    Method method =
      TestProtocol.class.getMethod("echo", new Class[] { String.class });
    String[] values = (String[])RPC.call(method, new String[][]{{"a"},{"b"}},
                                         new InetSocketAddress[] {addr, addr});
    assertTrue(Arrays.equals(values, new String[]{"a","b"}));


    server.stop();
  }
  public static void main(String[] args) throws Exception {
    // crank up the volume!
    LOG.setLevel(Level.FINE);
    Client.LOG.setLevel(Level.FINE);
    Server.LOG.setLevel(Level.FINE);
    LogFormatter.setShowThreadIDs(true);

    new TestRPC("test").testCalls();

  }

}
