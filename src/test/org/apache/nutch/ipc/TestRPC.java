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

import org.apache.nutch.io.Writable;
import org.apache.nutch.io.IntWritable;
import org.apache.nutch.io.NullWritable;

import java.io.IOException;
import java.net.InetSocketAddress;

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
    IntWritable add(IntWritable v1, IntWritable v2) throws IOException;
    IntWritable add(IntWritable[] values) throws IOException;
    IntWritable error() throws IOException;
  }

  public class TestImpl implements TestProtocol {

    public void ping() {}

    public String echo(String value) throws IOException { return value; }

    public String[] echo(String[] values) throws IOException { return values; }

    public IntWritable add(IntWritable v1, IntWritable v2) {
      return new IntWritable(v1.get() + v2.get());
    }

    public IntWritable add(IntWritable[] values) {
      int sum = 0;
      for (int i = 0; i < values.length; i++) {
        sum += values[i].get();
      }
      return new IntWritable(sum);
    }

    public IntWritable error() throws IOException {
      throw new IOException("bobo");
    }

  }

  public void testCalls() throws Exception {
    Server server = RPC.getServer(new TestImpl(), PORT);
    server.start();

    TestProtocol proxy =
      (TestProtocol)RPC.getProxy(TestProtocol.class,
                                 new InetSocketAddress(PORT));
    
    proxy.ping();

    String stringResult = proxy.echo("foo");
    assertEquals(stringResult, "foo");

    String[] stringResults = proxy.echo(new String[]{"foo","bar"});
    assertTrue(Arrays.equals(stringResults, new String[]{"foo","bar"}));

    IntWritable intResult = proxy.add(new IntWritable(1), new IntWritable(2));
    assertEquals(intResult, new IntWritable(3));

    intResult = proxy.add(new IntWritable[]
      { new IntWritable(1), new IntWritable(2) });
    assertEquals(intResult, new IntWritable(3));

    boolean caught = false;
    try {
      proxy.error();
    } catch (IOException e) {
      LOG.fine("Caught " + e);
      caught = true;
    }
    assertTrue(caught);

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
