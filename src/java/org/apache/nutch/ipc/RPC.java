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

import java.lang.reflect.Proxy;
import java.lang.reflect.Method;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;

import java.net.InetSocketAddress;
import java.io.*;

import org.apache.nutch.io.*;

/** A simple RPC mechanism.  A protocol is a Java interface.  All method
 * parameters and return values must implement Writable.  All methods should
 * throw only IOException.  No field data is transmitted. */
public class RPC {

  private RPC() {}                                  // no public ctor


  public static void writeObject(DataOutput out, Object instance)
    throws IOException {

    if (instance == null) {                       // null
      instance = NullWritable.get();
    }

    Class theClass = instance.getClass();
    UTF8.writeString(out, theClass.getName());
    if (theClass.isArray()) {                     // array
      int length = Array.getLength(instance);
      out.writeInt(length);
      for (int i = 0; i < length; i++) {
        writeObject(out, Array.get(instance, i));
      }
      
    } else if (theClass == String.class) {        // String
      UTF8.writeString(out, (String)instance);
      
    } else {                                      // Writable
      ((Writable)instance).write(out);
    }
  }
  
  
  public static Object readObject(DataInput in)
    throws IOException {
    
    Class theClass;
    try {
      theClass = Class.forName(UTF8.readString(in));
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e.toString());
    }
    
    if (theClass == NullWritable.class) {         // null
      return null;
      
    } else if (theClass.isArray()) {              // array
      int length = in.readInt();
      Object array = Array.newInstance(theClass.getComponentType(), length);
      for (int i = 0; i < length; i++) {
        ObjectWritable element = new ObjectWritable();
        element.readFields(in);
        Array.set(array, i, element.get());
      }
      return array;
      
    } else if (theClass == String.class) {        // String
      return UTF8.readString(in);
      
    } else {                                      // Writable
      try {
        Writable instance = (Writable)theClass.newInstance();
        instance.readFields(in);
        return instance;
      } catch (InstantiationException e) {
        throw new RuntimeException(e);
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /** A method invocation, including the method name and its parameters.*/
  private static class Invocation implements Writable {
    private String methodName;
    private Class[] parameterClasses;
    private Object[] parameters;

    public Invocation() {}

    public Invocation(Method method, Object[] parameters) {
      this.methodName = method.getName();
      this.parameterClasses = method.getParameterTypes();
      this.parameters = parameters;
    }

    /** The name of the method invoked. */
    public String getMethodName() { return methodName; }

    /** The parameter classes. */
    public Class[] getParameterClasses() { return parameterClasses; }

    /** The parameter instances. */
    public Object[] getParameters() { return parameters; }

    public void readFields(DataInput in) throws IOException {
      methodName = UTF8.readString(in);
      parameters = new Object[in.readInt()];
      parameterClasses = new Class[parameters.length];
      for (int i = 0; i < parameters.length; i++) {
        parameters[i] = readObject(in);
        parameterClasses[i] = parameters[i].getClass();
      }
    }

    public void write(DataOutput out) throws IOException {
      UTF8.writeString(out, methodName);
      out.writeInt(parameterClasses.length);
      for (int i = 0; i < parameterClasses.length; i++) {
        writeObject(out, parameters[i]);
      }
    }

  }

  /** A polymorphic Writable that packages a Writable with its class name.
   * Also handles arrays and strings w/o a Writable wrapper.
   */
  private static class ObjectWritable implements Writable {
    private Object instance;

    public ObjectWritable() {}

    public ObjectWritable(Object instance) {
      this.instance = instance;
    }

    /** Return the instance. */
    public Object get() { return instance; }

    public void readFields(DataInput in) throws IOException {
      instance = readObject(in);
    }

    public void write(DataOutput out) throws IOException {
      writeObject(out, instance);
    }

  }

  private static Client CLIENT = new Client(ObjectWritable.class);

  private static class Invoker implements InvocationHandler {
    private InetSocketAddress address;

    public Invoker(InetSocketAddress address) {
      this.address = address;
    }

    public Object invoke(Object proxy, Method method, Object[] args)
      throws Throwable {
      ObjectWritable value = (ObjectWritable)
        CLIENT.call(new Invocation(method, args), address);
      return value.get();
    }
  }

  /** Construct a client-side proxy object that implements the named protocol,
   * talking to a server at the named address. */
  public static Object getProxy(Class protocol, InetSocketAddress addr) {
    return Proxy.newProxyInstance(protocol.getClassLoader(),
                                  new Class[] { protocol },
                                  new Invoker(addr));
  }
  

  /** Construct a server for the named instance listening on the named port. */
  public static Server getServer(final Object instance, final int port) {
    return getServer(instance, port, 1);
  }

  /** Construct a server for the named instance listening on the named port. */
  public static Server getServer(final Object instance, final int port,
                                 final int numHandlers) {
    return new Server(port, Invocation.class, numHandlers) {
        
        Class implementation = instance.getClass();

        public Writable call(Writable param) throws IOException {
          try {
            Invocation call = (Invocation)param;
            Method method =
              implementation.getMethod(call.getMethodName(),
                                       call.getParameterClasses());

            Object value = method.invoke(instance, call.getParameters());

            return new ObjectWritable(value);

          } catch (InvocationTargetException e) {
            Throwable target = e.getTargetException();
            if (target instanceof IOException) {
              throw (IOException)target;
            } else {
              IOException ioe = new IOException(target.toString());
              ioe.setStackTrace(target.getStackTrace());
              throw ioe;
            }
          } catch (Throwable e) {
            IOException ioe = new IOException(e.toString());
            ioe.setStackTrace(e.getStackTrace());
            throw ioe;
          }
        }
      };
  }
  
}
