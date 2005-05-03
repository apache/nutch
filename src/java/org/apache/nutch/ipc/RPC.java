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
import java.util.logging.Logger;
import java.io.*;
import java.util.*;

import org.apache.nutch.io.*;
import org.apache.nutch.util.*;

/** A simple RPC mechanism.  A protocol is a Java interface.  All methods
 * should throw only IOException.  No field data is transmitted. */
public class RPC {
  public static final Logger LOG =
    LogFormatter.getLogger("org.apache.nutch.ipc.RPC");

  private RPC() {}                                  // no public ctor

  private static final Map PRIMITIVE_NAMES = new HashMap();
  static {
    PRIMITIVE_NAMES.put("boolean", Boolean.TYPE);
    PRIMITIVE_NAMES.put("byte", Byte.TYPE);
    PRIMITIVE_NAMES.put("char", Character.TYPE);
    PRIMITIVE_NAMES.put("short", Short.TYPE);
    PRIMITIVE_NAMES.put("int", Integer.TYPE);
    PRIMITIVE_NAMES.put("long", Long.TYPE);
    PRIMITIVE_NAMES.put("float", Float.TYPE);
    PRIMITIVE_NAMES.put("double", Double.TYPE);
    PRIMITIVE_NAMES.put("void", Void.TYPE);
  }

  private static class NullInstance implements Writable {
    private Class theClass;
    public NullInstance() {}
    public NullInstance(Class theClass) { this.theClass = theClass; }
    public void readFields(DataInput in) throws IOException {
      try {
        theClass = Class.forName(UTF8.readString(in));
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e.toString());
      }
    }
    public void write(DataOutput out) throws IOException {
      UTF8.writeString(out, theClass.getName());
    }
  }

  private static void writeObject(DataOutput out, Object instance,
                                  Class theClass) throws IOException {

    if (instance == null) {                       // null
      instance = new NullInstance(theClass);
      theClass = NullInstance.class;
    }

    UTF8.writeString(out, theClass.getName());

    if (theClass.isArray()) {                     // array
      int length = Array.getLength(instance);
      out.writeInt(length);
      for (int i = 0; i < length; i++) {
        writeObject(out, Array.get(instance, i), theClass.getComponentType());
      }
      
    } else if (theClass == String.class) {        // String
      UTF8.writeString(out, (String)instance);
      
    } else if (theClass.isPrimitive()) {          // primitive type

      if (theClass == Boolean.TYPE) {            // boolean
        out.writeBoolean(((Boolean)instance).booleanValue());
      } else if (theClass == Character.TYPE) {    // char
        out.writeChar(((Character)instance).charValue());
      } else if (theClass == Byte.TYPE) {         // byte
        out.writeByte(((Byte)instance).byteValue());
      } else if (theClass == Short.TYPE) {        // short
        out.writeShort(((Short)instance).shortValue());
      } else if (theClass == Integer.TYPE) {      // int
        out.writeInt(((Integer)instance).intValue());
      } else if (theClass == Long.TYPE) {         // long
        out.writeLong(((Long)instance).longValue());
      } else if (theClass == Float.TYPE) {        // float
        out.writeFloat(((Float)instance).floatValue());
      } else if (theClass == Double.TYPE) {       // double
        out.writeDouble(((Double)instance).doubleValue());
      } else if (theClass == Void.TYPE) {         // void
      } else {
        throw new IllegalArgumentException("Not a known primitive: "+theClass);
      }
      
    } else if (instance instanceof Writable) {        // Writable
      ((Writable)instance).write(out);
    } else {
      throw new IOException("Can't write: " + instance + " as " + theClass);
    }
  }
  
  
  private static Object readObject(DataInput in)
    throws IOException {
    return readObject(in, null);
  }
    
  private static Object readObject(DataInput in, Class[] storeClass)
    throws IOException {
    
    String name = UTF8.readString(in);
    Class theClass = (Class)PRIMITIVE_NAMES.get(name);
    if (theClass == null) {
      try {
        theClass = Class.forName(name);
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e.toString());
      }
    }    

    if (storeClass != null)
      storeClass[0] = theClass;

    if (theClass == NullInstance.class) {         // null
      NullInstance instance = new NullInstance();
      instance.readFields(in);
      storeClass[0] = instance.theClass;
      return null;

    } else if (theClass.isPrimitive()) {          // primitive types

      if (theClass == Boolean.TYPE) {             // boolean
        return Boolean.valueOf(in.readBoolean());
      } else if (theClass == Character.TYPE) {    // char
        return new Character(in.readChar());
      } else if (theClass == Byte.TYPE) {         // byte
        return new Byte(in.readByte());
      } else if (theClass == Short.TYPE) {        // short
        return new Short(in.readShort());
      } else if (theClass == Integer.TYPE) {      // int
        return new Integer(in.readInt());
      } else if (theClass == Long.TYPE) {         // long
        return new Long(in.readLong());
      } else if (theClass == Float.TYPE) {        // float
        return new Float(in.readFloat());
      } else if (theClass == Double.TYPE) {       // double
        return new Double(in.readDouble());
      } else if (theClass == Void.TYPE) {         // void
        return null;
      } else {
        throw new IllegalArgumentException("Not a known primitive: "+theClass);
      }

    } else if (theClass.isArray()) {              // array
      int length = in.readInt();
      Object array = Array.newInstance(theClass.getComponentType(), length);
      for (int i = 0; i < length; i++) {
        Array.set(array, i, readObject(in));
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

      Class[] storeClass = new Class[1];
      for (int i = 0; i < parameters.length; i++) {
        parameters[i] = readObject(in, storeClass);
        parameterClasses[i] = storeClass[0];
      }
    }

    public void write(DataOutput out) throws IOException {
      UTF8.writeString(out, methodName);
      out.writeInt(parameterClasses.length);
      for (int i = 0; i < parameterClasses.length; i++) {
        writeObject(out, parameters[i], parameterClasses[i]);
      }
    }

    public String toString() {
      StringBuffer buffer = new StringBuffer();
      buffer.append(methodName);
      buffer.append("(");
      for (int i = 0; i < parameters.length; i++) {
        if (i != 0)
          buffer.append(", ");
        buffer.append(parameters[i]);
      }
      buffer.append(")");
      return buffer.toString();
    }

  }

  /** A polymorphic Writable that packages a Writable with its class name.
   * Also handles arrays and strings w/o a Writable wrapper.
   */
  private static class ObjectWritable implements Writable {
    private Class theClass;
    private Object instance;

    public ObjectWritable() {}

    public ObjectWritable(Class theClass, Object instance) {
      this.theClass = theClass;
      this.instance = instance;
    }

    /** Return the instance. */
    public Object get() { return instance; }

    public void readFields(DataInput in) throws IOException {
      Class[] storeClass = new Class[1];
      instance = readObject(in, storeClass);
      theClass = storeClass[0];
    }

    public void write(DataOutput out) throws IOException {
      writeObject(out, instance, theClass);
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

  /** Make multiple, parallel calls to a set of servers. */
  public static Object[] call(Method method, Object[][] params,
                              InetSocketAddress[] addrs)
    throws IOException {

    Invocation[] invocations = new Invocation[params.length];
    for (int i = 0; i < params.length; i++)
      invocations[i] = new Invocation(method, params[i]);
    
    Writable[] wrappedValues = CLIENT.call(invocations, addrs);
    
    Object[] values =
      (Object[])Array.newInstance(method.getReturnType(),wrappedValues.length);
    for (int i = 0; i < values.length; i++)
      values[i] = ((ObjectWritable)wrappedValues[i]).get();
    
    return values;
  }
  

  /** Construct a server for the named instance listening on the named port. */
  public static Server getServer(final Object instance, final int port) {
    return getServer(instance, port, 1, false);
  }

  /** Construct a server for the named instance listening on the named port. */
  public static Server getServer(final Object instance, final int port,
                                 final int numHandlers,
                                 final boolean verbose) {
    return new Server(port, Invocation.class, numHandlers) {
        
        Class implementation = instance.getClass();

        public Writable call(Writable param) throws IOException {
          try {
            Invocation call = (Invocation)param;
            if (verbose) LOG.info("Call: " + call);

            Method method =
              implementation.getMethod(call.getMethodName(),
                                       call.getParameterClasses());

            Object value = method.invoke(instance, call.getParameters());
            if (verbose) LOG.info("Return: " + value);

            return new ObjectWritable(method.getReturnType(), value);

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
