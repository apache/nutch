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

/** A simple RPC mechanism.
 *
 * A <i>protocol</i> is a Java interface.  All parameters and return types must
 * be one of:
 *
 * <ul> <li>a primitive type, <code>boolean</code>, <code>byte</code>,
 * <code>char</code>, <code>short</code>, <code>int</code>, <code>long</code>,
 * <code>float</code>, <code>double</code>, or <code>void</code>; or</li>
 *
 * <li>a {@link String}; or</li>
 *
 * <li>a {@link Writable}; or</li>
 *
 * <li>an array of the above types</li> </ul>
 *
 * All methods in the protocol should throw only IOException.  No field data of
 * the protocol instance is transmitted.
 */
public class RPC {
  private static final Logger LOG =
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
    private Class declaredClass;
    public NullInstance() {}
    public NullInstance(Class declaredClass) {
      this.declaredClass = declaredClass;
    }
    public void readFields(DataInput in) throws IOException {
      String className = UTF8.readString(in);
      declaredClass = (Class)PRIMITIVE_NAMES.get(className);
      if (declaredClass == null) {
        try {
          declaredClass = Class.forName(className);
        } catch (ClassNotFoundException e) {
          throw new RuntimeException(e.toString());
        }
      }
    }
    public void write(DataOutput out) throws IOException {
      UTF8.writeString(out, declaredClass.getName());
    }
  }

  private static void writeObject(DataOutput out, Object instance,
                                  Class declaredClass) throws IOException {

    if (instance == null) {                       // null
      instance = new NullInstance(declaredClass);
      declaredClass = NullInstance.class;
    }

    if (instance instanceof Writable) {           // Writable

      // write instance's class, to support subclasses of the declared class
      UTF8.writeString(out, instance.getClass().getName());
      
      ((Writable)instance).write(out);

      return;
    }

    // write declared class for primitives, as they can't be subclassed, and
    // the class of the instance may be a wrapper
    UTF8.writeString(out, declaredClass.getName());

    if (declaredClass.isArray()) {                // array
      int length = Array.getLength(instance);
      out.writeInt(length);
      for (int i = 0; i < length; i++) {
        writeObject(out, Array.get(instance, i),
                    declaredClass.getComponentType());
      }
      
    } else if (declaredClass == String.class) {   // String
      UTF8.writeString(out, (String)instance);
      
    } else if (declaredClass.isPrimitive()) {     // primitive type

      if (declaredClass == Boolean.TYPE) {        // boolean
        out.writeBoolean(((Boolean)instance).booleanValue());
      } else if (declaredClass == Character.TYPE) { // char
        out.writeChar(((Character)instance).charValue());
      } else if (declaredClass == Byte.TYPE) {    // byte
        out.writeByte(((Byte)instance).byteValue());
      } else if (declaredClass == Short.TYPE) {   // short
        out.writeShort(((Short)instance).shortValue());
      } else if (declaredClass == Integer.TYPE) { // int
        out.writeInt(((Integer)instance).intValue());
      } else if (declaredClass == Long.TYPE) {    // long
        out.writeLong(((Long)instance).longValue());
      } else if (declaredClass == Float.TYPE) {   // float
        out.writeFloat(((Float)instance).floatValue());
      } else if (declaredClass == Double.TYPE) {  // double
        out.writeDouble(((Double)instance).doubleValue());
      } else if (declaredClass == Void.TYPE) {    // void
      } else {
        throw new IllegalArgumentException("Not a primitive: "+declaredClass);
      }
      
    } else {
      throw new IOException("Can't write: "+instance+" as "+declaredClass);
    }
  }
  
  
  private static Object readObject(DataInput in)
    throws IOException {
    return readObject(in, null);
  }
    
  private static Object readObject(DataInput in, ObjectWritable objectWritable)
    throws IOException {
    String className = UTF8.readString(in);
    Class declaredClass = (Class)PRIMITIVE_NAMES.get(className);
    if (declaredClass == null) {
      try {
        declaredClass = Class.forName(className);
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e.toString());
      }
    }    

    Object instance;
    
    if (declaredClass == NullInstance.class) {         // null
      NullInstance wrapper = new NullInstance();
      wrapper.readFields(in);
      declaredClass = wrapper.declaredClass;
      instance = null;

    } else if (declaredClass.isPrimitive()) {          // primitive types

      if (declaredClass == Boolean.TYPE) {             // boolean
        instance = Boolean.valueOf(in.readBoolean());
      } else if (declaredClass == Character.TYPE) {    // char
        instance = new Character(in.readChar());
      } else if (declaredClass == Byte.TYPE) {         // byte
        instance = new Byte(in.readByte());
      } else if (declaredClass == Short.TYPE) {        // short
        instance = new Short(in.readShort());
      } else if (declaredClass == Integer.TYPE) {      // int
        instance = new Integer(in.readInt());
      } else if (declaredClass == Long.TYPE) {         // long
        instance = new Long(in.readLong());
      } else if (declaredClass == Float.TYPE) {        // float
        instance = new Float(in.readFloat());
      } else if (declaredClass == Double.TYPE) {       // double
        instance = new Double(in.readDouble());
      } else if (declaredClass == Void.TYPE) {         // void
        instance = null;
      } else {
        throw new IllegalArgumentException("Not a primitive: "+declaredClass);
      }

    } else if (declaredClass.isArray()) {              // array
      int length = in.readInt();
      instance = Array.newInstance(declaredClass.getComponentType(), length);
      for (int i = 0; i < length; i++) {
        Array.set(instance, i, readObject(in));
      }
      
    } else if (declaredClass == String.class) {        // String
      instance = UTF8.readString(in);
      
    } else {                                      // Writable
      try {
        Writable writable = (Writable)declaredClass.newInstance();
        writable.readFields(in);
        instance = writable;
      } catch (InstantiationException e) {
        throw new RuntimeException(e);
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }

    if (objectWritable != null) {                 // store values
      objectWritable.declaredClass = declaredClass;
      objectWritable.instance = instance;
    }

    return instance;
      
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

      ObjectWritable objectWritable = new ObjectWritable();
      for (int i = 0; i < parameters.length; i++) {
        parameters[i] = readObject(in, objectWritable);
        parameterClasses[i] = objectWritable.declaredClass;
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
    private Class declaredClass;
    private Object instance;

    public ObjectWritable() {}

    public ObjectWritable(Class declaredClass, Object instance) {
      this.declaredClass = declaredClass;
      this.instance = instance;
    }

    /** Return the instance. */
    public Object get() { return instance; }

    public void readFields(DataInput in) throws IOException {
      readObject(in, this);
    }

    public void write(DataOutput out) throws IOException {
      writeObject(out, instance, declaredClass);
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

  /** Expert: Make multiple, parallel calls to a set of servers. */
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
      if (wrappedValues[i] != null)
        values[i] = ((ObjectWritable)wrappedValues[i]).get();
    
    return values;
  }
  

  /** Construct a server for a protocol implementation instance listening on a
   * port. */
  public static Server getServer(final Object instance, final int port) {
    return getServer(instance, port, 1, false);
  }

  /** Construct a server for a protocol implementation instance listening on a
   * port. */
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
