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

package org.apache.nutch.io;

import java.io.*;

import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public final class WritableUtils  {

  public static byte[] readCompressedByteArray(DataInput in) throws IOException {
    int length = in.readInt();
    byte[] buffer = new byte[length];
    in.readFully(buffer);      // could/should use readFully(buffer,0,length)?
    GZIPInputStream gzi = new GZIPInputStream(new ByteArrayInputStream(buffer, 0, buffer.length));
    byte[] outbuf = new byte[length];
    ByteArrayOutputStream bos =  new ByteArrayOutputStream();
     int len;
     while((len=gzi.read(outbuf,0,outbuf.length)) != -1){
       bos.write(outbuf,0,len);
     }
     byte[] decompressed =  bos.toByteArray();
     bos.close();
     gzi.close();
     return decompressed;
  }

  public static int  writeCompressedByteArray(DataOutput out, byte[] bytes) throws IOException {
    ByteArrayOutputStream bos =  new ByteArrayOutputStream();
    GZIPOutputStream gzout = new GZIPOutputStream(bos);
    gzout.write(bytes,0,bytes.length);
    gzout.close();
    byte[] buffer = bos.toByteArray();
    int len = buffer.length;
    out.writeInt(len);
    out.write(buffer,0,len);
    /* debug only! Once we have confidence, can lose this. */
    return ((bytes.length != 0) ? (100*buffer.length)/bytes.length : 0);
  }


  /* Ugly utility, maybe someone else can do this better  */
  public static String readCompressedString(DataInput in) throws IOException {
    return new String(readCompressedByteArray(in),"UTF-8");
  }


  public static int  writeCompressedString(DataOutput out, String s) throws IOException {
    return writeCompressedByteArray(out, s.getBytes("UTF-8"));
  }

  /*
   *
   * Write a String as a Network Int n, followed by n Bytes
   * Alternative to 16 bit read/writeUTF.
   * Encoding standard is... ?
   * 
   */
  public static void writeString(DataOutput out, String s) throws IOException {
    byte[] buffer = s.getBytes("UTF-8");
    int len = buffer.length;
    out.writeInt(len);
    out.write(buffer,0,len);
  }

  /*
   * Read a String as a Network Int n, followed by n Bytes
   * Alternative to 16 bit read/writeUTF.
   * Encoding standard is... ?
   *
   */
  public static String readString(DataInput in) throws IOException{
    int length = in.readInt();
    byte[] buffer = new byte[length];
    in.readFully(buffer);      // could/should use readFully(buffer,0,length)?
    return new String(buffer,"UTF-8");  
  }


  /*
   * Write a String array as a Nework Int N, followed by Int N Byte Array Strings.
   * Could be generalised using introspection.
   *
   */
  public static void writeStringArray(DataOutput out, String[] s) throws IOException{
    out.writeInt(s.length);
    for(int i=0;i < s.length;i++){
      writeString(out,s[i]);
    }
  }

  /*
   * Write a String array as a Nework Int N, followed by Int N Byte Array Strings.
   * Could be generalised using introspection. Actually this bit couldn't...
   *
   */
  public static  String[] readStringArray(DataInput in) throws IOException {
    int len = in.readInt();
    String[] s = new String[len];
    for(int i=0;i < len;i++){
      s[i] = readString(in);
    }
    return s;
  }


  /*
   *
   * Test Utility Method Display Byte Array. 
   *
   */
  public static void displayByteArray(byte[] record){
    int i;
    for(i=0;i < record.length -1 ; i++){
      if (i % 16 == 0) { System.out.println(); }
      System.out.print(Integer.toHexString(record[i]  >> 4 & 0x0F));
      System.out.print(Integer.toHexString(record[i] & 0x0F));
      System.out.print(",");
    }
    System.out.print(Integer.toHexString(record[i]  >> 4 & 0x0F));
    System.out.print(Integer.toHexString(record[i] & 0x0F));
    System.out.println();
  }


}
