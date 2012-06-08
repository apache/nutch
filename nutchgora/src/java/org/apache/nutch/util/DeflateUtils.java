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

package org.apache.nutch.util;

import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;
import java.util.zip.DeflaterOutputStream;

// Commons Logging imports
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  A collection of utility methods for working on deflated data.
 */
public class DeflateUtils {
  
  private static final Logger LOG = LoggerFactory.getLogger(DeflateUtils.class);
  private static final int EXPECTED_COMPRESSION_RATIO = 5;
  private static final int BUF_SIZE = 4096;

  /**
   * Returns an inflated copy of the input array.  If the deflated 
   * input has been truncated or corrupted, a best-effort attempt is
   * made to inflate as much as possible.  If no data can be extracted
   * <code>null</code> is returned.
   */
  public static final byte[] inflateBestEffort(byte[] in) {
    return inflateBestEffort(in, Integer.MAX_VALUE);
  }

  /**
   * Returns an inflated copy of the input array, truncated to
   * <code>sizeLimit</code> bytes, if necessary.  If the deflated input
   * has been truncated or corrupted, a best-effort attempt is made to
   * inflate as much as possible.  If no data can be extracted
   * <code>null</code> is returned.
   */
  public static final byte[] inflateBestEffort(byte[] in, int sizeLimit) {
    // decompress using InflaterInputStream 
    ByteArrayOutputStream outStream = 
      new ByteArrayOutputStream(EXPECTED_COMPRESSION_RATIO * in.length);

    // "true" because HTTP does not provide zlib headers
    Inflater inflater = new Inflater(true);
    InflaterInputStream inStream = 
      new InflaterInputStream(new ByteArrayInputStream(in), inflater);

    byte[] buf = new byte[BUF_SIZE];
    int written = 0;
    while (true) {
      try {
	int size = inStream.read(buf);
	if (size <= 0) 
	  break;
	if ((written + size) > sizeLimit) {
	  outStream.write(buf, 0, sizeLimit - written);
	  break;
	}
	outStream.write(buf, 0, size);
	written+= size;
      } catch (Exception e) {
	LOG.info( "Caught Exception in inflateBestEffort", e );
	break;
      }
    }
    try {
      outStream.close();
    } catch (IOException e) {
    }

    return outStream.toByteArray();
  }


  /**
   * Returns an inflated copy of the input array.  
   * @throws IOException if the input cannot be properly decompressed
   */
  public static final byte[] inflate(byte[] in) throws IOException {
    // decompress using InflaterInputStream 
    ByteArrayOutputStream outStream = 
      new ByteArrayOutputStream(EXPECTED_COMPRESSION_RATIO * in.length);

    InflaterInputStream inStream = 
      new InflaterInputStream ( new ByteArrayInputStream(in) );

    byte[] buf = new byte[BUF_SIZE];
    while (true) {
      int size = inStream.read(buf);
      if (size <= 0) 
        break;
      outStream.write(buf, 0, size);
    }
    outStream.close();

    return outStream.toByteArray();
  }

  /**
   * Returns a deflated copy of the input array.
   */
  public static final byte[] deflate(byte[] in) {
    // compress using DeflaterOutputStream 
    ByteArrayOutputStream byteOut = 
      new ByteArrayOutputStream(in.length / EXPECTED_COMPRESSION_RATIO);

    DeflaterOutputStream outStream = new DeflaterOutputStream(byteOut);

    try {
      outStream.write(in);
    } catch (Exception e) {
      LOG.error("Failed to implement outStream.write (input)", e);
    }

    try {
      outStream.close();
    } catch (IOException e) {
      LOG.error("Failed to implement outStream.close", e);
    }

    return byteOut.toByteArray();
  }
}
