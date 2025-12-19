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
package org.commoncrawl.util;

import java.nio.charset.StandardCharsets;

/**
 * Wrap a byte array as a {@link CharSequence} in
 * {@link StandardCharsets#ISO_8859_1} encoding.
 * 
 * For regular expression matching on ASCII characters only, the wrapper should
 * be faster than creating a {@link String} from the byte array or a
 * subsequence, because no bytes are converted to chars and no memory is
 * allocated for a new String.
 * 
 * Similar wrappers are part of
 * <a href="https://extjwnl.sourceforge.net/">extJWNL</a>,
 * <a href="https://github.com/LAW-Unimi/BUbiNG">BUbiNG</a>, and other Java
 * libraries.
 */
public class ByteArrayCharSequence implements CharSequence {

  private final byte[] data;
  private final int length;
  private final int offset;

  public ByteArrayCharSequence() {
    this(new byte[0], 0, 0);
  }

  public ByteArrayCharSequence(final byte[] data) {
    this(data, 0, data.length);
  }

  public ByteArrayCharSequence(final byte[] data, int length) {
    this(data, 0, length);
  }

  public ByteArrayCharSequence(final byte[] data, int offset, int length) {
    this.data = data;
    if (offset < 0) {
      throw new ArrayIndexOutOfBoundsException("Negative offset: " + offset);
    }
    if (length < 0) {
      throw new IllegalArgumentException("Negative length:" + length);
    }
    if ((offset + length) > data.length) {
      throw new ArrayIndexOutOfBoundsException(
          "(Offset + length) > array_length");
    }
    this.length = length;
    this.offset = offset;
  }

  @Override
  public int length() {
    return this.length;
  }

  @Override
  public char charAt(int index) {
    if (index >= length) {
      throw new IndexOutOfBoundsException("" + index);
    }
    return (char) (data[offset + index] & 0xff);
  }

  @Override
  public CharSequence subSequence(int start, int end) {
    return new ByteArrayCharSequence(data, offset + start, end - start);
  }

  @Override
  public String toString() {
    return new String(data, offset, length, StandardCharsets.ISO_8859_1);
  }
}