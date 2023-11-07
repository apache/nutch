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
package org.apache.nutch.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.regex.Pattern;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Unit tests for StringUtil methods. */
@Tag("org.apache.nutch.util")
@Tag("core")
public class TestStringUtil {

  public void testRightPad() {
    String s = "my string";

    String ps = StringUtil.rightPad(s, 0);
    assertTrue(s.equals(ps));

    ps = StringUtil.rightPad(s, 9);
    assertTrue(s.equals(ps));

    ps = StringUtil.rightPad(s, 10);
    assertTrue((s + " ").equals(ps));

    ps = StringUtil.rightPad(s, 15);
    assertTrue((s + "      ").equals(ps));
  }

  @Test
  public void testLeftPad() {
    String s = "my string";

    String ps = StringUtil.leftPad(s, 0);
    assertTrue(s.equals(ps));

    ps = StringUtil.leftPad(s, 9);
    assertTrue(s.equals(ps));

    ps = StringUtil.leftPad(s, 10);
    assertTrue((" " + s).equals(ps));

    ps = StringUtil.leftPad(s, 15);
    assertTrue(("      " + s).equals(ps));
  }

  @Test
  public void testMaskPasswords() {
    String secret = "password";
    String masked = StringUtil.mask(secret);
    assertNotEquals(secret, masked);
    assertEquals(secret.length(), masked.length());

    char mask = 'X';
    masked = StringUtil.mask(secret, mask);
    assertNotEquals(secret, masked);
    assertEquals(secret.length(), masked.length());
    masked.chars().forEach((c) -> assertEquals(mask, c));

    String strWithSecret = "amqp://username:password@example.org:5672/virtualHost";
    Pattern maskPasswordPattern = Pattern.compile("^amqp://[^:]+:([^@]+)@");
    masked = StringUtil.mask(strWithSecret, maskPasswordPattern, mask);
    assertNotEquals(strWithSecret, masked);
    assertEquals(strWithSecret.length(), masked.length());
    assertFalse(masked.contains(secret));
    assertTrue(masked.contains(StringUtil.mask(secret, mask)));
  }

}
