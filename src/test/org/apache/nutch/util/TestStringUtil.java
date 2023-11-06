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

import java.util.regex.Pattern;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Unit tests for StringUtil methods. */
@Tag("util")
public class TestStringUtil {

  public void testRightPad() {
    String s = "my string";

    String ps = StringUtil.rightPad(s, 0);
    Assertions.assertTrue(s.equals(ps));

    ps = StringUtil.rightPad(s, 9);
    Assertions.assertTrue(s.equals(ps));

    ps = StringUtil.rightPad(s, 10);
    Assertions.assertTrue((s + " ").equals(ps));

    ps = StringUtil.rightPad(s, 15);
    Assertions.assertTrue((s + "      ").equals(ps));
  }

  @Test
  public void testLeftPad() {
    String s = "my string";

    String ps = StringUtil.leftPad(s, 0);
    Assertions.assertTrue(s.equals(ps));

    ps = StringUtil.leftPad(s, 9);
    Assertions.assertTrue(s.equals(ps));

    ps = StringUtil.leftPad(s, 10);
    Assertions.assertTrue((" " + s).equals(ps));

    ps = StringUtil.leftPad(s, 15);
    Assertions.assertTrue(("      " + s).equals(ps));
  }

  @Test
  public void testMaskPasswords() {
    String secret = "password";
    String masked = StringUtil.mask(secret);
    Assertions.assertNotEquals(secret, masked);
    Assertions.assertEquals(secret.length(), masked.length());

    char mask = 'X';
    masked = StringUtil.mask(secret, mask);
    Assertions.assertNotEquals(secret, masked);
    Assertions.assertEquals(secret.length(), masked.length());
    masked.chars().forEach((c) -> Assertions.assertEquals(mask, c));

    String strWithSecret = "amqp://username:password@example.org:5672/virtualHost";
    Pattern maskPasswordPattern = Pattern.compile("^amqp://[^:]+:([^@]+)@");
    masked = StringUtil.mask(strWithSecret, maskPasswordPattern, mask);
    Assertions.assertNotEquals(strWithSecret, masked);
    Assertions.assertEquals(strWithSecret.length(), masked.length());
    Assertions.assertFalse(masked.contains(secret));
    Assertions.assertTrue(masked.contains(StringUtil.mask(secret, mask)));
  }

}
