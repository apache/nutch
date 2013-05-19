/**
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

import org.junit.Test;
import static org.junit.Assert.*;

/** Unit tests for StringUtil methods. */
public class TestStringUtil {

  @Test
  public void testRightPad() {
    String s= "my string";

    String ps= StringUtil.rightPad(s, 0);
    assertTrue(s.equals(ps));

    ps= StringUtil.rightPad(s, 9);
    assertTrue(s.equals(ps));

    ps= StringUtil.rightPad(s, 10);
    assertTrue( (s+" ").equals(ps) );

    ps= StringUtil.rightPad(s, 15);
    assertTrue( (s+"      ").equals(ps) );

  }

  @Test
  public void testLeftPad() {
    String s= "my string";

    String ps= StringUtil.leftPad(s, 0);
    assertTrue(s.equals(ps));

    ps= StringUtil.leftPad(s, 9);
    assertTrue(s.equals(ps));

    ps= StringUtil.leftPad(s, 10);
    assertTrue( (" "+s).equals(ps) );

    ps= StringUtil.leftPad(s, 15);
    assertTrue( ("      "+s).equals(ps) );

  }

}
