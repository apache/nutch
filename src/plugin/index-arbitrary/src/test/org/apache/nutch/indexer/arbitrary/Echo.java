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
package org.apache.nutch.indexer.arbitrary;


import java.io.PrintStream;

public class Echo {

  private static PrintStream out = System.out;
  private String words;

  public Echo(String args[]) {
    super();
    words = String.valueOf(args[1]);
  }

  public String getText() {
    return words;
  }

  public static void main(String[] args) {
    Echo echo = new Echo(args);
    out.println(echo.getText());
  }
}
