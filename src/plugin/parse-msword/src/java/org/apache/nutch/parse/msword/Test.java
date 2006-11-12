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

package org.apache.nutch.parse.msword;

import java.io.*;
/**
 * Title:
 * Description:
 * Copyright:    Copyright (c) 2003
 * Company:
 * @author
 * @version 1.0
 */

class Test
{

  public Test()
  {
  }
  public static void main(String[] args)
  {
    try
    {
      WordExtractor extractor = new WordExtractor();
      String s = extractor.extractText(new FileInputStream(args[0]));
      System.out.println(s);
      //OutputStreamWriter out = new OutputStreamWriter(new FileOutputStream("C:\\test.txt"), "UTF-16LE");
      OutputStreamWriter out = new OutputStreamWriter(new FileOutputStream("./test.txt"));
      out.write(s);
      out.flush();
      out.close();
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
  }
}
