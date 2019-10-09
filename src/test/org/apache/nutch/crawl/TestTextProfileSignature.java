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
package org.apache.nutch.crawl;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseImpl;
import org.apache.nutch.parse.ParseStatus;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.StringUtil;
import org.junit.Assert;
import org.junit.Test;

public class TestTextProfileSignature {

  @Test
  public void testGetSignature() {
    Configuration conf = NutchConfiguration.create();
    Signature textProf = new TextProfileSignature();
    textProf.setConf(conf);
    String text = "Hello World The Quick Brown Fox Jumped Over the Lazy Fox";
    ParseData pd = new ParseData(ParseStatus.STATUS_SUCCESS, "Hello World",
        new Outlink[0], new Metadata());
    byte[] signature1 = textProf.calculate(new Content(),
        new ParseImpl(text, pd));
    Assert.assertNotNull(signature1);
    List<String> words = Arrays.asList(text.split("\\s"));
    Collections.shuffle(words);
    String text2 = String.join(" ", words);
    byte[] signature2 = textProf.calculate(new Content(),
        new ParseImpl(text2, pd));
    Assert.assertNotNull(signature2);
    Assert.assertEquals(StringUtil.toHexString(signature1),
        StringUtil.toHexString(signature2));
  }
}
