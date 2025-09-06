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

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class DumpFileUtilTest {

    @Test
    public void testGetUrlMD5() throws Exception {
        String testUrl = "http://apache.org";

        String result = DumpFileUtil.getUrlMD5(testUrl);

        assertThat(result, is("991e599262e04ea2ec76b6c5aed499a7"));
    }

    @Test
    public void testCreateTwoLevelsDirectory() throws Exception {
        String testUrl = "http://apache.org";
        String basePath = "/tmp";
        String fullDir = DumpFileUtil.createTwoLevelsDirectory(basePath, DumpFileUtil.getUrlMD5(testUrl));

        assertThat(fullDir, is("/tmp/96/ea"));

        String basePath2 = "/this/path/is/not/existed/just/for/testing";
        String fullDir2 = DumpFileUtil.createTwoLevelsDirectory(basePath2, DumpFileUtil.getUrlMD5(testUrl));

        assertThat(fullDir2, nullValue());
    }

    @Test
    public void testCreateFileName() throws Exception {
        String testUrl = "http://apache.org";
        String baseName = "test";
        String extension = "html";
        String fullDir = DumpFileUtil.createFileName(DumpFileUtil.getUrlMD5(testUrl), baseName, extension);

        assertThat(fullDir, is("991e599262e04ea2ec76b6c5aed499a7_test.html"));

        String tooLongBaseName = "testtesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttest";
        String fullDir2 = DumpFileUtil.createFileName(DumpFileUtil.getUrlMD5(testUrl), tooLongBaseName, extension);

        assertThat(fullDir2,
            is("991e599262e04ea2ec76b6c5aed499a7_testtesttesttesttesttesttesttest.html"));

        String tooLongExtension = "testtesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttest";
        String fullDir3 = DumpFileUtil.createFileName(DumpFileUtil.getUrlMD5(testUrl), baseName, tooLongExtension);

        assertThat(fullDir3, is("991e599262e04ea2ec76b6c5aed499a7_test.testt"));
    }
}
