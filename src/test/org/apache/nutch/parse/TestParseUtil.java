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

package org.apache.nutch.parse;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.crawl.CrawlStatus;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.NutchConfiguration;
import org.junit.Before;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import static org.junit.Assert.*;

/**
 * Unit tests for ParseUtil methods.
 * 
 * @author Canan Girgin
 *
 */
public class TestParseUtil {

    private Configuration conf;
    private ParseUtil parseUtil;

    /**
     * Inits the Test Case
     */
    @Before
    public void setUp() throws Exception {
        conf = NutchConfiguration.create();
        conf.set("parser.html.outlinks.max.target.length", "40");
        parseUtil = new ParseUtil(conf);
    }

    @Test
    public void testOutlinksMaxLength() throws UnsupportedEncodingException {
        WebPage page = WebPage.newBuilder().build();
        page.setBaseUrl(new Utf8("http://www.example.com/"));
        page.setContentType(new Utf8("text/plain"));
        String content= "Test with http://www.nutch.org/index.html is it found? "
                + "What about http://www.apache.org/foundation/ "
                + "A longer URL could be http://www.sybit.com/solutions/portals.html";
        page.setContent(ByteBuffer.wrap(content.getBytes("utf-8")));
        page.setStatus((int)CrawlStatus.STATUS_FETCHED);
        parseUtil.process("http://www.example.com/", page);
        assertTrue("Wrong URL!", page.getOutlinks().size() == 2);
        assertTrue("Wrong URL", page.getOutlinks().containsKey(new Utf8("http://www.nutch.org/index.html")));
        assertTrue("Wrong URL", page.getOutlinks().containsKey(new Utf8("http://www.apache.org/foundation/")));

    }

}
