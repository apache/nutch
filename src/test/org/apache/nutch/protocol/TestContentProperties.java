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

package org.apache.nutch.protocol;

import org.apache.nutch.io.TestWritable;

import junit.framework.TestCase;

public class TestContentProperties extends TestCase {

    public void testOneValue() throws Exception {
        ContentProperties properties = new ContentProperties();
        String value = "aValue";
        properties.setProperty("aKey", value);
        assertEquals(value, properties.get("aKey"));
        assertEquals(value, properties.get("akey"));
    }

    public void testMultiValue() throws Exception {
        ContentProperties properties = new ContentProperties();
        String value = "aValue";
        for (int i = 0; i < 100; i++) {
            properties.setProperty("aKey", value + i);

        }
        assertEquals(value + 99, properties.get("aKey"));
        assertEquals(value + 99, properties.getProperty("aKey"));
        String[] propertie = properties.getProperties("aKey");
        for (int i = 0; i < 100; i++) {
            assertEquals(value + i, propertie[i]);

        }
    }

    public void testSerialization() throws Exception {
        ContentProperties properties = new ContentProperties();
        for (int i = 0; i < 10; i++) {
            properties.setProperty("key", "" + i);
        }
        TestWritable.testWritable(properties);
        Content content = new Content("url", "url", new byte[0], "text/html",
                new ContentProperties());
        ContentProperties metadata = content.getMetadata();
        for (int i = 0; i < 100; i++) {
            metadata.setProperty("aKey", "" + i);
        }
        TestWritable.testWritable(content);
    }

}
