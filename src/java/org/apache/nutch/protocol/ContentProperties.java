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

import java.util.Enumeration;
import java.util.Iterator;
import java.util.Properties;
import java.util.TreeMap;

/**
 * case insensitive properties
 */
public class ContentProperties extends TreeMap {

    /**
     * construct the TreeMap with a case insensitive comparator
     */
    public ContentProperties() {
        super(String.CASE_INSENSITIVE_ORDER);
    }

    /**
     * initialize with default values
     * 
     * @param defaults
     */
    public ContentProperties(Properties defaults) {
        super(String.CASE_INSENSITIVE_ORDER);
        putAll(defaults);
    }

    /**
     * @param key
     * @return the property value or null
     */
    public String getProperty(String key) {
        return (String) get(key);
    }

    /**
     * sets the key value tuple
     * 
     * @param key
     * @param value
     */
    public void setProperty(String key, String value) {
        put(key, value);

    }

    public Enumeration propertyNames() {
        return new KeyEnumeration(keySet().iterator());
    }

    class KeyEnumeration implements Enumeration {

        private Iterator fIterator;

        public KeyEnumeration(Iterator iterator) {
            fIterator = iterator;
        }

        public boolean hasMoreElements() {
            return fIterator.hasNext();

        }

        public Object nextElement() {
            return fIterator.next();
        }

    }

}
