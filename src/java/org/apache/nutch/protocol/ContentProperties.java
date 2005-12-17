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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Properties;
import java.util.TreeMap;

import org.apache.nutch.io.UTF8;
import org.apache.nutch.io.Writable;

/**
 * writable case insensitive properties
 */
public class ContentProperties extends TreeMap implements Writable {

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

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Map#get(java.lang.Object)
     */
    public Object get(Object arg0) {
        Object object = super.get(arg0);
        if (object != null && object instanceof ArrayList) {
            ArrayList list = (ArrayList) object;
            return list.get(list.size() - 1);
        }
        return object;
    }

    /**
     * @param key
     * @return the properties as a string array if there is no such property we
     *         retunr a array with 0 entries
     */
    public String[] getProperties(String key) {
        Object object = super.get(key);
        if (object != null && !(object instanceof ArrayList)) {
            return new String[] { (String) object };
        } else if (object != null && object instanceof ArrayList) {
            ArrayList list = (ArrayList) object;
            return (String[]) list.toArray(new String[list.size()]);
        }
        return new String[0];
    }

    /**
     * sets the key value tuple
     * 
     * @param key
     * @param value
     */
    public void setProperty(String key, String value) {
        Object object = super.get(key);
        if (object != null && !(object instanceof ArrayList)) {
            ArrayList arrayList = new ArrayList();
            arrayList.add(object);
            arrayList.add(value);
            put(key, arrayList);
        } else if (object instanceof ArrayList) {
            ((ArrayList) object).add(value);
        } else {
            put(key, value);
        }

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

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.nutch.io.Writable#write(java.io.DataOutput)
     */
    public final void write(DataOutput out) throws IOException {
        out.writeInt(keySet().size());
        Iterator iterator = keySet().iterator();
        String key;
        String[] properties;
        while (iterator.hasNext()) {
            key = (String) iterator.next();
            UTF8.writeString(out, key);
            properties = getProperties(key);
            out.writeInt(properties.length);
            for (int i = 0; i < properties.length; i++) {
                UTF8.writeString(out, properties[i]);
            }
        }

    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.nutch.io.Writable#readFields(java.io.DataInput)
     */
    public final void readFields(DataInput in) throws IOException {
        int keySize = in.readInt();
        String key;
        for (int i = 0; i < keySize; i++) {
            key = UTF8.readString(in);
            int valueSize = in.readInt();
            for (int j = 0; j < valueSize; j++) {
                setProperty(key, UTF8.readString(in));
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    public boolean equals(Object obj) {
        if (!(obj instanceof ContentProperties)) {
            return false;
        }
        ContentProperties properties = (ContentProperties) obj;
        Enumeration enumeration = properties.propertyNames();
        while (enumeration.hasMoreElements()) {
            String key = (String) enumeration.nextElement();
            String[] values = properties.getProperties(key);
            String[] myValues = getProperties(key);
            if (values.length != myValues.length) {
                return false;
            }
            for (int i = 0; i < values.length; i++) {
                if (!values[i].equals(myValues[i])) {
                    return false;
                }

            }
        }

        return true;
    }

}
