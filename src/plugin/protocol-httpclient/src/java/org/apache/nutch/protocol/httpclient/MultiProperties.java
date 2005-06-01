/* Copyright (c) 2004 The Nutch Organization. All rights reserved. */
/* Use subject to the conditions in http://www.nutch.org/LICENSE.txt. */

package org.apache.nutch.protocol.httpclient;

import java.util.Properties;
import java.util.Map;
import java.util.TreeMap;
import java.util.Vector;
import java.util.Iterator;
import java.util.logging.Logger;

import org.apache.nutch.util.LogFormatter;

/**
 * An extension to {@link Properties} which allows multiple values for a single key.
 * The {@link #get(Object)} method may return a single value or a
 * {@link java.util.Collection} of values.
 *
 * @author Matt Tencati
 */

public class MultiProperties extends Properties {
    public static final Logger LOG = LogFormatter
            .getLogger("net.nutch.protocol.http.MultiProperties");

    private Map multiMap = null;

    /**
     * Creates an empty MultiProperties list with no default values.
     */
    public MultiProperties() {
        super();
        multiMap = new TreeMap();
    }

    /**
     * Creates an empty MultiProperties list with the specified defaults.
     * 
     * @param defaults A Properties object to use as default values
     */
    public MultiProperties(Properties defaults) {
        super(defaults);
        multiMap = new TreeMap();
    }

    /** 
     * Adds the given value using the key
     * 
     * @param key The key for the value
     * @param value The value to store in the Property map
     * 
     * @see java.util.Dictionary#put(java.lang.Object, java.lang.Object)
     */
    public Object put(Object key, Object value) throws NullPointerException {
        Object o = multiMap.get(key);
        Vector values = null;
        if (o == null || !(o instanceof Vector)) {
            values = new Vector();
            multiMap.put(key, values);
        } else {
            values = (Vector) o;
        }
        values.add(value);
        multiMap.put(key, values);

        return super.put(key, value);
    }

    /**
     * Add all entries of the given map to this MultiProperties list.  The 
     * entries will be added using the key retrieved from {@link Map#keySet()}
     * 
     * @param t The {@link Map} object which containts the values to add.
     * 
     * @see java.util.Map#putAll(java.util.Map)
     */
    public void putAll(Map t) throws NullPointerException {
        //LOG.info("PUT ALL\n" + t.toString());
        Iterator i = t.keySet().iterator();
        while (i.hasNext()) {
            Object key = i.next();
            Object value = t.get(key);
            put(key, value);
        }
    }

    /** 
     * Returns the value associated with the given key.  The return object
     * may be either a single value (String) or a Collection of values.
     * 
     * @param key Used to lookup the value
     * @return Returns a single value or multiple values if available
     * @see java.util.Dictionary#get(java.lang.Object)
     */
    public Object get(Object key) throws NullPointerException {
        if (key == null)
            throw new NullPointerException("Lookup null key");
        Object o = multiMap.get(key);
        if (o != null && o instanceof Vector) {
            if (((Vector) o).size() == 1) {
                return super.get(key);
            }
        }
        return o;
    }

    /** 
     * A string representation of this MultiProperties list.
     * 
     * @return A multi-line String object.
     * @see java.lang.Object#toString()
     */
    public String toString() {
        StringBuffer sb = new StringBuffer();
        Iterator i = multiMap.keySet().iterator();
        while (i.hasNext()) {
            String key = (String) i.next();
            Vector values = (Vector) multiMap.get(key);
            Iterator valIterator = values.iterator();
            sb.append(key + " {\n");
            while (valIterator.hasNext()) {
                String value = (String) valIterator.next();
                sb.append("\t" + value + "\n");
            }
            sb.append("\t}");
        }
        return sb.toString();
    }

}
