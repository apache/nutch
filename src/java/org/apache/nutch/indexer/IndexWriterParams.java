package org.apache.nutch.indexer;

import org.apache.hadoop.util.StringUtils;

import java.util.HashMap;
import java.util.Map;

public class IndexWriterParams extends HashMap<String, String> {

    /**
     * Constructs a new <tt>HashMap</tt> with the same mappings as the
     * specified <tt>Map</tt>.  The <tt>HashMap</tt> is created with
     * default load factor (0.75) and an initial capacity sufficient to
     * hold the mappings in the specified <tt>Map</tt>.
     *
     * @param m the map whose mappings are to be placed in this map
     * @throws NullPointerException if the specified map is null
     */
    IndexWriterParams(Map<? extends String, ? extends String> m) {
        super(m);
    }

    public String get(String name, String defaultValue) {
        return this.getOrDefault(name, defaultValue);
    }

    public boolean getBoolean(String name, boolean defaultValue) {
        String value;
        if ((value = this.get(name)) != null) {
            return Boolean.parseBoolean(value);
        }

        return defaultValue;
    }

    public long getLong(String name, long defaultValue) {
        String value;
        if ((value = this.get(name)) != null) {
            return Long.parseLong(value);
        }

        return defaultValue;
    }

    public int getInt(String name, int defaultValue) {
        String value;
        if ((value = this.get(name)) != null) {
            return Integer.parseInt(value);
        }

        return defaultValue;
    }

    public String[] getStrings(String name) {
        String value = this.get(name);
        return StringUtils.getStrings(value);
    }
}
