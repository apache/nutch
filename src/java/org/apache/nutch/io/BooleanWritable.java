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

package org.apache.nutch.io;

import java.io.*;

/** 
 * A WritableComparable for booleans. 
 */
public class BooleanWritable implements WritableComparable {
    private boolean value;

    /** 
     */
    public BooleanWritable() {};

    /** 
     */
    public BooleanWritable(boolean value) {
        set(value);
    }

    /** 
     * Set the value of the BooleanWritable
     */    
    public void set(boolean value) {
        this.value = value;
    }

    /**
     * Returns the value of the BooleanWritable
     */
    public boolean get() {
        return value;
    }

    /**
     */
    public void readFields(DataInput in) throws IOException {
        value = in.readBoolean();
    }

    /**
     */
    public void write(DataOutput out) throws IOException {
        out.writeBoolean(value);
    }

    /**
     */
    public boolean equals(Object o) {
        if (!(o instanceof BooleanWritable)) {
            return false;
        }
        BooleanWritable other = (BooleanWritable) o;
        return this.value == other.value;
    }

    public int hashCode() {
      return value ? 0 : 1;
    }



    /**
     */
    public int compareTo(Object o) {
        boolean a = this.value;
        boolean b = ((BooleanWritable) o).value;
        return ((a == b) ? 0 : (a == false) ? -1 : 1);
    }

    /** 
     * A Comparator optimized for BooleanWritable. 
     */ 
    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(BooleanWritable.class);
        }

        public int compare(byte[] b1, int s1, int l1,
                           byte[] b2, int s2, int l2) {
            boolean a = (readInt(b1, s1) == 1) ? true : false;
            boolean b = (readInt(b2, s2) == 1) ? true : false;
            return ((a == b) ? 0 : (a == false) ? -1 : 1);
        }
    }


    static {
      WritableComparator.define(BooleanWritable.class, new Comparator());
    }
}
