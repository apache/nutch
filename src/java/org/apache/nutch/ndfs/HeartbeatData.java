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
package org.apache.nutch.ndfs;

import org.apache.nutch.io.*;
import org.apache.nutch.ipc.*;
import org.apache.nutch.util.*;

import java.io.*;
import java.net.*;
import java.util.*;

/********************************************************
 * Heartbeat data
 *
 * @author Mike Cafarella
 ********************************************************/
public class HeartbeatData implements Writable, FSConstants {
    UTF8 name;
    long capacity, remaining;

    /**
     */
    public HeartbeatData() {
        this.name = new UTF8();
    }
        
    /**
     */
    public HeartbeatData(String name, long capacity, long remaining) {
        this.name = new UTF8(name);
        this.capacity = capacity;
        this.remaining = remaining;
    }
        
    /**
     */
    public void write(DataOutput out) throws IOException {
        name.write(out);
        out.writeLong(capacity);
        out.writeLong(remaining);
    }

    /**
     */
    public void readFields(DataInput in) throws IOException {
        this.name = new UTF8();
        name.readFields(in);
        this.capacity = in.readLong();
        this.remaining = in.readLong();
    }

    public UTF8 getName() {
        return name;
    }
    public long getCapacity() {
        return capacity;
    }
    public long getRemaining() {
        return remaining;
    }
}
