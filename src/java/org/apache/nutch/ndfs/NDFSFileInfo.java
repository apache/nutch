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
import org.apache.nutch.util.*;

import java.io.*;
import java.util.*;

/******************************************************
 * NDFSFileInfo tracks info about remote files, including
 * name, size, etc.  
 * 
 * @author Mike Cafarella
 ******************************************************/
public class NDFSFileInfo implements Writable {
    UTF8 path;
    long len;
    long contentsLen;
    boolean isDir;

    /**
     */
    public NDFSFileInfo() {
    }

    /**
     */
    public NDFSFileInfo(UTF8 path, long len, long contentsLen, boolean isDir) {
        this.path = path;
        this.len = len;
        this.contentsLen = contentsLen;
        this.isDir = isDir;
    }

    /**
     */
    public String getPath() {
        return new File(path.toString()).getPath();
    }

    /**
     */
    public String getName() {
        return new File(path.toString()).getName();
    }

    /**
     */
    public String getParent() {
        return new File(path.toString()).getParent();
    }

    /**
     */
    public long getLen() {
        return len;
    }

    /**
     */
    public long getContentsLen() {
        return contentsLen;
    }

    /**
     */
    public boolean isDir() {
        return isDir;
    }

    //////////////////////////////////////////////////
    // Writable
    //////////////////////////////////////////////////
    public void write(DataOutput out) throws IOException {
        path.write(out);
        out.writeLong(len);
        out.writeLong(contentsLen);
        out.writeBoolean(isDir);
    }

    public void readFields(DataInput in) throws IOException {
        this.path = new UTF8();
        this.path.readFields(in);
        this.len = in.readLong();
        this.contentsLen = in.readLong();
        this.isDir = in.readBoolean();
    }
}

