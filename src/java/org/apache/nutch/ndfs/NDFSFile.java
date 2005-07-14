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

import java.io.*;


/*****************************************************************
 * NDFSFile is a traditional java File that's been annotated with
 * some extra information.
 *
 * @author Mike Cafarella
 *****************************************************************/
public class NDFSFile extends File {
    NDFSFileInfo info;

    /** Separator used in NDFS filenames. */
    public static final String NDFS_FILE_SEPARATOR = "/";
    
    /**
     */
    public NDFSFile(NDFSFileInfo info) {
        super(info.getPath());
        this.info = info;
    }

    /**
     * A number of File methods are unsupported in this subclass
     */
    public boolean canRead() {
        return false;
    }
    public boolean canWrite() {
        return false;
    }
    public boolean createNewFile() {
        return false;
    }
    public boolean delete() {
        return false;
    }
    public void deleteOnExit() {
    }
    public boolean isHidden() {
        return false;
    }

    /**
     * We need to reimplement some of them
     */
    public boolean isDirectory() {
        return info.isDir();
    }
    public boolean isFile() {
        return ! isDirectory();
    }
    public long length() {
        return info.getLen();
    }

    /**
     * And add a few extras
     */
    public long getContentsLength() {
        return info.getContentsLen();
    }
    
    /**
     * Retrieving parent path from NDFS path string
     * @param path - NDFS path 
     * @return - parent path of NDFS path, or null if no parent exist.
     */
    public static String getNDFSParent(String path) {
        if (path == null)
            return null;
        if (NDFS_FILE_SEPARATOR.equals(path))
            return null;
        int index = path.lastIndexOf(NDFS_FILE_SEPARATOR); 
        if (index == -1)
            return null;
        if (index == 0)
            return NDFS_FILE_SEPARATOR;
        return path.substring(0, index);
    }
}
