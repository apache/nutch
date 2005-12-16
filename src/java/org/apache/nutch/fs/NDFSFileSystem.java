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

package org.apache.nutch.fs;

import java.io.*;
import java.net.*;
import java.util.*;

import org.apache.nutch.io.*;
import org.apache.nutch.ndfs.*;
import org.apache.nutch.util.NutchConf;

/****************************************************************
 * Implementation of the abstract NutchFileSystem for the NDFS system.
 * This is the distributed file system.  It can be distributed over
 * 1 or more machines 
 * @author Mike Cafarella
 *****************************************************************/
public class NDFSFileSystem extends NutchFileSystem {
    private static final String HOME_DIR =
      "/user/" + System.getProperty("user.name") + "/";

    private Random r = new Random();
    private String name;

    NDFSClient ndfs;

    /**
     * Create the ShareSet automatically, and then go on to
     * the regular constructor.
     */
    public NDFSFileSystem(InetSocketAddress namenode) throws IOException {
      this.ndfs = new NDFSClient(namenode);
      this.name = namenode.getHostName() + ":" + namenode.getPort();
    }

    public String getName() { return name; }

    private UTF8 getPath(File file) {
      String path = getNDFSPath(file);
      if (!path.startsWith(NDFSFile.NDFS_FILE_SEPARATOR)) {
        path = getNDFSPath(new File(HOME_DIR, path)); // make absolute
      }
      return new UTF8(path);
    }

    public String[][] getFileCacheHints(File f, long start, long len) throws IOException {
      return ndfs.getHints(getPath(f), start, len);
    }

    public NFSInputStream openRaw(File f) throws IOException {
      return ndfs.open(getPath(f));
    }

    public NFSOutputStream createRaw(File f, boolean overwrite)
      throws IOException {
      return ndfs.create(getPath(f), overwrite);
    }

    /**
     * Rename files/dirs
     */
    public boolean renameRaw(File src, File dst) throws IOException {
      return ndfs.rename(getPath(src), getPath(dst));
    }

    /**
     * Get rid of File f, whether a true file or dir.
     */
    public boolean deleteRaw(File f) throws IOException {
        return ndfs.delete(getPath(f));
    }

    /**
     */
    public boolean exists(File f) throws IOException {
        return ndfs.exists(getPath(f));
    }

    /**
     */
    public boolean isDirectory(File f) throws IOException {
        return ndfs.isDirectory(getPath(f));
    }

    /**
     */
    public long getLength(File f) throws IOException {
        NDFSFileInfo info[] = ndfs.listFiles(getPath(f));
        return info[0].getLen();
    }

    /**
     */
    public File[] listFilesRaw(File f) throws IOException {
        NDFSFileInfo info[] = ndfs.listFiles(getPath(f));
        if (info == null) {
            return new File[0];
        } else {
            File results[] = new NDFSFile[info.length];
            for (int i = 0; i < info.length; i++) {
                results[i] = new NDFSFile(info[i]);
            }
            return results;
        }
    }

    /**
     */
    public void mkdirs(File f) throws IOException {
        ndfs.mkdirs(getPath(f));
    }

    /**
     * Obtain a filesystem lock at File f.
     */
    public void lock(File f, boolean shared) throws IOException {
        ndfs.lock(getPath(f), ! shared);
    }

    /**
     * Release a held lock
     */
    public void release(File f) throws IOException {
        ndfs.release(getPath(f));
    }

    /**
     * Remove the src when finished.
     */
    public void moveFromLocalFile(File src, File dst) throws IOException {
        doFromLocalFile(src, dst, true);
    }

    /**
     * keep the src when finished.
     */
    public void copyFromLocalFile(File src, File dst) throws IOException {
        doFromLocalFile(src, dst, false);
    }

    private void doFromLocalFile(File src, File dst, boolean deleteSource) throws IOException {
        if (exists(dst)) {
            if (! isDirectory(dst)) {
                throw new IOException("Target " + dst + " already exists");
            } else {
                dst = new File(dst, src.getName());
                if (exists(dst)) {
                    throw new IOException("Target " + dst + " already exists");
                }
            }
        }

        if (src.isDirectory()) {
            mkdirs(dst);
            File contents[] = src.listFiles();
            for (int i = 0; i < contents.length; i++) {
                doFromLocalFile(contents[i], new File(dst, contents[i].getName()), deleteSource);
            }
        } else {
            byte buf[] = new byte[NutchConf.get().getInt("io.file.buffer.size", 4096)];
            InputStream in = new BufferedInputStream(new FileInputStream(src));
            try {
                OutputStream out = create(dst);
                try {
                    int bytesRead = in.read(buf);
                    while (bytesRead >= 0) {
                        out.write(buf, 0, bytesRead);
                        bytesRead = in.read(buf);
                    }
                } finally {
                    out.close();
                }
            } finally {
                in.close();
            } 
        }
        if (deleteSource)
            src.delete();
    }

    /**
     * Takes a hierarchy of files from the NFS system and writes to
     * the given local target.
     */
    public void copyToLocalFile(File src, File dst) throws IOException {
        if (dst.exists()) {
            if (! dst.isDirectory()) {
                throw new IOException("Target " + dst + " already exists");
            } else {
                dst = new File(dst, src.getName());
                if (dst.exists()) {
                    throw new IOException("Target " + dst + " already exists");
                }
            }
        }
        dst = dst.getCanonicalFile();

        if (isDirectory(src)) {
            dst.mkdirs();
            File contents[] = listFiles(src);
            for (int i = 0; i < contents.length; i++) {
                copyToLocalFile(contents[i], new File(dst, contents[i].getName()));
            }
        } else {
            byte buf[] = new byte[NutchConf.get().getInt("io.file.buffer.size", 4096)];
            InputStream in = open(src);
            try {
                OutputStream out = NutchFileSystem.getNamed("local").create(dst);
                try {
                    int bytesRead = in.read(buf);
                    while (bytesRead >= 0) {
                        out.write(buf, 0, bytesRead);
                        bytesRead = in.read(buf);
                    }
                } finally {
                    out.close();
                }
            } finally {
                in.close();
            } 
        }
    }

    /**
     * Output will go to the tmp working area.  There may be some source
     * material that we obtain first.
     */
    public File startLocalOutput(File nfsOutputFile, File tmpLocalFile) throws IOException {
        if (exists(nfsOutputFile)) {
            copyToLocalFile(nfsOutputFile, tmpLocalFile);
        }
        return tmpLocalFile;
    }

    /**
     * Move completed local data to NDFS destination
     */
    public void completeLocalOutput(File nfsOutputFile, File tmpLocalFile) throws IOException {
        moveFromLocalFile(tmpLocalFile, nfsOutputFile);
    }

    /**
     * Fetch remote NDFS file, place at tmpLocalFile
     */
    public File startLocalInput(File nfsInputFile, File tmpLocalFile) throws IOException {
        copyToLocalFile(nfsInputFile, tmpLocalFile);
        return tmpLocalFile;
    }

    /**
     * We're done with the local stuff, so delete it
     */
    public void completeLocalInput(File localFile) throws IOException {
        // Get rid of the local copy - we don't need it anymore.
        FileUtil.fullyDelete(localFile);
    }

    /**
     * Shut down the FS.  Not necessary for regular filesystem.
     */
    public void close() throws IOException {
        ndfs.close();
    }

    /**
     */
    public String toString() {
        return "NDFS[" + ndfs + "]";
    }

    /**
     */
    public NDFSClient getClient() {
        return ndfs;
    }
    
    private String getNDFSPath(File f) {
      List l = new ArrayList();
      l.add(f.getName());
      File parent = f.getParentFile();
      while (parent != null) {
        l.add(parent.getName());
        parent = parent.getParentFile();
      }
      StringBuffer path = new StringBuffer();
      path.append(l.get(l.size() - 1));
      for (int i = l.size() - 2; i >= 0; i--) {
        path.append(NDFSFile.NDFS_FILE_SEPARATOR);
        path.append(l.get(i));
      }
      return path.toString();
    }

    public void reportChecksumFailure(File f, NFSInputStream in,
                                      long start, long length, int crc) {
      
      // ignore for now, causing task to fail, and hope that when task is
      // retried it gets a different copy of the block that is not corrupt.

      // FIXME: we should move the bad block(s) involved to a bad block
      // directory on their datanode, and then re-replicate the blocks, so that
      // no data is lost. a task may fail, but on retry it should succeed.
    }
}
