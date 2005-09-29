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
import java.util.*;
import java.nio.channels.*;

import org.apache.nutch.ndfs.NDFSFile;
import org.apache.nutch.ndfs.NDFSFileInfo;
import org.apache.nutch.io.UTF8;

/****************************************************************
 * Implement the NutchFileSystem interface for the local disk.
 * This is pretty easy.  The interface exists so we can use either
 * remote or local Files very easily.
 *
 * @author Mike Cafarella
 *****************************************************************/
public class LocalFileSystem extends NutchFileSystem {
    TreeMap sharedLockDataSet = new TreeMap();
    TreeMap nonsharedLockDataSet = new TreeMap();
    TreeMap lockObjSet = new TreeMap();
    // by default use copy/delete instead of rename
    boolean useCopyForRename = true;

    /**
     */
    public LocalFileSystem() throws IOException {
        super();
        // if you find an OS which reliably supports non-POSIX
        // rename(2) across filesystems / volumes, you can
        // uncomment this.
        // String os = System.getProperty("os.name");
        // if (os.toLowerCase().indexOf("os-with-super-rename") != -1)
        //     useCopyForRename = false;
    }


    public String getName() { return "local"; }

    /*******************************************************
     * For open()'s NFSInputStream
     *******************************************************/
    class LocalNFSFileInputStream extends NFSInputStream {
        FileInputStream fis;

        public LocalNFSFileInputStream(File f) throws IOException {
          this.fis = new FileInputStream(f);
        }

        public void seek(long pos) throws IOException {
          fis.getChannel().position(pos);
        }

        public long getPos() throws IOException {
          return fis.getChannel().position();
        }

        /*
         * Just forward to the fis
         */
        public int available() throws IOException { return fis.available(); }
        public void close() throws IOException { fis.close(); }
        public boolean markSupport() { return false; }
        public int read() throws IOException { return fis.read(); }
        public int read(byte[] b) throws IOException { return fis.read(b); }
        public int read(byte[] b, int off, int len) throws IOException {
            return fis.read(b, off, len);
        }
        public long skip(long n) throws IOException { return fis.skip(n); }
    }
    
    /**
     * Open the file at f
     */
    public NFSInputStream open(File f) throws IOException {
        if (! f.exists()) {
            throw new FileNotFoundException(f.toString());
        }
        return new LocalNFSFileInputStream(f);
    }

    /*********************************************************
     * For create()'s NFSOutputStream.
     *********************************************************/
    class LocalNFSFileOutputStream extends NFSOutputStream {
      FileOutputStream fos;

      public LocalNFSFileOutputStream(File f) throws IOException {
        this.fos = new FileOutputStream(f);
      }

      public long getPos() throws IOException {
        return fos.getChannel().position();
      }

      /*
       * Just forward to the fos
       */
      public void close() throws IOException { fos.close(); }
      public void flush() throws IOException { fos.flush(); }
      public void write(byte[] b) throws IOException { fos.write(b); }
      public void write(byte[] b, int off, int len) throws IOException {
        fos.write(b, off, len);
      }
      public void write(int b) throws IOException { fos.write(b); }
    }

    public NFSOutputStream create(File f, boolean overwrite) throws IOException {
        if (f.exists() && ! overwrite) {
            throw new IOException("File already exists:"+f);
        }
        File parent = f.getParentFile();
        if (parent != null)
          parent.mkdirs();

        return new LocalNFSFileOutputStream(f);
    }

    /**
     * Rename files/dirs
     */
    public boolean rename(File src, File dst) throws IOException {
        if (useCopyForRename) {
            FileUtil.copyContents(this, src, dst, true);
            return fullyDelete(src);
        } else return src.renameTo(dst);
    }

    /**
     * Get rid of File f, whether a true file or dir.
     */
    public boolean delete(File f) throws IOException {
        if (f.isFile()) {
            return f.delete();
        } else return fullyDelete(f);
    }

    /**
     */
    public boolean exists(File f) throws IOException {
        return f.exists();
    }

    /**
     */
    public boolean isDirectory(File f) throws IOException {
        return f.isDirectory();
    }

    /**
     */
    public long getLength(File f) throws IOException {
        return f.length();
    }

    /**
     */
    public File[] listFiles(File f) throws IOException {
        File[] files = f.listFiles();
        if (files == null) return null;
        // 20041022, xing, Watch out here:
        // currently NDFSFile.java does not support those methods
        //    public boolean canRead()
        //    public boolean canWrite()
        //    public boolean createNewFile()
        //    public boolean delete()
        //    public void deleteOnExit()
        //    public boolean isHidden()
        // so you can not rely on returned list for these operations.
        NDFSFile[] nfiles = new NDFSFile[files.length];
        for (int i = 0; i < files.length; i++) {
            long len = files[i].length();
            UTF8 name = new UTF8(files[i].toString());
            NDFSFileInfo info = new NDFSFileInfo(name, len, len, files[i].isDirectory());
            nfiles[i] = new NDFSFile(info);
        }
        return nfiles;
    }

    /**
     */
    public void mkdirs(File f) throws IOException {
        f.mkdirs();
    }

    /**
     * Obtain a filesystem lock at File f.
     */
    public synchronized void lock(File f, boolean shared) throws IOException {
        f.createNewFile();

        FileLock lockObj = null;
        if (shared) {
            FileInputStream lockData = new FileInputStream(f);
            lockObj = lockData.getChannel().lock(0L, Long.MAX_VALUE, shared);
            sharedLockDataSet.put(f, lockData);
        } else {
            FileOutputStream lockData = new FileOutputStream(f);
            lockObj = lockData.getChannel().lock(0L, Long.MAX_VALUE, shared);
            nonsharedLockDataSet.put(f, lockData);
        }
        lockObjSet.put(f, lockObj);
    }

    /**
     * Release a held lock
     */
    public synchronized void release(File f) throws IOException {
        FileLock lockObj = (FileLock) lockObjSet.get(f);
        FileInputStream sharedLockData = (FileInputStream) sharedLockDataSet.get(f);
        FileOutputStream nonsharedLockData = (FileOutputStream) nonsharedLockDataSet.get(f);

        if (lockObj == null) {
            throw new IOException("Given target not held as lock");
        }
        if (sharedLockData == null && nonsharedLockData == null) {
            throw new IOException("Given target not held as lock");
        }

        lockObj.release();
        lockObjSet.remove(f);
        if (sharedLockData != null) {
            sharedLockData.close();
            sharedLockDataSet.remove(f);
        } else {
            nonsharedLockData.close();
            nonsharedLockDataSet.remove(f);
        }
    }

    /**
     * In the case of the local filesystem, we can just rename the file.
     */
    public void moveFromLocalFile(File src, File dst) throws IOException {
        if (! src.equals(dst)) {
            if (useCopyForRename) {
                FileUtil.copyContents(this, src, dst, true);
                fullyDelete(src);
            } else src.renameTo(dst);
        }
    }

    /**
     * Similar to moveFromLocalFile(), except the source is kept intact.
     */
    public void copyFromLocalFile(File src, File dst) throws IOException {
        if (! src.equals(dst)) {
            FileUtil.copyContents(this, src, dst, true);
        }
    }

    /**
     * We can't delete the src file in this case.  Too bad.
     */
    public void copyToLocalFile(File src, File dst) throws IOException {
        if (! src.equals(dst)) {
            FileUtil.copyContents(this, src, dst, true);
        }
    }

    /**
     * We can write output directly to the final location
     */
    public File startLocalOutput(File nfsOutputFile, File tmpLocalFile) throws IOException {
        return nfsOutputFile;
    }

    /**
     * It's in the right place - nothing to do.
     */
    public void completeLocalOutput(File nfsWorkingFile, File tmpLocalFile) throws IOException {
    }

    /**
     * We can read directly from the real local fs.
     */
    public File startLocalInput(File nfsInputFile, File tmpLocalFile) throws IOException {
        return nfsInputFile;
    }

    /**
     * We're done reading.  Nothing to clean up.
     */
    public void completeLocalInput(File localFile) throws IOException {
        // Ignore the file, it's at the right destination!
    }

    /**
     * Shut down the FS.  Not necessary for regular filesystem.
     */
    public void close() throws IOException {
    }

    /**
     */
    public String toString() {
        return "LocalFS";
    }
    
    /**
     * Implement our own version instead of using the one in FileUtil,
     * to avoid infinite recursion.
     * @param dir
     * @return
     * @throws IOException
     */
    private boolean fullyDelete(File dir) throws IOException {
        File contents[] = dir.listFiles();
        if (contents != null) {
            for (int i = 0; i < contents.length; i++) {
                if (contents[i].isFile()) {
                    if (! contents[i].delete()) {
                        return false;
                    }
                } else {
                    if (! fullyDelete(contents[i])) {
                        return false;
                    }
                }
            }
        }
        return dir.delete();
    }
}
