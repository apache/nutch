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
import java.util.logging.*;

import org.apache.nutch.ndfs.*;
import org.apache.nutch.util.*;

/****************************************************************
 * NutchFileSystem is an interface for a fairly simple
 * distributed file system.  A Nutch installation might consist
 * of multiple machines, which should swap files transparently.
 * This interface allows other Nutch systems to find and place
 * files into the distributed Nutch-controlled file world.
 *
 * The standard job of NutchFileSystem is to take the location-
 * independent NutchFile objects, and resolve them using local
 * knowledge and local instances of ShareGroup.
 * 
 * @author Mike Cafarella
 *****************************************************************/
public abstract class NutchFileSystem {
    public static final Logger LOG = LogFormatter.getLogger("org.apache.nutch.util.NutchFileSystem");

    private static final HashMap NAME_TO_FS = new HashMap();
  
    /**
     * Parse the cmd-line args, starting at i.  Remove consumed args
     * from array.  We expect param in the form:
     * '-local | -ndfs <namenode:port>'
     *
     * @deprecated use fs.default.name config option instead
     */
    public static NutchFileSystem parseArgs(String argv[], int i) throws IOException {
        /**
        if (argv.length - i < 1) {
            throw new IOException("Must indicate filesystem type for NDFS");
        }
        */
        int orig = i;
        NutchFileSystem nfs = null;
        String cmd = argv[i];
        if ("-ndfs".equals(cmd)) {
            i++;
            InetSocketAddress addr = DataNode.createSocketAddr(argv[i++]);
            nfs = new NDFSFileSystem(addr);
        } else if ("-local".equals(cmd)) {
            i++;
            nfs = new LocalFileSystem();
        } else {
            nfs = get();                          // using default
            LOG.info("No FS indicated, using default:"+nfs.getName());

        }
        System.arraycopy(argv, i, argv, orig, argv.length - i);
        for (int j = argv.length - i; j < argv.length; j++) {
            argv[j] = null;
        }
        return nfs;
    }


    /** Returns the default filesystem implementation.*/
    public static NutchFileSystem get() throws IOException {
      return get(NutchConf.get());
    }

    /** Returns the configured filesystem implementation.*/
    public static NutchFileSystem get(NutchConf conf) throws IOException {
      return getNamed(conf.get("fs.default.name", "local"));
    }

    /** Returns a name for this filesystem, suitable to pass to {@link
     * NutchFileSystem#getNamed(String).*/
    public abstract String getName();
  
    /** Returns a named filesystem.  Names are either the string "local" or a
     * host:port pair, naming an NDFS name server.*/
    public static NutchFileSystem getNamed(String name) throws IOException {
      NutchFileSystem fs = (NutchFileSystem)NAME_TO_FS.get(name);
      if (fs == null) {
        if ("local".equals(name)) {
          fs = new LocalFileSystem();
        } else {
          fs = new NDFSFileSystem(DataNode.createSocketAddr(name));
        }
        NAME_TO_FS.put(name, fs);
      }
      return fs;
    }

    /** Return the name of the checksum file associated with a file.*/
    public static File getChecksumFile(File file) {
      return new File(file.getParentFile(), "."+file.getName()+".crc");
    }

    /** Return true iff file is a checksum file name.*/
    public static boolean isChecksumFile(File file) {
      String name = file.getName();
      return name.startsWith(".") && name.endsWith(".crc");
    }

    ///////////////////////////////////////////////////////////////
    // NutchFileSystem
    ///////////////////////////////////////////////////////////////
    /**
     */
    public NutchFileSystem() {
    }

    /**
     * Opens an NFSDataInputStream for the indicated File.
     */
    public NFSDataInputStream open(File f) throws IOException {
      return open(f, NutchConf.get().getInt("io.file.buffer.size", 4096));
    }

    /**
     * Opens an NFSDataInputStream at the indicated File.
     * @param f the file name to open
     * @param overwrite if a file with this name already exists, then if true,
     *   the file will be overwritten, and if false an error will be thrown.
     * @param bufferSize the size of the buffer to be used.
     */
    public NFSDataInputStream open(File f, int bufferSize) throws IOException {
      return new NFSDataInputStream(this, f, bufferSize);
    }

    /**
     * Opens an InputStream for the indicated File, whether local
     * or via NDFS.
     */
    public abstract NFSInputStream openRaw(File f) throws IOException;

    /**
     * Opens an NFSDataOutputStream at the indicated File.
     * Files are overwritten by default.
     */
    public NFSDataOutputStream create(File f) throws IOException {
      return create(f, true,
                    NutchConf.get().getInt("io.file.buffer.size", 4096));
    }

    /**
     * Opens an NFSDataOutputStream at the indicated File.
     * @param f the file name to open
     * @param overwrite if a file with this name already exists, then if true,
     *   the file will be overwritten, and if false an error will be thrown.
     * @param bufferSize the size of the buffer to be used.
     */
    public NFSDataOutputStream create(File f, boolean overwrite,
                                      int bufferSize) throws IOException {
      return new NFSDataOutputStream(this, f, overwrite, bufferSize);
    }

    /** Opens an OutputStream at the indicated File.
     * @param f the file name to open
     * @param overwrite if a file with this name already exists, then if true,
     *   the file will be overwritten, and if false an error will be thrown.
     */
    public abstract NFSOutputStream createRaw(File f, boolean overwrite)
      throws IOException;

    /**
     * Creates the given File as a brand-new zero-length file.  If
     * create fails, or if it already existed, return false.
     */
    public boolean createNewFile(File f) throws IOException {
        if (exists(f)) {
            return false;
        } else {
            OutputStream out = createRaw(f, false);
            try {
            } finally {
              out.close();
            }
            return true;
        }
    }

    /**
     * Renames File src to File dst.  Can take place on local fs
     * or remote NDFS.
     */
    public boolean rename(File src, File dst) throws IOException {
      if (isDirectory(src)) {
        return renameRaw(src, dst);
      } else {

        boolean value = renameRaw(src, dst);

        File checkFile = getChecksumFile(src);
        if (exists(checkFile))
          renameRaw(checkFile, getChecksumFile(dst)); // try to rename checksum

        return value;
      }
      
    }

    /**
     * Renames File src to File dst.  Can take place on local fs
     * or remote NDFS.
     */
    public abstract boolean renameRaw(File src, File dst) throws IOException;

    /**
     * Deletes File
     */
    public boolean delete(File f) throws IOException {
      if (isDirectory(f)) {
        return deleteRaw(f);
      } else {
        deleteRaw(getChecksumFile(f));            // try to delete checksum
        return deleteRaw(f);
      }
    }

    /**
     * Deletes File
     */
    public abstract boolean deleteRaw(File f) throws IOException;

    /**
     * Check if exists
     */
    public abstract boolean exists(File f) throws IOException;

    public abstract boolean isDirectory(File f) throws IOException;

    public boolean isFile(File f) throws IOException {
        if (exists(f) && ! isDirectory(f)) {
            return true;
        } else {
            return false;
        }
    }
    
    public abstract long getLength(File f) throws IOException;

    public File[] listFiles(File f) throws IOException {
      return listFiles(f, new FileFilter() {
          public boolean accept(File file) {
            return !isChecksumFile(file);
          }
        });
    }

    public abstract File[] listFilesRaw(File f) throws IOException;

    public File[] listFiles(File f, FileFilter filter) throws IOException {
        Vector results = new Vector();
        File listing[] = listFilesRaw(f);
        if (listing != null) {
          for (int i = 0; i < listing.length; i++) {
            if (filter.accept(listing[i])) {
              results.add(listing[i]);
            }
          }
        }
        return (File[]) results.toArray(new File[results.size()]);
    }

    /**
     * Make the given file and all non-existent parents into
     * directories.
     */
    public abstract void mkdirs(File f) throws IOException;

    /**
     * Obtain a lock on the given File
     */
    public abstract void lock(File f, boolean shared) throws IOException;

    /**
     * Release the lock
     */
    public abstract void release(File f) throws IOException;

    /**
     * The src file is on the local disk.  Add it to NFS at
     * the given dst name and the source is kept intact afterwards
     */
    // not implemneted yet
    public abstract void copyFromLocalFile(File src, File dst) throws IOException;

    /**
     * The src file is on the local disk.  Add it to NFS at
     * the given dst name, removing the source afterwards.
     */
    public abstract void moveFromLocalFile(File src, File dst) throws IOException;

    /**
     * The src file is under NFS2, and the dst is on the local disk.
     * Copy it from NFS control to the local dst name.
     */
    public abstract void copyToLocalFile(File src, File dst) throws IOException;

    /**
     * the same as copyToLocalFile(File src, File dst), except that
     * the source is removed afterward.
     */
    // not implemented yet
    //public abstract void moveToLocalFile(File src, File dst) throws IOException;

    /**
     * Returns a local File that the user can write output to.  The caller
     * provides both the eventual NFS target name and the local working
     * file.  If the NFS is local, we write directly into the target.  If
     * the NFS is remote, we write into the tmp local area.
     */
    public abstract File startLocalOutput(File nfsOutputFile, File tmpLocalFile) throws IOException;

    /**
     * Called when we're all done writing to the target.  A local NFS will
     * do nothing, because we've written to exactly the right place.  A remote
     * NFS will copy the contents of tmpLocalFile to the correct target at
     * nfsOutputFile.
     */
    public abstract void completeLocalOutput(File nfsOutputFile, File tmpLocalFile) throws IOException;

    /**
     * Returns a local File that the user can read from.  The caller 
     * provides both the eventual NFS target name and the local working
     * file.  If the NFS is local, we read directly from the source.  If
     * the NFS is remote, we write data into the tmp local area.
     */
    public abstract File startLocalInput(File nfsInputFile, File tmpLocalFile) throws IOException;

    /**
     * Called when we're all done writing to the target.  A local NFS will
     * do nothing, because we've written to exactly the right place.  A remote
     * NFS will copy the contents of tmpLocalFile to the correct target at
     * nfsOutputFile.
     */
    public abstract void completeLocalInput(File localFile) throws IOException;

    /**
     * No more filesystem operations are needed.  Will
     * release any held locks.
     */
    public abstract void close() throws IOException;

    /**
     * Report a checksum error to the file system.
     * @param f the file name containing the error
     * @param in the stream open on the file
     * @param start the position of the beginning of the bad data in the file
     * @param length the length of the bad data in the file
     * @param crc the expected CRC32 of the data
     */
    public abstract void reportChecksumFailure(File f, NFSInputStream in,
                                               long start, long length,
                                               int crc);

}
