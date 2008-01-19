/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nutch.indexer;

import java.io.*;
import org.apache.lucene.store.*;
import org.apache.nutch.util.HadoopFSUtil;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.Configuration;

/** Reads a Lucene index stored in DFS. */
public class FsDirectory extends Directory {

  private FileSystem fs;
  private Path directory;
  private int ioFileBufferSize;

  public FsDirectory(FileSystem fs, Path directory, boolean create, Configuration conf)
    throws IOException {

    this.fs = fs;
    this.directory = directory;
    this.ioFileBufferSize = conf.getInt("io.file.buffer.size", 4096);
    
    if (create) {
      create();
    }

    if (!fs.getFileStatus(directory).isDir())
      throw new IOException(directory + " not a directory");
  }

  private void create() throws IOException {
    if (!fs.exists(directory)) {
      fs.mkdirs(directory);
    }

    if (!fs.getFileStatus(directory).isDir())
      throw new IOException(directory + " not a directory");

    // clear old files
    Path[] files = fs.listPaths(directory, HadoopFSUtil.getPassAllFilter());
    for (int i = 0; i < files.length; i++) {
      if (!fs.delete(files[i]))
        throw new IOException("Cannot delete " + files[i]);
    }
  }

  public String[] list() throws IOException {
    Path[] files = fs.listPaths(directory, HadoopFSUtil.getPassAllFilter());
    if (files == null) return null;

    String[] result = new String[files.length];
    for (int i = 0; i < files.length; i++) {
      result[i] = files[i].getName();
    }
    return result;
  }

  public boolean fileExists(String name) throws IOException {
    return fs.exists(new Path(directory, name));
  }

  public long fileModified(String name) {
    throw new UnsupportedOperationException();
  }

  public void touchFile(String name) {
    throw new UnsupportedOperationException();
  }

  public long fileLength(String name) throws IOException {
    return fs.getFileStatus(new Path(directory, name)).getLen();
  }

  public void deleteFile(String name) throws IOException {
    if (!fs.delete(new Path(directory, name)))
      throw new IOException("Cannot delete " + name);
  }

  public void renameFile(String from, String to) throws IOException {
    // DFS is currently broken when target already exists,
    // so we explicitly delete the target first.
    Path target = new Path(directory, to);
    if (fs.exists(target)) {
      fs.delete(target);
    }
    fs.rename(new Path(directory, from), target);
  }

  public IndexOutput createOutput(String name) throws IOException {
    Path file = new Path(directory, name);
    if (fs.exists(file) && !fs.delete(file))      // delete existing, if any
      throw new IOException("Cannot overwrite: " + file);

    return new DfsIndexOutput(file, this.ioFileBufferSize);
  }


  public IndexInput openInput(String name) throws IOException {
    return new DfsIndexInput(new Path(directory, name), this.ioFileBufferSize);
  }

  public Lock makeLock(final String name) {
    return new Lock() {
      public boolean obtain() {
        return true;
      }
      public void release() {
      }
      public boolean isLocked() {
        throw new UnsupportedOperationException();
      }
      public String toString() {
        return "Lock@" + new Path(directory, name);
      }
    };
  }

  public synchronized void close() throws IOException {
    fs.close();
  }

  public String toString() {
    return this.getClass().getName() + "@" + directory;
  }


  private class DfsIndexInput extends BufferedIndexInput {

    /** Shared by clones. */
    private class Descriptor {
      public FSDataInputStream in;
      public long position;                       // cache of in.getPos()
      public Descriptor(Path file, int ioFileBufferSize) throws IOException {
        this.in = fs.open(file);
      }
    }

    private final Descriptor descriptor;
    private final long length;
    private boolean isClone;

    public DfsIndexInput(Path path, int ioFileBufferSize) throws IOException {
      descriptor = new Descriptor(path,ioFileBufferSize);
      length = fs.getFileStatus(path).getLen();
    }

    protected void readInternal(byte[] b, int offset, int len)
      throws IOException {
      synchronized (descriptor) {
        long position = getFilePointer();
        if (position != descriptor.position) {
          descriptor.in.seek(position);
          descriptor.position = position;
        }
        int total = 0;
        do {
          int i = descriptor.in.read(b, offset+total, len-total);
          if (i == -1)
            throw new IOException("read past EOF");
          descriptor.position += i;
          total += i;
        } while (total < len);
      }
    }

    public void close() throws IOException {
      if (!isClone) {
        descriptor.in.close();
      }
    }

    protected void seekInternal(long position) {} // handled in readInternal()

    public long length() {
      return length;
    }

    protected void finalize() throws IOException {
      close();                                      // close the file
    }

    public Object clone() {
      DfsIndexInput clone = (DfsIndexInput)super.clone();
      clone.isClone = true;
      return clone;
    }
  }

  private class DfsIndexOutput extends BufferedIndexOutput {
    private FSDataOutputStream out;

    public DfsIndexOutput(Path path, int ioFileBufferSize) throws IOException {
      out = fs.create(path);
    }

    public void flushBuffer(byte[] b, int offset, int size) throws IOException {
      out.write(b, offset, size);
    }

    public void close() throws IOException {
      super.close();
      out.close();
    }

    public void seek(long pos) throws IOException {
      throw new UnsupportedOperationException();
    }

    public long length() throws IOException {
      return out.getPos();
    }

    protected void finalize() throws IOException {
      out.close();                                // close the file
    }

  }

}
