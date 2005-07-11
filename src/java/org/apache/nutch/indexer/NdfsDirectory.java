/**
 * Copyright 2004 The Apache Software Foundation
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

package org.apache.nutch.indexer;

import java.io.*;
import org.apache.lucene.store.*;
import org.apache.nutch.fs.*;

/** Reads a Lucene index stored in NDFS. */
public class NdfsDirectory extends Directory {

  private NutchFileSystem fs;
  private File directory;

  public NdfsDirectory(NutchFileSystem fs, File directory, boolean create)
    throws IOException {

    this.fs = fs;
    this.directory = directory;

    if (create) {
      create();
    }

    if (!fs.isDirectory(directory))
      throw new IOException(directory + " not a directory");
  }

  private void create() throws IOException {
    if (!fs.exists(directory)) {
      fs.mkdirs(directory);
    }

    if (!fs.isDirectory(directory))
      throw new IOException(directory + " not a directory");

    // clear old files
    File[] files = fs.listFiles(directory);
    for (int i = 0; i < files.length; i++) {
      if (!files[i].delete())
        throw new IOException("Cannot delete " + files[i]);
    }
  }

  public String[] list() throws IOException {
    File[] files = fs.listFiles(directory);
    if (files == null) return null;

    String[] result = new String[files.length];
    for (int i = 0; i < files.length; i++) {
      result[i] = files[i].getName();
    }
    return result;
  }

  public boolean fileExists(String name) throws IOException {
    return fs.exists(new File(directory, name));
  }

  public long fileModified(String name) {
    throw new UnsupportedOperationException();
  }

  public void touchFile(String name) {
    throw new UnsupportedOperationException();
  }

  public long fileLength(String name) throws IOException {
    return fs.getLength(new File(directory, name));
  }

  public void deleteFile(String name) throws IOException {
    if (!fs.delete(new File(directory, name)))
      throw new IOException("Cannot delete " + name);
  }

  public void renameFile(String from, String to) throws IOException {
    fs.rename(new File(directory, from), new File(directory, to));
  }

  public IndexOutput createOutput(String name) throws IOException {
    File file = new File(directory, name);
    if (fs.exists(file) && !fs.delete(file))      // delete existing, if any
      throw new IOException("Cannot overwrite: " + file);

    return new NdfsIndexOutput(file);
  }


  public IndexInput openInput(String name) throws IOException {
    return new NdfsIndexInput(new File(directory, name));
  }

  public Lock makeLock(final String name) {
    return new Lock() {
      public boolean obtain() {
        try {
          fs.lock(new File(directory, name), false);
          return true;
        } catch (IOException e) {
          return false;
        }
      }
      public void release() {
        try {
          fs.release(new File(directory, name));
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
      public boolean isLocked() {
        throw new UnsupportedOperationException();
      }
      public String toString() {
        return "Lock@" + new File(directory, name);
      }
    };
  }

  public synchronized void close() throws IOException {
    fs.close();
  }

  public String toString() {
    return this.getClass().getName() + "@" + directory;
  }


  private class NdfsIndexInput extends BufferedIndexInput {

    /** Shared by clones. */
    private class Descriptor {
      public NFSInputStream in;
      public long position;                       // cache of in.getPos()
      public Descriptor(File file) throws IOException {
        this.in = fs.open(file);
      }
    }

    private final Descriptor descriptor;
    private final long length;
    private boolean isClone;

    public NdfsIndexInput(File path) throws IOException {
      descriptor = new Descriptor(path);
      length = fs.getLength(path);
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
      NdfsIndexInput clone = (NdfsIndexInput)super.clone();
      clone.isClone = true;
      return clone;
    }
  }

  private class NdfsIndexOutput extends BufferedIndexOutput {
    private NFSOutputStream out;

    public NdfsIndexOutput(File path) throws IOException {
      out = fs.create(path);
    }

    public void flushBuffer(byte[] b, int size) throws IOException {
      out.write(b, 0, size);
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
