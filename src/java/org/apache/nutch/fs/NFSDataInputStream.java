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
import org.apache.nutch.util.NutchConf;

/** Utility that wraps a {@link NFSInputStream} in a {@link DataInputStream}
 * and buffers input through a {@link BufferedInputStream}. */
public class NFSDataInputStream extends DataInputStream {
  
  /** Cache the file position.  This improves performance significantly.*/
  private static class PositionCache extends FilterInputStream {
    long position;

    public PositionCache(NFSInputStream in) throws IOException {
      super(in);
      this.position = in.getPos();
    }

    // This is the only read() method called by BufferedInputStream, so we trap
    // calls to it in order to cache the position.
    public int read(byte b[], int off, int len) throws IOException {
      int result = in.read(b, off, len);
      position += result;
      return result;
    }

    public void seek(long desired) throws IOException {
      ((NFSInputStream)in).seek(desired);         // seek underlying stream
      position = desired;                         // update position
    }
      
    public long getPos() throws IOException {
      return position;                            // return cached position
    }
    
  }

  /** Buffer input.  This improves performance significantly.*/
  private static class Buffer extends BufferedInputStream {
    public Buffer(PositionCache in, int bufferSize) throws IOException {
      super(in, bufferSize);
    }

    public void seek(long desired) throws IOException {
      long current = getPos();
      long start = (current - this.pos);
      if (desired >= start && desired < start + this.count) {
        this.pos += (desired - current);          // can position within buffer
      } else {
        this.count = 0;                           // invalidate buffer
        this.pos = 0;

        ((PositionCache)in).seek(desired);        // seek underlying stream
      }
    }
      
    public long getPos() throws IOException { // adjust for buffer
      return ((PositionCache)in).getPos() - (this.count - this.pos);
    }

    // optimized version of read()
    public int read() throws IOException {
      if (pos >= count)
        return super.read();
      return buf[pos++] & 0xff;
    }

}

  public NFSDataInputStream(NFSInputStream in) throws IOException {
    this(in, NutchConf.get().getInt("io.file.buffer.size", 4096));
  }

  public NFSDataInputStream(NFSInputStream in, int bufferSize)
    throws IOException {
    super(new Buffer(new PositionCache(in), bufferSize));
  }
    
  public void seek(long desired) throws IOException {
    ((Buffer)in).seek(desired);
  }

  public long getPos() throws IOException {
    return ((Buffer)in).getPos();
  }

}
