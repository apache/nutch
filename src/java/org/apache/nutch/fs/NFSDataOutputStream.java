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
import java.util.zip.Checksum;
import java.util.zip.CRC32;
import org.apache.nutch.util.NutchConf;

/** Utility that wraps a {@link NFSOutputStream} in a {@link DataOutputStream},
 * buffers output through a {@link BufferedOutputStream} and creates a checksum
 * file. */
public class NFSDataOutputStream extends DataOutputStream {
  public static final byte[] CHECKSUM_VERSION = new byte[] {'c', 'r', 'c', 0};
  
  /** Store checksums for data. */
  private static class Summer extends FilterOutputStream {

    private NFSDataOutputStream sums;
    private Checksum sum = new CRC32();
    private int inSum;
    private int bytesPerSum;

    public Summer(NutchFileSystem fs, File file, boolean overwrite, NutchConf nutchConf)
      throws IOException {
      super(fs.createRaw(file, overwrite));
      this.bytesPerSum = nutchConf.getInt("io.bytes.per.checksum", 512);
      this.sums =
        new NFSDataOutputStream(fs.createRaw(fs.getChecksumFile(file), true), nutchConf);

      sums.write(CHECKSUM_VERSION, 0, CHECKSUM_VERSION.length);
      sums.writeInt(this.bytesPerSum);
    }

    public void write(byte b[], int off, int len) throws IOException {
      int summed = 0;
      while (summed < len) {

        int goal = this.bytesPerSum - inSum;
        int inBuf = len - summed;
        int toSum = inBuf <= goal ? inBuf : goal;

        sum.update(b, off+summed, toSum);
        summed += toSum;

        inSum += toSum;
        if (inSum == this.bytesPerSum) {
          writeSum();
        }
      }

      out.write(b, off, len);
    }

    private void writeSum() throws IOException {
      if (inSum != 0) {
        sums.writeInt((int)sum.getValue());
        sum.reset();
        inSum = 0;
      }
    }

    public void close() throws IOException {
      writeSum();
      sums.close();
      super.close();
    }

  }

  private static class PositionCache extends FilterOutputStream {
    long position;

    public PositionCache(OutputStream out) throws IOException {
      super(out);
    }

    // This is the only write() method called by BufferedOutputStream, so we
    // trap calls to it in order to cache the position.
    public void write(byte b[], int off, int len) throws IOException {
      out.write(b, off, len);
      position += len;                            // update position
    }
      
    public long getPos() throws IOException {
      return position;                            // return cached position
    }
    
  }

  private static class Buffer extends BufferedOutputStream {
    public Buffer(OutputStream out, int bufferSize) throws IOException {
      super(out, bufferSize);
    }

    public long getPos() throws IOException {
      return ((PositionCache)out).getPos() + this.count;
    }

    // optimized version of write(int)
    public void write(int b) throws IOException {
      if (count >= buf.length) {
        super.write(b);
      } else {
        buf[count++] = (byte)b;
      }
    }

  }

  public NFSDataOutputStream(NutchFileSystem fs, File file,
                             boolean overwrite, NutchConf nutchConf)
    throws IOException {
    super(new Buffer(new PositionCache(new Summer(fs, file, overwrite, nutchConf)),
            nutchConf.getInt("io.file.buffer.size", 4096)));
  }

  /** Construct without checksums. */
  public NFSDataOutputStream(NFSOutputStream out, NutchConf nutchConf) throws IOException {
    this(out, nutchConf.getInt("io.file.buffer.size", 4096));
  }

  /** Construct without checksums. */
  public NFSDataOutputStream(NFSOutputStream out, int bufferSize)
    throws IOException {
    super(new Buffer(new PositionCache(out), bufferSize));
  }

  public long getPos() throws IOException {
    return ((Buffer)out).getPos();
  }

}
