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
package org.apache.nutch.tools.arc;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

/**
 * <p>The <code>ArchRecordReader</code> class provides a record reader which 
 * reads records from arc files.</p>
 * 
 * <p>Arc files are essentially tars of gzips.  Each record in an arc file is
 * a compressed gzip.  Multiple records are concatenated together to form a
 * complete arc.  For more information on the arc file format see
 * {@link http://www.archive.org/web/researcher/ArcFileFormat.php}.</p>
 * 
 * <p>Arc files are used by the internet archive and grub projects.</p>
 * 
 * @see http://www.archive.org/
 * @see http://www.grub.org/
 */
public class ArcRecordReader
  implements RecordReader {

  public static final Log LOG = LogFactory.getLog(ArcRecordReader.class);

  protected Configuration conf;
  protected long splitStart = 0;
  protected long pos = 0;
  protected long splitEnd = 0;
  protected long splitLen = 0;
  protected long fileLen = 0;
  protected FSDataInputStream in;

  private static byte[] MAGIC = {(byte)0x1F, (byte)0x8B};

  /**
   * <p>Returns true if the byte array passed matches the gzip header magic 
   * number.</p>
   * 
   * @param input The byte array to check.
   * 
   * @return True if the byte array matches the gzip header magic number.
   */
  public static boolean isMagic(byte[] input) {

	// check for null and incorrect length
    if (input == null || input.length != MAGIC.length) {
      return false;
    }
    
    // check byte by byte
    for (int i = 0; i < MAGIC.length; i++) {
      if (MAGIC[i] != input[i]) {
        return false;
      }
    }
    
    // must match
    return true;
  }

  /**
   * Constructor that sets the configuration and file split.
   * 
   * @param conf The job configuration.
   * @param split The file split to read from.
   * 
   * @throws IOException  If an IO error occurs while initializing file split.
   */
  public ArcRecordReader(Configuration conf, FileSplit split)
    throws IOException {

    Path path = split.getPath();
    FileSystem fs = path.getFileSystem(conf);
    fileLen = fs.getFileStatus(split.getPath()).getLen();
    this.conf = conf;
    this.in = fs.open(split.getPath());
    this.splitStart = split.getStart();
    this.splitEnd = splitStart + split.getLength();
    this.splitLen = split.getLength();
    in.seek(splitStart);
  }

  /**
   * Closes the record reader resources.
   */
  public void close()
    throws IOException {
    this.in.close();
  }

  /**
   * Creates a new instance of the <code>Text</code> object for the key.
   */
  public WritableComparable createKey() {
    return (WritableComparable)ReflectionUtils.newInstance(Text.class, conf);
  }

  /**
   * Creates a new instance of the <code>BytesWritable</code> object for the key
   */
  public Writable createValue() {
    return (Writable)ReflectionUtils.newInstance(BytesWritable.class, conf);
  }

  /**
   * Returns the current position in the file.
   * 
   * @return The long of the current position in the file.
   */
  public long getPos()
    throws IOException {
    return in.getPos();
  }

  /**
   * Returns the percentage of progress in processing the file.  This will be
   * represented as a float from 0 to 1 with 1 being 100% completed.
   * 
   * @return The percentage of progress as a float from 0 to 1.
   */
  public float getProgress()
    throws IOException {
	  
    // if we haven't even started
    if (splitEnd == splitStart) {
      return 0.0f;
    }
    else {
      // the progress is current pos - where we started  / length of the split
      return Math.min(1.0f, (getPos() - splitStart) / (float)splitLen);
    }
  }

  /**
   * <p>Returns true if the next record in the split is read into the key and 
   * value pair.  The key will be the arc record header and the values will be
   * the raw content bytes of the arc record.</p>
   * 
   * @param key The record key
   * @param value The record value
   * 
   * @return True if the next record is read.
   * 
   * @throws IOException If an error occurs while reading the record value.
   */
  public boolean next(WritableComparable key, Writable value)
    throws IOException {

    try {
      
      // get the starting position on the input stream
      long startRead = in.getPos();
      byte[] magicBuffer = null;
      
      // we need this loop to handle false positives in reading of gzip records
      while (true) {
        
        // while we haven't passed the end of the split
        if (startRead >= splitEnd) {
          return false;
        }
        
        // scanning for the gzip header
        boolean foundStart = false;
        while (!foundStart) {
          
          // start at the current file position and scan for 1K at time, break
          // if there is no more to read
          startRead = in.getPos();
          magicBuffer = new byte[1024];
          int read = in.read(magicBuffer);
          if (read < 0) {
            break;
          }
          
          // scan the byte array for the gzip header magic number.  This happens
          // byte by byte
          for (int i = 0; i < read - 1; i++) {
            byte[] testMagic = new byte[2];
            System.arraycopy(magicBuffer, i, testMagic, 0, 2);            
            if (isMagic(testMagic)) {              
              // set the next start to the current gzip header
              startRead += i;
              foundStart = true;
              break;
            }
          }
        }
        
        // seek to the start of the gzip header
        in.seek(startRead);
        ByteArrayOutputStream baos = null;
        int totalRead = 0;

        try {
          
          // read 4K of the gzip at a time putting into a byte array
          byte[] buffer = new byte[4096];
          GZIPInputStream zin = new GZIPInputStream(in);
          int gzipRead = -1;
          baos = new ByteArrayOutputStream();
          while ((gzipRead = zin.read(buffer, 0, buffer.length)) != -1) {
            baos.write(buffer, 0, gzipRead);
            totalRead += gzipRead;
          }
        }
        catch (Exception e) {
          
          // there are times we get false positives where the gzip header exists
          // but it is not an actual gzip record, so we ignore it and start
          // over seeking
          System.out.println("Ignoring position: " + (startRead));
          if (startRead + 1 < fileLen) {
            in.seek(startRead + 1);
          }
          continue;
        }

        // change the output stream to a byte array
        byte[] content = baos.toByteArray();
        
        // the first line of the raw content in arc files is the header
        int eol = 0;
        for (int i = 0; i < content.length; i++) {
          if (i > 0 && content[i] == '\n') {
            eol = i;
            break;
          }
        }
        
        // create the header and the raw content minus the header
        String header = new String(content, 0, eol).trim();
        byte[] raw = new byte[(content.length - eol) - 1];
        System.arraycopy(content, eol + 1, raw, 0, raw.length);
        
        // populate key and values with the header and raw content.
        Text keyText = (Text)key;
        keyText.set(header);
        BytesWritable valueBytes = (BytesWritable)value;
        valueBytes.set(raw, 0, raw.length);

        // TODO: It would be best to start at the end of the gzip read but 
        // the bytes read in gzip don't match raw bytes in the file so we 
        // overshoot the next header.  With this current method you get
        // some false positives but don't miss records.
        if (startRead + 1 < fileLen) {
          in.seek(startRead + 1);
        }
        
        // populated the record, now return
        return true;
      }
    }
    catch (Exception e) {
      LOG.equals(StringUtils.stringifyException(e));      
    }
    
    // couldn't populate the record or there is no next record to read
    return false;
  }
}
