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
import junit.framework.TestCase;
import java.util.logging.*;

import org.apache.nutch.fs.*;
import org.apache.nutch.mapred.*;
import org.apache.nutch.mapred.lib.*;
import org.apache.nutch.io.*;
import org.apache.nutch.util.*;

public class TestNutchFileSystem extends TestCase {
  private static final Logger LOG = InputFormatBase.LOG;

  private static final long MEGA = 1024 * 1024;

  private static String ROOT = System.getProperty("test.build.data",".");
  private static File CONTROL_DIR = new File(ROOT, "fs_control");
  private static File WRITE_DIR = new File(ROOT, "fs_write");
  private static File READ_DIR = new File(ROOT, "fs_read");
  private static File DATA_DIR = new File(ROOT, "fs_data");

  public void testFs() throws Exception {
    testFs(10 * MEGA, 100, 0);
  }

  public static void testFs(long megaBytes, int numFiles, long seed)
    throws Exception {

    NutchFileSystem fs = NutchFileSystem.get();

    if (seed == 0)
      seed = new Random().nextLong();

    LOG.info("seed = "+seed);

    createControlFile(fs, megaBytes, numFiles, seed);
    writeTest(fs);
    readTest(fs);
  }

  public static void createControlFile(NutchFileSystem fs,
                                       long megaBytes, int numFiles,
                                       long seed) throws Exception {

    LOG.info("creating control file: "+megaBytes+" bytes, "+numFiles+" files");

    File controlFile = new File(CONTROL_DIR, "files");
    fs.delete(controlFile);
    Random random = new Random(seed);

    SequenceFile.Writer writer =
      new SequenceFile.Writer(fs, controlFile.toString(),
                              UTF8.class, LongWritable.class);

    long totalSize = 0;
    long maxSize = ((megaBytes / numFiles) * 2) + 1;
    try {
      while (totalSize < megaBytes) {
        UTF8 name = new UTF8(Long.toString(random.nextLong()));

        long size = random.nextLong();
        if (size < 0)
          size = -size;
        size = size % maxSize;

        //LOG.info(" adding: name="+name+" size="+size);

        writer.append(name, new LongWritable(size));

        totalSize += size;
      }
    } finally {
      writer.close();
    }
    LOG.info("created control file for: "+totalSize+" bytes");
  }

  public static class WriteMapper extends NutchConfigured implements Mapper {
    private Random random = new Random();
    private byte[] buffer = new byte[8192];
    private NutchFileSystem fs;

    {
      try {
        fs = NutchFileSystem.get();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    public WriteMapper() { super(null); }
    
    public WriteMapper(NutchConf conf) { super(conf); }

    public void configure(JobConf job) {
      setConf(job);
    }

    public void map(WritableComparable key, Writable value,
                    OutputCollector collector) throws IOException {
      String name = ((UTF8)key).toString();
      long size = ((LongWritable)value).get();
      long seed = Long.parseLong(name);

      random.setSeed(seed);
      //LOG.info("writing: name="+name+" size="+size);

      OutputStream out = fs.create(new File(DATA_DIR, name));

      long written = 0;
      try {
        while (written < size) {
          random.nextBytes(buffer);
          long remains = size - written;
          int length = (remains<=buffer.length) ? (int)remains : buffer.length;
          out.write(buffer, 0, length);
          written += length;
        }
      } finally {
        out.close();
      }

      collector.collect(new UTF8("bytes"), new LongWritable(written));
    }
  }

  public static void writeTest(NutchFileSystem fs)
    throws Exception {

    fs.delete(WRITE_DIR);

    JobConf job = new JobConf(NutchConf.get());

    job.setInputDir(CONTROL_DIR);
    job.setInputFormat(SequenceFileInputFormat.class);
    job.setInputKeyClass(UTF8.class);
    job.setInputValueClass(LongWritable.class);

    job.setMapperClass(WriteMapper.class);
    job.setReducerClass(LongSumReducer.class);

    job.setOutputDir(WRITE_DIR);
    job.setOutputKeyClass(UTF8.class);
    job.setOutputValueClass(LongWritable.class);
    job.setNumReduceTasks(1);
    JobClient.runJob(job);
  }

  public static class ReadMapper extends NutchConfigured implements Mapper {
    private Random random = new Random();
    private byte[] buffer = new byte[8192];
    private byte[] check  = new byte[8192];
    private NutchFileSystem fs;

    {
      try {
        fs = NutchFileSystem.get();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    public ReadMapper() { super(null); }
    
    public ReadMapper(NutchConf conf) { super(conf); }

    public void configure(JobConf job) {
      setConf(job);
    }

    public void map(WritableComparable key, Writable value,
                    OutputCollector collector) throws IOException {
      String name = ((UTF8)key).toString();
      long size = ((LongWritable)value).get();
      long seed = Long.parseLong(name);

      random.setSeed(seed);
      //LOG.info("reading: name="+name+" size="+size);

      InputStream in = fs.open(new File(DATA_DIR, name));

      long read = 0;
      try {
        while (read < size) {
          long remains = size - read;
          int req = (remains<=buffer.length) ? (int)remains : buffer.length;
          int got = in.read(buffer, 0, req);
          read += got;
          assertEquals(got, req);
          random.nextBytes(check);
          if (got != buffer.length) {
            Arrays.fill(buffer, got, buffer.length, (byte)0);
            Arrays.fill(check, got, check.length, (byte)0);
          }
          assertTrue(Arrays.equals(buffer, check));
        }
      } finally {
        in.close();
      }

      collector.collect(new UTF8("bytes"), new LongWritable(read));
    }
  }

  public static void readTest(NutchFileSystem fs)
    throws Exception {

    fs.delete(READ_DIR);

    JobConf job = new JobConf(NutchConf.get());

    job.setInputDir(CONTROL_DIR);
    job.setInputFormat(SequenceFileInputFormat.class);
    job.setInputKeyClass(UTF8.class);
    job.setInputValueClass(LongWritable.class);

    job.setMapperClass(ReadMapper.class);
    job.setReducerClass(LongSumReducer.class);

    job.setOutputDir(READ_DIR);
    job.setOutputKeyClass(UTF8.class);
    job.setOutputValueClass(LongWritable.class);
    job.setNumReduceTasks(1);
    JobClient.runJob(job);
  }


  public static void main(String[] args) throws Exception {
    int megaBytes = 10;
    int files = 100;
    boolean check = true;

    String usage = "Usage: TestNutchFileSystem -files N -megaBytes M";
    
    if (args.length == 0) {
        System.err.println(usage);
        System.exit(-1);
    }
    for (int i = 0; i < args.length; i++) {       // parse command line
      if (args[i].equals("-files")) {
        files = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-megaBytes")) {
        megaBytes = Integer.parseInt(args[++i]);
      }
    }
    LOG.info("files = " + files);
    LOG.info("megaBytes = " + megaBytes);
    LOG.info("check = " + check);
  
    testFs(megaBytes * MEGA, files, 0);
  }
}
