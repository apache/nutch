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

package org.apache.nutch.io;

import java.io.*;
import java.util.*;
import junit.framework.TestCase;
import java.util.logging.*;

import org.apache.nutch.fs.*;
import org.apache.nutch.util.*;

/** Support for flat files of binary key/value pairs. */
public class TestSequenceFile extends TestCase {
  private static Logger LOG = SequenceFile.LOG;

  private static NutchConf nutchConf = new NutchConf();
  
  public TestSequenceFile(String name) { super(name); }

  /** Unit tests for SequenceFile. */
  public void testSequenceFile() throws Exception {
    int count = 1024 * 10;
    int megabytes = 1;
    int factor = 5;
    String file = System.getProperty("test.build.data",".") + "/test.seq";
 
    int seed = new Random().nextInt();

    NutchFileSystem nfs = new LocalFileSystem(new NutchConf());
    try {
        //LOG.setLevel(Level.FINE);
        writeTest(nfs, count, seed, file, false);
        readTest(nfs, count, seed, file);

        sortTest(nfs, count, megabytes, factor, false, file);
        checkSort(nfs, count, seed, file);

        sortTest(nfs, count, megabytes, factor, true, file);
        checkSort(nfs, count, seed, file);

        mergeTest(nfs, count, seed, file, false, factor, megabytes);
        checkSort(nfs, count, seed, file);

        mergeTest(nfs, count, seed, file, true, factor, megabytes);
        checkSort(nfs, count, seed, file);
    } finally {
        nfs.close();
    }
  }

  private static void writeTest(NutchFileSystem nfs, int count, int seed,
                                String file, boolean compress)
    throws IOException {
    new File(file).delete();
    LOG.fine("creating with " + count + " records");
    SequenceFile.Writer writer =
      new SequenceFile.Writer(nfs, file, RandomDatum.class, RandomDatum.class,
                              compress);
    RandomDatum.Generator generator = new RandomDatum.Generator(seed);
    for (int i = 0; i < count; i++) {
      generator.next();
      RandomDatum key = generator.getKey();
      RandomDatum value = generator.getValue();

      writer.append(key, value);
    }
    writer.close();
  }

  private static void readTest(NutchFileSystem nfs, int count, int seed, String file)
    throws IOException {
    RandomDatum k = new RandomDatum();
    RandomDatum v = new RandomDatum();
    LOG.fine("reading " + count + " records");
    SequenceFile.Reader reader = new SequenceFile.Reader(nfs, file, nutchConf);
    RandomDatum.Generator generator = new RandomDatum.Generator(seed);
    for (int i = 0; i < count; i++) {
      generator.next();
      RandomDatum key = generator.getKey();
      RandomDatum value = generator.getValue();
      
      reader.next(k, v);
      
      if (!k.equals(key))
        throw new RuntimeException("wrong key at " + i);
      if (!v.equals(value))
        throw new RuntimeException("wrong value at " + i);
    }
    reader.close();
  }


  private static void sortTest(NutchFileSystem nfs, int count, int megabytes, 
                               int factor, boolean fast, String file)
    throws IOException {
    new File(file+".sorted").delete();
    SequenceFile.Sorter sorter = newSorter(nfs, fast, megabytes, factor);
    LOG.fine("sorting " + count + " records");
    sorter.sort(file, file+".sorted");
    LOG.fine("done sorting " + count + " records");
  }

  private static void checkSort(NutchFileSystem nfs, int count, int seed, String file)
    throws IOException {
    LOG.fine("sorting " + count + " records in memory for check");
    RandomDatum.Generator generator = new RandomDatum.Generator(seed);
    SortedMap map = new TreeMap();
    for (int i = 0; i < count; i++) {
      generator.next();
      RandomDatum key = generator.getKey();
      RandomDatum value = generator.getValue();
      map.put(key, value);
    }

    LOG.fine("checking order of " + count + " records");
    RandomDatum k = new RandomDatum();
    RandomDatum v = new RandomDatum();
    Iterator iterator = map.entrySet().iterator();
    SequenceFile.Reader reader = new SequenceFile.Reader(nfs, file + ".sorted", nutchConf);
    for (int i = 0; i < count; i++) {
      Map.Entry entry = (Map.Entry)iterator.next();
      RandomDatum key = (RandomDatum)entry.getKey();
      RandomDatum value = (RandomDatum)entry.getValue();

      reader.next(k, v);

      if (!k.equals(key))
        throw new RuntimeException("wrong key at " + i);
      if (!v.equals(value))
        throw new RuntimeException("wrong value at " + i);
    }

    reader.close();
    LOG.fine("sucessfully checked " + count + " records");
  }

  private static void mergeTest(NutchFileSystem nfs, int count, int seed, 
                                String file, boolean fast, int factor, 
                                int megabytes)
    throws IOException {

    LOG.fine("creating "+factor+" files with "+count/factor+" records");

    SequenceFile.Writer[] writers = new SequenceFile.Writer[factor];
    String[] names = new String[factor];
    String[] sortedNames = new String[factor];
    
    for (int i = 0; i < factor; i++) {
      names[i] = file+"."+i;
      sortedNames[i] = names[i] + ".sorted";
      nfs.delete(new File(names[i]));
      nfs.delete(new File(sortedNames[i]));
      writers[i] =
        new SequenceFile.Writer(nfs, names[i], RandomDatum.class,RandomDatum.class);
    }

    RandomDatum.Generator generator = new RandomDatum.Generator(seed);

    for (int i = 0; i < count; i++) {
      generator.next();
      RandomDatum key = generator.getKey();
      RandomDatum value = generator.getValue();

      writers[i%factor].append(key, value);
    }

    for (int i = 0; i < factor; i++)
      writers[i].close();

    for (int i = 0; i < factor; i++) {
      LOG.fine("sorting file " + i + " with " + count/factor + " records");
      newSorter(nfs, fast, megabytes, factor).sort(names[i], sortedNames[i]);
    }

    LOG.fine("merging " + factor + " files with " + count/factor + " records");
    nfs.delete(new File(file+".sorted"));
    newSorter(nfs, fast, megabytes, factor).merge(sortedNames, file+".sorted");
  }

  private static SequenceFile.Sorter newSorter(NutchFileSystem nfs, 
                                               boolean fast,
                                               int megabytes, int factor) {
    SequenceFile.Sorter sorter = 
      fast
      ? new SequenceFile.Sorter(nfs, new RandomDatum.Comparator(),RandomDatum.class, nutchConf)
      : new SequenceFile.Sorter(nfs, RandomDatum.class, RandomDatum.class, nutchConf);
    sorter.setMemory(megabytes * 1024*1024);
    sorter.setFactor(factor);
    return sorter;
  }


  /** For debugging and testing. */
  public static void main(String[] args) throws Exception {
    int count = 1024 * 1024;
    int megabytes = 1;
    int factor = 10;
    boolean create = true;
    boolean check = false;
    boolean fast = false;
    boolean merge = false;
    boolean compress = false;
    String file = null;
    String usage = "Usage: SequenceFile (-local | -ndfs <namenode:port>) [-count N] [-megabytes M] [-factor F] [-nocreate] [-check] [-fast] [-merge] [-compress] file";
    
    if (args.length == 0) {
        System.err.println(usage);
        System.exit(-1);
    }
    int i = 0;
    NutchFileSystem nfs = NutchFileSystem.parseArgs(args, i, nutchConf);      
    try {
      for (; i < args.length; i++) {       // parse command line
          if (args[i] == null) {
              continue;
          } else if (args[i].equals("-count")) {
              count = Integer.parseInt(args[++i]);
          } else if (args[i].equals("-megabytes")) {
              megabytes = Integer.parseInt(args[++i]);
          } else if (args[i].equals("-factor")) {
              factor = Integer.parseInt(args[++i]);
          } else if (args[i].equals("-nocreate")) {
              create = false;
          } else if (args[i].equals("-check")) {
              check = true;
          } else if (args[i].equals("-fast")) {
              fast = true;
          } else if (args[i].equals("-merge")) {
              merge = true;
          } else if (args[i].equals("-compress")) {
              compress = true;
          } else {
              // file is required parameter
              file = args[i];
          }
        }
        LOG.info("count = " + count);
        LOG.info("megabytes = " + megabytes);
        LOG.info("factor = " + factor);
        LOG.info("create = " + create);
        LOG.info("check = " + check);
        LOG.info("fast = " + fast);
        LOG.info("merge = " + merge);
        LOG.info("compress = " + compress);
        LOG.info("file = " + file);

        int seed = 0;
 
        LOG.setLevel(Level.FINE);

        if (create && !merge) {
            writeTest(nfs, count, seed, file, compress);
            readTest(nfs, count, seed, file);
        }

        if (merge) {
            mergeTest(nfs, count, seed, file, fast, factor, megabytes);
        } else {
            sortTest(nfs, count, megabytes, factor, fast, file);
        }
    
        if (check) {
            checkSort(nfs, count, seed, file);
        }
      } finally {
          nfs.close();
      }
  }
}
