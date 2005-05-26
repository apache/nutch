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
package org.apache.nutch.mapred.demo;

import org.apache.nutch.mapred.JobConf;
import org.apache.nutch.mapred.JobClient;
import org.apache.nutch.mapred.RunningJob;
import org.apache.nutch.mapred.SequenceFileOutputFormat;
import org.apache.nutch.mapred.SequenceFileInputFormat;

import org.apache.nutch.mapred.lib.RegexMapper;
import org.apache.nutch.mapred.lib.InverseMapper;
import org.apache.nutch.mapred.lib.LongSumReducer;
import org.apache.nutch.mapred.lib.IdentityReducer;

import org.apache.nutch.io.UTF8;
import org.apache.nutch.io.LongWritable;
import org.apache.nutch.io.WritableComparator;

import org.apache.nutch.util.NutchConf;

import java.io.File;
import java.util.Random;

/* Extracts matching regexs from input files and counts them. */
public class Grep {
  private Grep() {}                               // singleton

  public static void main(String[] args) throws Exception {
    if (args.length < 3) {
      System.out.println("Grep <inDir> <outDir> <regex> [<group>]");
      System.exit(-1);
    }

    NutchConf defaults = NutchConf.get();

    File tempDir =
      new File("grep-temp-"+
               Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

    JobConf grepJob = new JobConf(defaults);

    grepJob.setNumMapTasks(18);
    grepJob.setInputDir(new File(args[0]));

    grepJob.setMapperClass(RegexMapper.class);
    grepJob.set("mapred.mapper.regex", args[2]);
    if (args.length == 4)
      grepJob.set("mapred.mapper.regex.group", args[3]);
    
    grepJob.setCombinerClass(LongSumReducer.class);
    grepJob.setReducerClass(LongSumReducer.class);

    grepJob.setNumReduceTasks(6);
    grepJob.setOutputDir(tempDir);
    grepJob.setOutputFormat(SequenceFileOutputFormat.class);
    grepJob.setOutputKeyClass(UTF8.class);
    grepJob.setOutputValueClass(LongWritable.class);

    JobClient.runJob(grepJob);

    JobConf sortJob = new JobConf(defaults);

    sortJob.setNumMapTasks(6);

    sortJob.setInputDir(tempDir);
    sortJob.setInputFormat(SequenceFileInputFormat.class);
    sortJob.setInputKeyClass(UTF8.class);
    sortJob.setInputValueClass(LongWritable.class);

    sortJob.setMapperClass(InverseMapper.class);

    sortJob.setNumReduceTasks(1);                 // write a single file
    sortJob.setOutputDir(new File(args[1]));
    sortJob.setOutputKeyComparatorClass           // sort by decreasing freq
      (LongWritable.DecreasingComparator.class);

    JobClient.runJob(sortJob);

    new JobClient(defaults).getFs().delete(tempDir);
  }

}
