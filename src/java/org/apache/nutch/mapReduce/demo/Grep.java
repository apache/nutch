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
package org.apache.nutch.mapReduce.demo;

import org.apache.nutch.mapReduce.JobConf;
import org.apache.nutch.mapReduce.JobClient;
import org.apache.nutch.mapReduce.RunningJob;

import org.apache.nutch.mapReduce.lib.RegexMapper;
import org.apache.nutch.mapReduce.lib.LongSumReducer;

import org.apache.nutch.io.UTF8;
import org.apache.nutch.io.LongWritable;

import java.io.File;

/* Extracts matching regexs from input files and counts them. */
public class Grep {
  private Grep() {}                               // singleton
  
  public static void main(String[] args) throws Exception {
    if (args.length < 3) {
      System.out.println("Grep <inDir> <outDir> <regex> [<group>]");
      System.exit(-1);
    }

    JobConf job = new JobConf();

    job.setInputDir(new File(args[0]));
    job.setOutputDir(new File(args[1]));

    job.setMapperClass(RegexMapper.class);
    job.set("mapred.mapper.regex", args[2]);
    if (args.length == 4)
      job.set("mapred.mapper.regex.group", args[3]);
    
    job.setCombinerClass(LongSumReducer.class);
    job.setReducerClass(LongSumReducer.class);

    job.setOutputKeyClass(UTF8.class);
    job.setOutputValueClass(LongWritable.class);

    JobClient.runJob(job);

  }

}
