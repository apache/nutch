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

package org.apache.nutch.mapred.lib;

import java.io.IOException;

import org.apache.nutch.mapred.Mapper;
import org.apache.nutch.mapred.OutputCollector;
import org.apache.nutch.mapred.JobConf;
import org.apache.nutch.mapred.Reporter;

import org.apache.nutch.io.WritableComparable;
import org.apache.nutch.io.Writable;
import org.apache.nutch.io.LongWritable;
import org.apache.nutch.io.UTF8;


import java.util.regex.Pattern;
import java.util.regex.Matcher;


/** A {@link Mapper} that extracts text matching a regular expression. */
public class RegexMapper implements Mapper {

  private Pattern pattern;
  private int group;

  public void configure(JobConf job) {
    pattern = Pattern.compile(job.get("mapred.mapper.regex"));
    group = job.getInt("mapred.mapper.regex.group", 0);
  }

  public void map(WritableComparable key, Writable value,
                  OutputCollector output, Reporter reporter)
    throws IOException {
    String text = ((UTF8)value).toString();
    Matcher matcher = pattern.matcher(text);
    while (matcher.find()) {
      output.collect(new UTF8(matcher.group(group)), new LongWritable(1));
    }
  }
}
