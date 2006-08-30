/**
 * Copyright 2006 The Apache Software Foundation
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
package org.apache.nutch.crawl;

import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.StringUtils;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.ToolBase;

/**
 * This tool merges several LinkDb-s into one, optionally filtering
 * URLs through the current URLFilters, to skip prohibited URLs and
 * links.
 * 
 * <p>It's possible to use this tool just for filtering - in that case
 * only one LinkDb should be specified in arguments.</p>
 * <p>If more than one LinkDb contains information about the same URL,
 * all inlinks are accumulated, but only at most <code>db.max.inlinks</code>
 * inlinks will ever be added.</p>
 * <p>If activated, URLFilters will be applied to both the target URLs and
 * to any incoming link URL. If a target URL is prohibited, all
 * inlinks to that target will be removed, including the target URL. If
 * some of incoming links are prohibited, only they will be removed, and they
 * won't count when checking the above-mentioned maximum limit.
 * 
 * @author Andrzej Bialecki
 */
public class LinkDbMerger extends ToolBase {
  public LinkDbMerger() {
    
  }
  
  public LinkDbMerger(Configuration conf) {
    setConf(conf);
  }

  public void merge(Path output, Path[] dbs, boolean normalize, boolean filter) throws Exception {
    JobConf job = LinkDb.createMergeJob(getConf(), output, normalize, filter);
    for (int i = 0; i < dbs.length; i++) {
      job.addInputPath(new Path(dbs[i], LinkDb.CURRENT_NAME));      
    }
    JobClient.runJob(job);
    FileSystem fs = FileSystem.get(getConf());
    fs.mkdirs(output);
    fs.rename(job.getOutputPath(), new Path(output, LinkDb.CURRENT_NAME));
  }
  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
    int res = new LinkDbMerger().doMain(NutchConfiguration.create(), args);
    System.exit(res);
  }
  
  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: LinkDbMerger <output_linkdb> <linkdb1> [<linkdb2> <linkdb3> ...] [-normalize] [-filter]");
      System.err.println("\toutput_linkdb\toutput LinkDb");
      System.err.println("\tlinkdb1 ...\tinput LinkDb-s (single input LinkDb is ok)");
      System.err.println("\t-normalize\tuse URLNormalizer on both fromUrls and toUrls in linkdb(s) (usually not needed)");
      System.err.println("\t-filter\tuse URLFilters on both fromUrls and toUrls in linkdb(s)");
      return -1;
    }
    Path output = new Path(args[0]);
    ArrayList dbs = new ArrayList();
    boolean normalize = false;
    boolean filter = false;
    for (int i = 1; i < args.length; i++) {
      if (args[i].equals("-filter")) {
        filter = true;
      } else if (args[i].equals("-normalize")) {
        normalize = true;
      } else dbs.add(new Path(args[i]));
    }
    try {
      merge(output, (Path[])dbs.toArray(new Path[dbs.size()]), normalize, filter);
      return 0;
    } catch (Exception e) {
      LOG.fatal("LinkDbMerger: " + StringUtils.stringifyException(e));
      return -1;
    }
  }

}
