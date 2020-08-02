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

package org.commoncrawl.tools;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.Generator2;
import org.apache.nutch.crawl.Generator2.DomainScorePair;
import org.apache.nutch.crawl.URLPartitioner;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.TimingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sample URLs used as Nutch seeds following per-domain limits.
 * 
 * Input:
 * <ul>
 * <li>URL and count of inlinks used to calculate seeds
 * 
 * <pre>
 * &lt;url&gt; \t &lt;inlink_count&gt;
 * </pre>
 * 
 * </li>
 * <li>domain name, limits and default score
 * 
 * <pre>
 * &lt;domain_name&gt; \t &lt;rank&gt; \t &lt;max_urls&gt; \t &lt;max_hosts&gt; \t  &lt;max_urls_per_host&gt; \t &lt;default_score&gt;
 * </pre>
 * 
 * </li>
 * 
 * Output:
 * 
 * <pre>
 * &lt;url&gt; \t nutch.score=&lt;score&gt;
 * </pre>
 * 
 * The score is calculated as:
 * 
 * <pre>
 * 0.001 * default_score * log10(1 + inlink_count)
 * </pre>
 */
public class UrlSampler extends Configured implements Tool {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  private static final Pattern URL_PATTERN = Pattern.compile("^https?://",
      Pattern.CASE_INSENSITIVE);

  public static class TextCountPair implements Writable {

    private Text text = new Text();
    private LongWritable count = new LongWritable();

    public void set(Text text, long count) {
      this.text.set(text);
      this.count.set(count);
    }

    public Text getText() {
      return text;
    }

    public LongWritable getCount() {
      return count;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      text.readFields(in);
      count.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
      text.write(out);
      count.write(out);
    }

  }

  public static class SampleMapper
      extends Mapper<Text, Text, DomainScorePair, TextCountPair> {

    private DomainScorePair outputKey = new DomainScorePair();
    private TextCountPair outputValue = new TextCountPair();

    @Override
    public void map(Text key, Text value, Context context)
        throws IOException, InterruptedException {

      if (!URL_PATTERN.matcher(key.toString()).find()) {
        // got <domain_name, limits>
        outputKey.set(key, Float.MAX_VALUE);
        outputValue.set(value, -1);
        context.write(outputKey, outputValue);
        return;
      }

      // got <url, sampling_factor>
      String url = key.toString();
      String domain;
      try {
        URL u = new URL(url);
        domain = URLPartitioner.getDomainName(u.getHost());
      } catch (Exception e) {
        LOG.warn("Malformed URL: '{}', skipping ({})", url, e.getMessage());
        context.getCounter("UrlSampler", "MALFORMED_URL").increment(1);
        return;
      }

      long count = 0;
      try {
        count = Long.parseLong(value.toString());
      } catch (NumberFormatException e) {
        LOG.error("Value is not a long integer (url: {} value: {})", url,
            value);
      }
      outputKey.set(domain, (float) (Math.random() * 1 + count));
      outputValue.set(key, count);
      context.write(outputKey, outputValue);
    }
  }

  public static class SampleReducer
      extends Reducer<DomainScorePair, TextCountPair, Text, Text> {

    private int maxUrlsPerDomain = 40;
    private int maxHostsPerDomain = 2;
    private int maxUrlsPerHost = 20;
    private float defaultScore = .001f;
    private Text meta = new Text();

    @Override
    public void setup(Context context) {
      Configuration conf = context.getConfiguration();
      maxUrlsPerDomain = conf.getInt("urlsample.urls.per.domain",
          maxUrlsPerDomain);
      maxUrlsPerHost = conf.getInt("urlsample.urls.per.host", maxUrlsPerHost);
      maxHostsPerDomain = conf.getInt("urlsample.hosts.per.domain",
          maxHostsPerDomain);
      defaultScore = conf.getFloat("urlsample.default.score", defaultScore);
    }

    @Override
    public void reduce(DomainScorePair key, Iterable<TextCountPair> values,
        Context context) throws IOException, InterruptedException {
      int maxUrls = maxUrlsPerDomain;
      int maxHosts = maxHostsPerDomain;
      int maxPerHost = maxUrlsPerHost;
      float domainScore = defaultScore;
      int nUrls = 0;
      int nUrlsSampled = 0;
      int skippedMaxUrls = 0;
      int skippedMaxHosts = 0;
      int skippedMaxUrlsPerHost = 0;
      double sumScores = .0;
      String domain = null;
      Text limits = null;
      Map<String, int[]> hosts = null;

      for (TextCountPair val : values) {
        Text text = val.getText();
        long count = val.getCount().get();
        if (limits == null && count == -1) {
          limits = new Text(text);
          // store limits but parse later lazily (if there are URLs to sample)
          continue;
        } else if (limits != null) {
          String[] l = limits.toString().split("\t");
          if (l.length >= 5) {
            try {
              // long rank = l[0]
              maxUrls = Integer.parseInt(l[1]);
              maxHosts = Integer.parseInt(l[2]);
              maxPerHost = Integer.parseInt(l[3]);
              domainScore = Float.parseFloat(l[4]);
            } catch (NumberFormatException e) {
              LOG.warn("Invalid domain limits: {}", limits, e);
            }
          }
          limits = null;
        }
        if (domain == null) {
          // processing first URL in values
          domain = key.getDomain().toString();
          hosts = new HashMap<>();
        }
        String host = null;
        try {
          host = new URL(text.toString()).getHost().toLowerCase(Locale.ROOT);
          if (host.endsWith(domain)) {
            // clip common domain name suffix to save storage space in map keys
            host = host.substring(0, host.length() - domain.length());
          } else {
            LOG.warn("Host {} does not have domain {} as suffix!", host,
                domain);
          }
        } catch (MalformedURLException e) {
          context.getCounter("UrlSampler", "MALFORMED_URL").increment(1);
          continue;
        }
        nUrls++;
        if (nUrlsSampled > maxUrls) {
          skippedMaxUrls++;
          continue;
        }
        int[] nUrlsPerHost = hosts.get(host);
        if (nUrlsPerHost != null) {
          if (nUrlsPerHost[0]++ > maxPerHost) {
            skippedMaxUrlsPerHost++;
            continue;
          }
        } else {
          if (hosts.size() >= maxHosts) {
            skippedMaxHosts++;
            continue;
          }
          hosts.put(host, new int[] { 0 });
        }
        nUrlsSampled++;
        double score = .001d * domainScore * Math.log10(1 + count);
        sumScores += score;
        meta.set(String.format(Locale.ROOT, "nutch.score=%.12f", score));
        context.write(text, meta);
      }
      if (nUrls == 0)
        return;
      context.getCounter("UrlSampler", "SKIPPED_MAX_URLS")
          .increment(skippedMaxUrls);
      context.getCounter("UrlSampler", "SKIPPED_MAX_URLS_PER_HOST")
          .increment(skippedMaxUrlsPerHost);
      context.getCounter("UrlSampler", "SKIPPED_MAX_HOSTS")
          .increment(skippedMaxHosts);
      LOG.info(
          "Sampled for domain {} : {} hosts, {} URLs ({} skipped: {} max. URLs, {} max. per host, {} max. hosts), sum of scores = {}",
          domain, hosts.size(), nUrlsSampled, (nUrls - nUrlsSampled),
          skippedMaxUrls, skippedMaxUrlsPerHost, skippedMaxHosts, sumScores);
    }
  }

  private void sample(Path[] inputs, Path output) throws Exception {

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    long start = System.currentTimeMillis();
    LOG.info("UrlSampler: starting at {}", sdf.format(start));

    Configuration conf = getConf();
    conf.setInt("partition.url.seed", new Random().nextInt());

    Job job = Job.getInstance(conf, UrlSampler.class.getName());
    job.setJarByClass(UrlSampler.class);
    job.setMapperClass(SampleMapper.class);
    job.setPartitionerClass(Generator2.Selector.class);
    job.setSortComparatorClass(Generator2.ScoreComparator.class);
    job.setGroupingComparatorClass(Generator2.DomainComparator.class);
    job.setReducerClass(SampleReducer.class);
    job.setInputFormatClass(KeyValueTextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setMapOutputKeyClass(Generator2.DomainScorePair.class);
    job.setMapOutputValueClass(TextCountPair.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    for (Path input : inputs) {
      KeyValueTextInputFormat.addInputPath(job, input);
    }
    FileOutputFormat.setOutputPath(job, output);

    try {
      // run the job
      boolean success = job.waitForCompletion(true);
      if (!success) {
        String message = "UrlSampler job did not succeed, job status: "
            + job.getStatus().getState() + ", reason: "
            + job.getStatus().getFailureInfo();
        LOG.error(message);
        // throw exception so that calling routine can exit with error
        throw new RuntimeException(message);
      }

    } catch (IOException | InterruptedException | ClassNotFoundException
        | NullPointerException e) {
      LOG.error("UrlSampler job failed: {}", e.getMessage());
      throw e;
    }

    long end = System.currentTimeMillis();
    LOG.info("UrlSampler: finished at {}, elapsed: {}", sdf.format(end),
        TimingUtil.elapsedTime(start, end));
  }

  public void usage() {
    System.err
        .println("Usage: UrlSampler [-D...] <input_dir>... <output_dir>\n");
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      usage();
      return -1;
    }

    Path[] inputs = new Path[args.length - 1];
    for (int i = 0; i < (args.length - 1); i++) {
      inputs[i] = new Path(args[i]);
    }
    Path output = new Path(args[args.length - 1]);

    try {
      sample(inputs, output);
    } catch (Exception e) {
      LOG.error("UrlSampler: " + StringUtils.stringifyException(e));
      return -1;
    }
    return 0;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(), new UrlSampler(),
        args);
    System.exit(res);
  }

}
