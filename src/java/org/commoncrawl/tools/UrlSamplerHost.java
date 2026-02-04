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
import java.net.URL;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.commons.lang3.time.StopWatch;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.Generator2;
import org.apache.nutch.crawl.Generator2.DomainScorePair;
import org.apache.nutch.util.NutchConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sample URLs used as Nutch seeds following per-host limits.
 * 
 * Input:
 * <ul>
 * <li>URL and inlink count
 * 
 * <pre>
 * &lt;url&gt; \t &lt;inlink_count&gt;
 * </pre>
 * 
 * </li>
 * <li>host name (leading <code>www.</code> may be stripped), limits and default score
 * 
 * <pre>
 * &lt;host_name&gt; \t &lt;rank&gt; \t &lt;max_urls&gt; \t &lt;default_score&gt;
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
public class UrlSamplerHost extends Configured implements Tool {

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

    private boolean hostStripWWW = false;

    private DomainScorePair outputKey = new DomainScorePair();
    private TextCountPair outputValue = new TextCountPair();

    /**
     * Strip leading <code>www.</code> from host name.
     * 
     * But do not strip if the host name is &quote;www.tld&quote; (e.g.,
     * <code>www.com</code>).
     * 
     * Stripping is required for per-host limit configurations before 2026,
     * based on Common Crawl web graphs where the leading <code>www.</code> was
     * stripped.
     * 
     * @param host
     *          name
     * @return host name with leading www. stripped
     */
    private static String hostStripWWW(String host) {
      // min. length: 4 + 3 + 1 = 8 (www. + 1-letter-domain + .2-letter-tld)
      if (host.length() >= 8 && host.startsWith("www.")
          && host.indexOf('.', 4) != -1) {
        host = host.substring(4);
      }
      return host;
    }

    @Override
    public void setup(Context context) {
      Configuration conf = context.getConfiguration();
      hostStripWWW = conf.getBoolean("urlsample.host.strip.www", false);
    }

    @Override
    public void map(Text key, Text value, Context context)
        throws IOException, InterruptedException {

      if (!URL_PATTERN.matcher(key.toString()).find()) {
        // got <host_name, limits>
        outputKey.set(key, Float.MAX_VALUE);
        outputValue.set(value, -1);
        context.write(outputKey, outputValue);
        return;
      }

      // got <url, sampling_factor>
      String url = key.toString();
      String host;
      try {
        URL u = new URL(url);
        host = u.getHost();
        if (hostStripWWW) {
          host = hostStripWWW(host);
        }
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
      outputKey.set(host, (float) (Math.random() * count));
      outputValue.set(key, count);
      context.write(outputKey, outputValue);
    }
  }

  public static class SampleReducer
      extends Reducer<DomainScorePair, TextCountPair, Text, Text> {

    private int maxUrlsPerHost = -1; // -1 : sample randomly
    private float defaultScore = .001f;
    private Text meta = new Text();

    @Override
    public void setup(Context context) {
      Configuration conf = context.getConfiguration();
      maxUrlsPerHost = conf.getInt("urlsample.urls.per.host", maxUrlsPerHost);
      defaultScore = conf.getFloat("urlsample.default.score", defaultScore);
    }

    @Override
    public void reduce(DomainScorePair key, Iterable<TextCountPair> values,
        Context context) throws IOException, InterruptedException {
      int maxUrls = maxUrlsPerHost;
      float hostScore = defaultScore;
      int nUrls = 0;
      int nUrlsSampled = 0;
      int skippedMaxUrlsPerHost = 0;
      int skippedRandom = 0;
      double sumScores = .0;
      String host = null;
      Text limits = null;

      for (TextCountPair val : values) {
        Text text = val.getText();
        long count = val.getCount().get();
        if (limits == null && count == -1) {
          limits = new Text(text);
          // store limits but parse later lazily (if there are URLs to sample)
          continue;
        } else if (limits != null) {
          String[] l = limits.toString().split("\t");
          if (l.length >= 3) {
            try {
              // long rank = l[0]
              maxUrls = Integer.parseInt(l[1]);
              hostScore = Float.parseFloat(l[2]);
            } catch (NumberFormatException e) {
              LOG.warn("Invalid host limits: {}", limits, e);
            }
          }
          limits = null;
        }
        if (host == null) {
          // processing first URL in values
          host = key.getDomain().toString();
        }
        nUrls++;
        if (nUrlsSampled > maxUrls) {
          if (maxUrls > -1) {
            skippedMaxUrlsPerHost++;
            continue;
          } else {
            // no limits, sample randomly with low probability
            double sampleProb = (.1d / (1 + nUrlsSampled))
                * Math.log10(1 + count);
            if (sampleProb < Math.random()) {
              skippedRandom++;
              continue;
            }
          }
        }
        nUrlsSampled++;
        double score = .001d * hostScore * Math.log10(1 + count);
        sumScores += score;
        meta.set(String.format(Locale.ROOT, "nutch.score=%.12f", score));
        context.write(text, meta);
      }
      // hosts == reduce input groups
      context.getCounter("UrlSamplerHost", "HOSTS").increment(1);
      // URLs == map output records, reduce input records
      context.getCounter("UrlSamplerHost", "URLS").increment(nUrls);
      if (nUrls > 0) {
        if (maxUrls > -1) {
          context.getCounter("UrlSamplerHost", "HOSTS_WITH_LIMIT").increment(1);
          context.getCounter("UrlSamplerHost", "URLS_HOST_WITH_LIMIT")
              .increment(nUrls);
        } else {
          context.getCounter("UrlSamplerHost", "HOSTS_WITHOUT_LIMIT")
              .increment(1);
          context.getCounter("UrlSamplerHost", "URLS_HOST_WITHOUT_LIMIT")
              .increment(nUrls);
        }
        if (nUrlsSampled > 0) {
          context.getCounter("UrlSamplerHost", "URLS_SAMPLED")
              .increment(nUrlsSampled);
          context.getCounter("UrlSamplerHost", "HOSTS_SAMPLED").increment(1);
          if (maxUrls > -1) {
            context.getCounter("UrlSamplerHost", "HOSTS_WITH_LIMIT_SAMPLED")
                .increment(1);
            context.getCounter("UrlSamplerHost", "URLS_HOST_WITH_LIMIT_SAMPLED")
                .increment(nUrlsSampled);
          } else {
            context.getCounter("UrlSamplerHost", "HOSTS_WITHOUT_LIMIT_SAMPLED")
                .increment(1);
            context
                .getCounter("UrlSamplerHost", "URLS_HOST_WITHOUT_LIMIT_SAMPLED")
                .increment(nUrlsSampled);
          }
        }
        context.getCounter("UrlSamplerHost", "SKIPPED_MAX_URLS_PER_HOST")
            .increment(skippedMaxUrlsPerHost);
        context.getCounter("UrlSamplerHost", "SKIPPED_RANDOM")
            .increment(skippedRandom);
        LOG.info(
            "Sampled for host {} : {} URLs ({} skipped: {} max. per host, {} random), sum of scores = {}",
            host, nUrlsSampled, (nUrls - nUrlsSampled), skippedMaxUrlsPerHost,
            skippedRandom, sumScores);
      }
    }
  }

  private void sample(Path[] inputs, Path output) throws Exception {

    StopWatch stopWatch = new StopWatch();
    stopWatch.start();
    LOG.info("UrlSamplerHost: starting");

    Configuration conf = getConf();
    conf.setInt("partition.url.seed", new Random().nextInt());

    Job job = Job.getInstance(conf, UrlSamplerHost.class.getName());
    job.setJarByClass(UrlSamplerHost.class);
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

    stopWatch.stop();
    LOG.info("UrlSamplerHost: finished, elapsed: {} ms",
        stopWatch.getTime(TimeUnit.MILLISECONDS));
  }

  public void usage() {
    System.err
      .println("Usage: UrlSamplerHost [-D...] <host_limits> <input_dir>... <output_dir>\n");
    System.err.println(
        "\nThe host_limits file defines the maximum number of URLs to sample per host.");
    System.err.println("\nProperties:");
    System.err.println(
        "\t-Durlsample.host.strip.www=(true|false)\tstrip leading www. from host names");
    System.err.println(
        "\t\t\t(depending on whether the limits file uses stripped host names)");
    System.err.println("Properties to configure defaults, if host is not in the limits file:");
    System.err.println(
        "\t-Durlsample.urls.per.host\tmax. number of URLs to sample per host");
    System.err.println(
        "\t\t\t-1 : sample randomly with low probability (default)");
    System.err.println(
        "\t-Durlsample.default.score\tdefault score for sampled URLs (default: 0.001)");
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
      LOG.error("UrlSamplerHost: ", e);
      return -1;
    }
    return 0;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(), new UrlSamplerHost(),
        args);
    System.exit(res);
  }

}
