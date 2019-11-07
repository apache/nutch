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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.net.URLFilterException;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.util.NutchConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import crawlercommons.domains.EffectiveTldFinder;

public class UrlCleaner extends Configured implements Tool {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  public static enum OutputKeyType {
    URL,
    HOST,
    DOMAIN,
    HOST_REVERSED,
    DOMAIN_REVERSED,
    HOST_REVERSED_TRAILING_DOT,
    DOMAIN_REVERSED_TRAILING_DOT;

    public static OutputKeyType get(String name) {
      for (OutputKeyType t : OutputKeyType.values()) {
        if (name.equalsIgnoreCase(t.toString())) {
          return t;
        }
      }
      return URL;
    }
  }

  private static final String CHECK_DOMAIN = "urlcleaner.check.domain";
  private static final String OUTPUT_TYPE = "urlcleaner.output.key.type";

  private Configuration config;


  public static class UrlCleanerMapper extends Mapper<Text, Text, Text, Text> {

    public static final String URL_NORMALIZING_SCOPE = "crawldb.url.normalizers.scope";

    private static Pattern SPLIT_HOST_PATTERN = Pattern.compile("\\.");

    private URLNormalizers urlNormalizers;
    private URLFilters filters;
    private String scope;
    private boolean checkDomain;
    private boolean needDomain;
    private boolean needHost;
    private OutputKeyType outputType;

    @Override
    public void setup(Context context) {
      Configuration conf = context.getConfiguration();
      scope = conf.get(URL_NORMALIZING_SCOPE, URLNormalizers.SCOPE_INJECT);
      urlNormalizers = new URLNormalizers(conf, scope);
      filters = new URLFilters(conf);
      checkDomain = conf.getBoolean(CHECK_DOMAIN, false);
      outputType = OutputKeyType.get(conf.get(OUTPUT_TYPE));
      LOG.info("check domain names: {}", checkDomain);
      LOG.info("output type: {}", outputType);
      needDomain = checkDomain || outputType == OutputKeyType.DOMAIN
          || outputType == OutputKeyType.DOMAIN_REVERSED
          || outputType == OutputKeyType.DOMAIN_REVERSED_TRAILING_DOT;
      needHost = needDomain || outputType == OutputKeyType.HOST
          || outputType == OutputKeyType.HOST_REVERSED
          || outputType == OutputKeyType.HOST_REVERSED_TRAILING_DOT;
    }

    public static String[] reverseHost(String hostName) {
      String[] rev = SPLIT_HOST_PATTERN.split(hostName);
      for (int i = 0; i < (rev.length/2); i++) {
        String temp = rev[i];
        rev[i] = rev[rev.length - i - 1];
        rev[rev.length - i - 1] = temp;
      }
      return rev;
    }

    @Override
    public void map(Text key, Text value, Context context)
        throws IOException, InterruptedException {

      String urlOrig = key.toString();
      String url = urlOrig.trim();

      try {
        url = urlNormalizers.normalize(url, scope);
      } catch (MalformedURLException e) {
        context.getCounter("urlcleaner", "urls_rejected").increment(1);
        return;        
      }
      try {
        url = filters.filter(url);
      } catch (URLFilterException e) {
        context.getCounter("urlcleaner", "urls_rejected").increment(1);
        return;
      }

      if (url == null) {
        context.getCounter("urlcleaner", "urls_rejected").increment(1);
        return;
      }

      String host = null, domain = null;
      if (needHost) {
        try {
          URL u = new URL(url);
          host = u.getHost();
          if (needDomain) {
            domain = EffectiveTldFinder.getAssignedDomain(host, true, true);
            if (checkDomain && domain == null) {
              context.getCounter("urlcleaner", "urls_rejected_invalid_domain")
                  .increment(1);
              return;
            }
          }
        } catch (MalformedURLException e) {
          context.getCounter("urlcleaner", "urls_rejected").increment(1);
          return;
        }
      }

      if (url.equals(urlOrig)) {
        context.getCounter("urlcleaner", "urls_accepted_unchanged").increment(1);
      } else {
        context.getCounter("urlcleaner", "urls_accepted_normalized").increment(1);
        key.set(url);
      }

      String keyVal = null;
      String addVal = url;
      switch (outputType) {
      case HOST:
        keyVal = (host != null) ? host : "";
        break;
      case DOMAIN:
        keyVal = (domain != null) ? domain : "";
        addVal = host + "\t" + url;
        break;
      case HOST_REVERSED:
        keyVal = (host != null) ? host : "";
        keyVal = String.join(".", reverseHost(keyVal));
        break;
      case DOMAIN_REVERSED:
        keyVal = (domain != null) ? domain : "";
        keyVal = String.join(".", reverseHost(keyVal));
        addVal = String.join(".", reverseHost(host)) + "\t" + url;
        break;
      case HOST_REVERSED_TRAILING_DOT:
        keyVal = (host != null) ? host : "";
        List<String> parts = new ArrayList<>();
        parts.addAll(Arrays.asList(reverseHost(keyVal)));
        parts.add("");
        keyVal = String.join(".", parts);
        break;
      case DOMAIN_REVERSED_TRAILING_DOT:
        keyVal = (domain != null) ? domain : "";
        parts = new ArrayList<>();
        parts.addAll(Arrays.asList(reverseHost(keyVal)));
        parts.add("");
        keyVal = String.join(".", parts);
        parts = new ArrayList<>();
        parts.addAll(Arrays.asList(reverseHost(host)));
        parts.add("");
        addVal = String.join(".", parts) + "\t" + url;
        break;
      default:
        break;
      }

      if (outputType != OutputKeyType.URL) {
        key.set(keyVal);
        value.set(addVal + "\t" + value.toString());
      }

      context.write(key, value);
    }

  }

  @Override
  public Configuration getConf() {
    return config;
  }

  @Override
  public void setConf(Configuration conf) {
    config = conf;
  }

  public void clean(Path input, Path output, boolean checkDomain,
      OutputKeyType outputType) throws Exception {

    Configuration conf = getConf();
    conf.setBoolean(CHECK_DOMAIN, checkDomain);
    conf.set(OUTPUT_TYPE, outputType.toString());

    Job job = Job.getInstance(conf, UrlCleaner.class.getName());
    job.setJarByClass(UrlCleaner.class);
    job.setMapperClass(UrlCleanerMapper.class);
    job.setReducerClass(Reducer.class);
    job.setInputFormatClass(KeyValueTextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    KeyValueTextInputFormat.addInputPath(job, input);
    FileOutputFormat.setOutputPath(job, output);

    try {
      // run the job
      boolean success = job.waitForCompletion(true);
      if (!success) {
        String message = "UrlCleaner job did not succeed, job status: "
            + job.getStatus().getState() + ", reason: "
            + job.getStatus().getFailureInfo();
        LOG.error(message);
        // throw exception so that calling routine can exit with error
        throw new RuntimeException(message);
      }

    } catch (IOException | InterruptedException | ClassNotFoundException | NullPointerException e) {
      LOG.error("UrlCleaner job failed: {}", e.getMessage());
      throw e;
    }

  }

  public void usage() {
    System.err.println(
        "Usage: UrlCleaner [-D...] [-checkDomain] [-outputKey <...>] <url_dir> <output_dir>\n");
  }

  @Override
  public int run(String[] args) throws Exception {

    boolean checkDomain = false;
    OutputKeyType outputType = OutputKeyType.URL;

    int i = 0;
    for (; i < (args.length - 2); i++) {
      if (args[i].equals("-checkDomain")) {
        checkDomain = true;
      } else if (args[i].equals("-outputKey")) {
        String key = args[++i];
        outputType = OutputKeyType.get(key);
      } else {
        LOG.info("Injector: Found invalid argument \"" + args[i] + "\"\n");
        usage();
        return -1;
      }
    }

    if ((args.length - i) < 2) {
      usage();
      return -1;
    }

    Path input = new Path(args[0+i]);
    Path output = new Path(args[1+i]);

    try {
      clean(input, output, checkDomain, outputType);
    } catch (Exception e) {
      LOG.error("UrlCleaner: " + StringUtils.stringifyException(e));
      return -1;
    }
    return 0;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(), new UrlCleaner(),
        args);
    System.exit(res);
  }

}
