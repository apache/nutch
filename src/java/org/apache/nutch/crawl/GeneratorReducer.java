package org.apache.nutch.crawl;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.crawl.GeneratorJob.SelectorEntry;
import org.apache.nutch.fetcher.FetcherJob.FetcherMapper;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.TableUtil;
import org.apache.nutch.util.URLUtil;
import org.gora.mapreduce.GoraReducer;

/** Reduce class for generate
 *
 * The #reduce() method write a random integer to all generated URLs. This random
 * number is then used by {@link FetcherMapper}.
 *
 */
public class GeneratorReducer
extends GoraReducer<SelectorEntry, WebPage, String, WebPage> {

  private long limit;
  private long maxCount;
  private long count = 0;
  private boolean byDomain = false;
  private Map<String, Integer> hostCountMap = new HashMap<String, Integer>();
  private Utf8 crawlId;

  @Override
  protected void reduce(SelectorEntry key, Iterable<WebPage> values,
      Context context) throws IOException, InterruptedException {
    for (WebPage page : values) {
      if (maxCount > 0) {
        String hostordomain;
        if (byDomain) {
          hostordomain = URLUtil.getDomainName(key.url);
        } else {
          hostordomain = URLUtil.getHost(key.url);
        }

        Integer hostCount = hostCountMap.get(hostordomain);
        if (hostCount == null) {
          hostCountMap.put(hostordomain, 0);
          hostCount = 0;
        }
        if (hostCount >= maxCount) {
          return;
        }
        hostCountMap.put(hostordomain, hostCount + 1);
      }
      if (count >= limit) {
        return;
      }

      Mark.GENERATE_MARK.putMark(page, crawlId);
      context.write(TableUtil.reverseUrl(key.url), page);
      context.getCounter("Generator", "GENERATE_MARK").increment(1);
      count++;
    }
  }

  @Override
  protected void setup(Context context)
      throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    long totalLimit = conf.getLong(GeneratorJob.GENERATOR_TOP_N, Long.MAX_VALUE);
    if (totalLimit == Long.MAX_VALUE) {
      limit = Long.MAX_VALUE;
    } else {
      limit = totalLimit / context.getNumReduceTasks();
    }
    maxCount = conf.getLong(GeneratorJob.GENERATOR_MAX_COUNT, -2);
    crawlId = new Utf8(conf.get(GeneratorJob.CRAWL_ID));
    String countMode =
      conf.get(GeneratorJob.GENERATOR_COUNT_MODE, GeneratorJob.GENERATOR_COUNT_VALUE_HOST);
    if (countMode.equals(GeneratorJob.GENERATOR_COUNT_VALUE_DOMAIN)) {
      byDomain = true;
    }

  }

}