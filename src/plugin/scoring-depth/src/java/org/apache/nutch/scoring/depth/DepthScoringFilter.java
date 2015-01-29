package org.apache.nutch.scoring.depth;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.scoring.ScoringFilter;
import org.apache.nutch.scoring.ScoringFilterException;

/**
 * This scoring filter limits the number of hops from the initial seed urls. If
 * the number of hops exceeds the depth (either the default value, or the one
 * set in the injector file) then all outlinks from that url are discarded,
 * effectively stopping further crawling along this path.
 */
public class DepthScoringFilter extends Configured implements ScoringFilter {
  private static final Log LOG = LogFactory.getLog(DepthScoringFilter.class);

  public static final String DEPTH_KEY = "_depth_";
  public static final Text DEPTH_KEY_W = new Text(DEPTH_KEY);
  public static final String MAX_DEPTH_KEY = "_maxdepth_";
  public static final Text MAX_DEPTH_KEY_W = new Text(MAX_DEPTH_KEY);

  // maximum value that we are never likely to reach
  // because the depth of the Web graph is that high only
  // for spam cliques.
  public static final int DEFAULT_MAX_DEPTH = 1000;

  private int defaultMaxDepth;

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    if (conf == null)
      return;
    defaultMaxDepth = conf.getInt("scoring.depth.max", DEFAULT_MAX_DEPTH);
    if (defaultMaxDepth <= 0) {
      defaultMaxDepth = DEFAULT_MAX_DEPTH;
    }
  }

  @Override
  public CrawlDatum distributeScoreToOutlinks(Text fromUrl,
      ParseData parseData, Collection<Entry<Text, CrawlDatum>> targets,
      CrawlDatum adjust, int allCount) throws ScoringFilterException {
    String depthString = parseData.getMeta(DEPTH_KEY);
    if (depthString == null) {
      LOG.warn("Missing depth, removing all outlinks from url " + fromUrl);
      targets.clear();
      return adjust;
    }
    int curDepth = Integer.parseInt(depthString);
    int curMaxDepth = defaultMaxDepth;
    IntWritable customMaxDepth = null;
    // allow overrides from injector
    String maxDepthString = parseData.getMeta(MAX_DEPTH_KEY);
    if (maxDepthString != null) {
      curMaxDepth = Integer.parseInt(maxDepthString);
      customMaxDepth = new IntWritable(curMaxDepth);
    }
    if (curDepth >= curMaxDepth) {
      // depth exceeded - throw away
      LOG.info("Depth limit (" + curMaxDepth
          + ") reached, ignoring outlinks for " + fromUrl);
      targets.clear();
      return adjust;
    }
    Iterator<Entry<Text, CrawlDatum>> it = targets.iterator();
    while (it.hasNext()) {
      Entry<Text, CrawlDatum> e = it.next();
      // record increased depth
      e.getValue().getMetaData()
          .put(DEPTH_KEY_W, new IntWritable(curDepth + 1));
      // record maxDepth if any
      if (customMaxDepth != null) {
        e.getValue().getMetaData().put(MAX_DEPTH_KEY_W, customMaxDepth);
      }
    }
    return adjust;
  }

  // prioritize by smaller values of depth
  @Override
  public float generatorSortValue(Text url, CrawlDatum datum, float initSort)
      throws ScoringFilterException {
    // boost up by current depth
    int curDepth, curMaxDepth;
    IntWritable maxDepth = (IntWritable) datum.getMetaData().get(
        MAX_DEPTH_KEY_W);
    if (maxDepth != null) {
      curMaxDepth = maxDepth.get();
    } else {
      curMaxDepth = defaultMaxDepth;
    }
    IntWritable depth = (IntWritable) datum.getMetaData().get(DEPTH_KEY_W);
    if (depth == null) {
      // penalize
      curDepth = curMaxDepth;
    } else {
      curDepth = depth.get();
    }
    int mul = curMaxDepth - curDepth;
    return initSort * (1 + mul);
  }

  public float indexerScore(Text url, NutchDocument doc, CrawlDatum dbDatum,
      CrawlDatum fetchDatum, Parse parse, Inlinks inlinks, float initScore)
      throws ScoringFilterException {
    return initScore;
  }

  @Override
  public void initialScore(Text url, CrawlDatum datum)
      throws ScoringFilterException {
    // the datum might already have some values set
    // e.g. obtained from redirection
    // in which case we don't want to override them
    if (datum.getMetaData().get(MAX_DEPTH_KEY_W) == null)
      datum.getMetaData()
          .put(MAX_DEPTH_KEY_W, new IntWritable(defaultMaxDepth));
    // initial depth is 1
    if (datum.getMetaData().get(DEPTH_KEY_W) == null)
      datum.getMetaData().put(DEPTH_KEY_W, new IntWritable(1));
  }

  @Override
  public void injectedScore(Text url, CrawlDatum datum)
      throws ScoringFilterException {

    // check for the presence of the depth limit key
    if (datum.getMetaData().get(MAX_DEPTH_KEY_W) != null) {
      // convert from Text to Int
      String depthString = datum.getMetaData().get(MAX_DEPTH_KEY_W).toString();
      datum.getMetaData().remove(MAX_DEPTH_KEY_W);
      int depth = Integer.parseInt(depthString);
      datum.getMetaData().put(MAX_DEPTH_KEY_W, new IntWritable(depth));
    } else { // put the default
      datum.getMetaData()
          .put(MAX_DEPTH_KEY_W, new IntWritable(defaultMaxDepth));
    }
    // initial depth is 1
    datum.getMetaData().put(DEPTH_KEY_W, new IntWritable(1));
  }

  @Override
  public void passScoreAfterParsing(Text url, Content content, Parse parse)
      throws ScoringFilterException {
    String depth = content.getMetadata().get(DEPTH_KEY);
    if (depth != null) {
      parse.getData().getParseMeta().set(DEPTH_KEY, depth);
    }
    String maxdepth = content.getMetadata().get(MAX_DEPTH_KEY);
    if (maxdepth != null) {
      parse.getData().getParseMeta().set(MAX_DEPTH_KEY, maxdepth);
    }
  }

  @Override
  public void passScoreBeforeParsing(Text url, CrawlDatum datum, Content content)
      throws ScoringFilterException {
    IntWritable depth = (IntWritable) datum.getMetaData().get(DEPTH_KEY_W);
    if (depth != null) {
      content.getMetadata().set(DEPTH_KEY, depth.toString());
    }
    IntWritable maxdepth = (IntWritable) datum.getMetaData().get(
        MAX_DEPTH_KEY_W);
    if (maxdepth != null) {
      content.getMetadata().set(MAX_DEPTH_KEY, maxdepth.toString());
    }
  }

  @Override
  public void updateDbScore(Text url, CrawlDatum old, CrawlDatum datum,
      List<CrawlDatum> inlinked) throws ScoringFilterException {
    // find a minimum of all depths
    int newDepth = DEFAULT_MAX_DEPTH;
    if (old != null) {
      IntWritable oldDepth = (IntWritable) old.getMetaData().get(DEPTH_KEY_W);
      if (oldDepth != null) {
        newDepth = oldDepth.get();
      } else {
        // not set ?
        initialScore(url, old);
      }
    }
    for (CrawlDatum lnk : inlinked) {
      IntWritable depth = (IntWritable) lnk.getMetaData().get(DEPTH_KEY_W);
      if (depth != null && depth.get() < newDepth) {
        newDepth = depth.get();
      }
    }
    datum.getMetaData().put(DEPTH_KEY_W, new IntWritable(newDepth));
  }
}
