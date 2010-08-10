package org.apache.nutch.indexer;

import java.io.IOException;

import org.apache.avro.util.Utf8;
import org.slf4j.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.StorageUtils;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.StringUtil;
import org.apache.nutch.util.TableUtil;
import org.gora.store.DataStore;

public class IndexerReducer
extends Reducer<String, WebPage, String, NutchDocument> {

  public static final Logger LOG = IndexerJob.LOG;

  private IndexingFilters filters;

  private ScoringFilters scoringFilters;

  private DataStore<String, WebPage> store;

  @Override
  protected void setup(Context context) throws IOException {
    Configuration conf = context.getConfiguration();
    filters = new IndexingFilters(conf);
    scoringFilters = new ScoringFilters(conf);
    try {
      store = StorageUtils.createDataStore(conf, String.class, WebPage.class);
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    }
  }

  @Override
  protected void reduce(String key, Iterable<WebPage> values,
      Context context) throws IOException, InterruptedException {
    WebPage page = values.iterator().next();
    NutchDocument doc = new NutchDocument();

    doc.add("id", key);
    doc.add("digest", StringUtil.toHexString(page.getSignature().array()));

    String url = TableUtil.unreverseUrl(key);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Indexing URL: " + url);
    }

    try {
      doc = filters.filter(doc, url, page);
    } catch (IndexingException e) {
      LOG.warn("Error indexing "+key+": "+e);
      return;
    }

    // skip documents discarded by indexing filters
    if (doc == null) return;

    float boost = 1.0f;
    // run scoring filters
    try {
      boost = scoringFilters.indexerScore(url, doc, page, boost);
    } catch (final ScoringFilterException e) {
      LOG.warn("Error calculating score " + key + ": " + e);
      return;
    }

    doc.setScore(boost);
    // store boost for use by explain and dedup
    doc.add("boost", Float.toString(boost));

    Utf8 mark = Mark.UPDATEDB_MARK.checkMark(page);
    if (mark != null) {
      Mark.INDEX_MARK.putMark(page, Mark.UPDATEDB_MARK.checkMark(page));
      store.put(key, page);
    }
    context.write(key, doc);
  }

  @Override
  public void cleanup(Context context) throws IOException {
    store.close();
  }

}
