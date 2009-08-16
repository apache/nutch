package org.apache.nutch.indexer;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.util.StringUtil;
import org.apache.nutch.util.hbase.WebTableRow;
import org.apache.nutch.util.hbase.TableUtil;

public class IndexerReducer
extends Reducer<ImmutableBytesWritable, WebTableRow, ImmutableBytesWritable, NutchDocument> {

  public static final Log LOG = Indexer.LOG;
  
  private IndexingFilters filters;
  
  private ScoringFilters scoringFilters;
  
  private HTable table;
  
  @Override
  protected void setup(Context context) throws IOException {
    Configuration conf = context.getConfiguration();
    filters = new IndexingFilters(conf);
    table = new HTable(conf.get(TableInputFormat.INPUT_TABLE));
    scoringFilters = new ScoringFilters(conf);
  }
  
  @Override
  protected void reduce(ImmutableBytesWritable key, Iterable<WebTableRow> values,
      Context context) throws IOException, InterruptedException {
    WebTableRow row = values.iterator().next();
    NutchDocument doc = new NutchDocument();

    doc.add("id", Bytes.toString(key.get()));
    doc.add("digest", StringUtil.toHexString(row.getSignature()));

    String url = TableUtil.unreverseUrl(Bytes.toString(key.get()));

    if (LOG.isDebugEnabled()) {
      LOG.debug("Indexing URL: " + url);
    }

    try {
      doc = filters.filter(doc, url, row);
    } catch (IndexingException e) {
      LOG.warn("Error indexing "+key+": "+e);
      return;
    }

    // skip documents discarded by indexing filters
    if (doc == null) return;

    float boost = 1.0f;
    // run scoring filters
    try {
      boost = scoringFilters.indexerScore(url, doc, row, boost);
    } catch (final ScoringFilterException e) {
      LOG.warn("Error calculating score " + key + ": " + e);
      return;
    }

    doc.setScore(boost);
    // store boost for use by explain and dedup
    doc.add("boost", Float.toString(boost));

    row.putMeta(Indexer.INDEX_MARK, TableUtil.YES_VAL);
    row.makeRowMutation().commit(table);
    context.write(key, doc);
  }
  
  @Override
  public void cleanup(Context context) throws IOException {
    table.close();
  }

}
