package org.apache.nutch.indexer.field.boost;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.field.FieldFilter;
import org.apache.nutch.indexer.field.FieldType;
import org.apache.nutch.indexer.field.FieldWritable;
import org.apache.nutch.indexer.field.Fields;

/**
 * A field filter that indexes fields of content type Boost or type Computation.
 * 
 * Boost fields are aggregated together to create a global score for a single 
 * Lucene document in the index.  An example of a Boost fields would be the 
 * LinkRank score.
 */
public class BoostFieldFilter
  implements FieldFilter {

  public static final Log LOG = LogFactory.getLog(BoostFieldFilter.class);
  private Configuration conf;

  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  public Configuration getConf() {
    return this.conf;
  }

  public Document filter(String url, Document doc, List<FieldWritable> fields)
    throws IndexingException {

    List<String> comps = new ArrayList<String>();
    float boost = 0.0f;

    for (FieldWritable field : fields) {

      // save the boost factor as unindexed fields, to show different scoring
      FieldType type = field.getType();
      if (type == FieldType.BOOST) {
        float fieldBoost = field.getBoost();
        boost += fieldBoost;
        doc.add(new Field(Fields.BOOSTFACTOR, field.getValue() + ": "
          + fieldBoost, Field.Store.YES, Field.Index.NO));
      }
      else if (type == FieldType.COMPUTATION) {
        comps.add(field.getValue());
      }
    }

    // set the boost for the document and save it in the index
    doc.setBoost(boost);
    doc.add(new Field(Fields.BOOST, Float.toString(boost), Field.Store.YES,
      Field.Index.NO));
    
    
    return doc;
  }

}
