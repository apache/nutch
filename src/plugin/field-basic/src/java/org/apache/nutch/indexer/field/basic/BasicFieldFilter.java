/*
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
package org.apache.nutch.indexer.field.basic;

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

/**
 * Adds any field of type content to the index.
 */
public class BasicFieldFilter
  implements FieldFilter {

  public static final Log LOG = LogFactory.getLog(BasicFieldFilter.class);
  private Configuration conf;
  private boolean supplemental = false;
  private String[] suppFields = null;

  private boolean isSupplementalField(String name) {
    for (int i = 0; i < suppFields.length; i++) {
      if (name != null && name.equals(suppFields[i])) {
        return true;
      }
    }
    return false;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
    this.supplemental = conf.getBoolean("index.supplemental", false);
    String suppStr = conf.get("index.supplemental.fields", null);
    if (suppStr != null) {
      suppFields = suppStr.split(",");
    }
  }

  public Configuration getConf() {
    return this.conf;
  }

  public Document filter(String url, Document doc, List<FieldWritable> fields)
    throws IndexingException {

    // loop through all of the fields
    for (FieldWritable field : fields) {

      // only grab content fields
      FieldType type = field.getType();
      if (type == FieldType.CONTENT) {

        String fieldName = field.getName();
        
        // supplemental index will only index certain key fields, allow creating
        // both a regular and a supplemental index
        if (!supplemental || (supplemental && isSupplementalField(fieldName))) {

          // create lucene fields from the FieldWritable objects
          Field.Store store = field.isStored() ? Field.Store.YES
            : Field.Store.NO;
          Field.Index indexed = field.isIndexed() ? field.isTokenized()
            ? Field.Index.TOKENIZED : Field.Index.UN_TOKENIZED : Field.Index.NO;
          Field docField = new Field(fieldName, field.getValue(), store,
            indexed);

          // if any field boost then set it
          float fieldBoost = field.getBoost();
          if (fieldBoost > 0) {
            docField.setBoost(fieldBoost);
          }

          // add the field to the lucene document
          doc.add(docField);
        }
        else {
          LOG.info("Ignoring " + fieldName + " field for " + url + " supplemental index");
        }
      }
    }

    return doc;
  }
}
