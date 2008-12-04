package org.apache.nutch.indexer.field;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.lucene.document.Document;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.plugin.Extension;
import org.apache.nutch.plugin.ExtensionPoint;
import org.apache.nutch.plugin.PluginRepository;
import org.apache.nutch.plugin.PluginRuntimeException;
import org.apache.nutch.util.ObjectCache;

/**
 * The FieldFilters class provides a standard way to collect, order, and run
 * all FieldFilter implementations that are active in the plugin system.
 */
public class FieldFilters {

  public static final String FIELD_FILTER_ORDER = "field.filter.order";

  public final static Log LOG = LogFactory.getLog(FieldFilters.class);

  private FieldFilter[] fieldFilters;

  /**
   * Configurable constructor.
   */
  public FieldFilters(Configuration conf) {

    // get the field filter order, the cache, and all field filters
    String order = conf.get(FIELD_FILTER_ORDER);
    ObjectCache objectCache = ObjectCache.get(conf);
    this.fieldFilters = (FieldFilter[])objectCache.getObject(FieldFilter.class.getName());
    
    if (this.fieldFilters == null) {

      String[] orderedFilters = null;
      if (order != null && !order.trim().equals("")) {
        orderedFilters = order.split("\\s+");
      }
      try {

        // get the field filter extension point
        ExtensionPoint point = PluginRepository.get(conf).getExtensionPoint(
          FieldFilter.X_POINT_ID);
        if (point == null) {
          throw new RuntimeException(FieldFilter.X_POINT_ID + " not found.");
        }
        
        // get all of the field filter plugins
        Extension[] extensions = point.getExtensions();
        HashMap<String, FieldFilter> filterMap = new HashMap<String, FieldFilter>();
        for (int i = 0; i < extensions.length; i++) {
          Extension extension = extensions[i];
          FieldFilter filter = (FieldFilter)extension.getExtensionInstance();
          LOG.info("Adding " + filter.getClass().getName());
          if (!filterMap.containsKey(filter.getClass().getName())) {
            filterMap.put(filter.getClass().getName(), filter);
          }
        }

        // order the filters if necessary
        if (orderedFilters == null) {
          objectCache.setObject(FieldFilter.class.getName(),
            filterMap.values().toArray(new FieldFilter[0]));
        }
        else {
          ArrayList<FieldFilter> filters = new ArrayList<FieldFilter>();
          for (int i = 0; i < orderedFilters.length; i++) {
            FieldFilter filter = filterMap.get(orderedFilters[i]);
            if (filter != null) {
              filters.add(filter);
            }
          }
          objectCache.setObject(FieldFilter.class.getName(),
            filters.toArray(new FieldFilter[filters.size()]));
        }
      }
      catch (PluginRuntimeException e) {
        throw new RuntimeException(e);
      }
      
      // set the filters in the cache
      this.fieldFilters = (FieldFilter[])objectCache.getObject(FieldFilter.class.getName());
    }
  }

  /**
   * Runs all FieldFilter extensions.
   * 
   * @param url The url to index.
   * @param doc The lucene index document.
   * @param fields The lucene fields.
   * 
   * @return The document to filter or null to not index this document and url.
   * 
   * @throws IndexingException If an error occurs while running filters.
   */
  public Document filter(String url, Document doc, List<FieldWritable> fields)
    throws IndexingException {

    // loop through and run the field filters
    for (int i = 0; i < this.fieldFilters.length; i++) {
      doc = this.fieldFilters[i].filter(url, doc, fields);
      if (doc == null) {
        return null;
      }
    }
    return doc;
  }

}
