package org.apache.nutch.searcher.response;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.plugin.Extension;
import org.apache.nutch.plugin.ExtensionPoint;
import org.apache.nutch.plugin.PluginRepository;
import org.apache.nutch.plugin.PluginRuntimeException;
import org.apache.nutch.util.ObjectCache;

/**
 * Utility class for getting all ResponseWriter implementations and for
 * returning the correct ResponseWriter for a given request type.
 */
public class ResponseWriters {

  private Map<String, ResponseWriter> responseWriters;

  /**
   * Constructor that configures the cache of ResponseWriter objects.
   * 
   * @param conf The Nutch configuration object.
   */
  public ResponseWriters(Configuration conf) {

    // get the cache and the cache key
    String cacheKey = ResponseWriter.class.getName();
    ObjectCache objectCache = ObjectCache.get(conf);
    this.responseWriters = (Map<String, ResponseWriter>)objectCache.getObject(cacheKey);

    // if already populated do nothing
    if (this.responseWriters == null) {

      try {

        // get the extension point and all ResponseWriter extensions
        ExtensionPoint point = PluginRepository.get(conf).getExtensionPoint(
          ResponseWriter.X_POINT_ID);
        if (point == null) {
          throw new RuntimeException(ResponseWriter.X_POINT_ID + " not found.");
        }

        // populate content type on the ResponseWriter classes, each response
        // writer can handle more than one response type
        Extension[] extensions = point.getExtensions();
        Map<String, ResponseWriter> writers = new HashMap<String, ResponseWriter>();
        for (int i = 0; i < extensions.length; i++) {
          Extension extension = extensions[i];
          ResponseWriter writer = (ResponseWriter)extension.getExtensionInstance();
          String[] responseTypes = extension.getAttribute("responseType").split(
            ",");
          String contentType = extension.getAttribute("contentType");
          writer.setContentType(contentType);
          for (int k = 0; k < responseTypes.length; k++) {
            writers.put(responseTypes[k], writer);
          }
        }

        // set null object if no writers, otherwise set the writers
        if (writers == null) {
          objectCache.setObject(cacheKey, new HashMap<String, ResponseWriter>());
        }
        else {
          objectCache.setObject(cacheKey, writers);
        }
      }
      catch (PluginRuntimeException e) {
        throw new RuntimeException(e);
      }

      // set the response writers map
      this.responseWriters = (Map<String, ResponseWriter>)objectCache.getObject(cacheKey);
    }
  }

  /**
   * Return the correct ResponseWriter object for the response type.
   * 
   * @param respType The response type, such as xml or json. Must correspond to
   * the value set in the plugin.xml file for the ResponseWriter extension.
   * 
   * @return The ResponseWriter that handles that response type or null if no
   * such object exists.
   */
  public ResponseWriter getResponseWriter(String respType) {
    return responseWriters.get(respType);
  }
}
