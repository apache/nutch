package org.apache.nutch.parse;

import org.w3c.dom.DocumentFragment;

// Hadoop imports
import org.apache.hadoop.conf.Configurable;

// Nutch imports
import org.apache.nutch.parse.HTMLMetaTags;
import org.apache.nutch.plugin.TablePluggable;
import org.apache.nutch.util.hbase.WebTableRow;


/** Extension point for DOM-based HTML parsers.  Permits one to add additional
 * metadata to HTML parses.  All plugins found which implement this extension
 * point are run sequentially on the parse.
 */
public interface HtmlParseFilter extends TablePluggable, Configurable {
  /** The name of the extension point. */
  final static String X_POINT_ID = HtmlParseFilter.class.getName();

  /** Adds metadata or otherwise modifies a parse of HTML content, given
   * the DOM tree of a page. */
  Parse filter(String url, WebTableRow row, Parse parseResult,
                    HTMLMetaTags metaTags, DocumentFragment doc);
  
}
