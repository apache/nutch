package org.apache.nutch.parse;

import org.apache.hadoop.conf.Configurable;
import org.apache.nutch.plugin.FieldPluggable;
import org.apache.nutch.storage.WebPage;
import org.w3c.dom.DocumentFragment;


/** Extension point for DOM-based HTML parsers.  Permits one to add additional
 * metadata to HTML parses.  All plugins found which implement this extension
 * point are run sequentially on the parse.
 */
public interface HtmlParseFilter extends FieldPluggable, Configurable {
  /** The name of the extension point. */
  final static String X_POINT_ID = HtmlParseFilter.class.getName();

  /** Adds metadata or otherwise modifies a parse of HTML content, given
   * the DOM tree of a page. */
  Parse filter(String url, WebPage page, Parse parse,
                    HTMLMetaTags metaTags, DocumentFragment doc);

}
