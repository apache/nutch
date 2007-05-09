package org.apache.nutch.parse;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;

/**
 * A utility class that stores result of a parse. Internally
 * a ParseResult stores &lt;{@link Text}, {@link Parse}&gt; pairs.
 */
public class ParseResult implements Iterable<Map.Entry<Text, Parse>> {
  private Map<Text, Parse> parseMap;
  private String originalUrl;
  
  public static final Log LOG = LogFactory.getLog(ParseResult.class);
  
  public ParseResult(String originalUrl) {
    parseMap = new HashMap<Text, Parse>();
    this.originalUrl = originalUrl;
  }
  
  public static ParseResult createParseResult(String url, Parse parse) {
    ParseResult parseResult = new ParseResult(url);
    parseResult.put(new Text(url), new ParseText(parse.getText()), parse.getData());
    return parseResult;
  }
  
  public boolean isEmpty() {
    return parseMap.isEmpty();
  }
  
  public int size() {
    return parseMap.size();
  }
  
  public Parse get(String key) {
    return get(new Text(key));
  }
  
  public Parse get(Text key) {
    return parseMap.get(key);
  }
  
  public void put(Text key, ParseText text, ParseData data) {
    put(key.toString(), text, data);
  }
  
  public void put(String key, ParseText text, ParseData data) {
    parseMap.put(new Text(key), new ParseImpl(text, data, key.equals(originalUrl)));
  }

  public Iterator<Entry<Text, Parse>> iterator() {
    return parseMap.entrySet().iterator();
  }
  
  public void filter() {
    for(Iterator<Entry<Text, Parse>> i = iterator(); i.hasNext();) {
      Entry<Text, Parse> entry = i.next();
      if (!entry.getValue().getData().getStatus().isSuccess()) {
        LOG.warn(entry.getKey() + " is not parsed successfully, filtering");
        i.remove();
      }
    }
      
  }
}
