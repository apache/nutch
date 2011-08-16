package org.apache.nutch.indexer;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.IndexingFilters;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseResult;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.Protocol;
import org.apache.nutch.protocol.ProtocolFactory;
import org.apache.nutch.util.NutchConfiguration;

/**
 * Reads and parses a URL and run the indexers on it. Displays the fields obtained and the first
 * 100 characters of their value
 * 
 * Tested with e.g. ./nutch org.apache.nutch.indexer.IndexingFiltersChecker http://www.lemonde.fr
 * @author Julien Nioche
 **/

public class IndexingFiltersChecker extends Configured implements Tool {
  
  public static final Log LOG = LogFactory.getLog(IndexingFiltersChecker.class);
  
  public IndexingFiltersChecker() {

  }
  
  public int run(String[] args) throws Exception {
    
    String contentType = null;
    String url = null;
    
    String usage = "Usage: IndexingFiltersChecker <url>";
    
    if (args.length != 1) {
      System.err.println(usage);
      System.exit(-1);
    }
    
    url = args[0];
    
    if (LOG.isInfoEnabled()) {
      LOG.info("fetching: " + url);
    }
        
    IndexingFilters indexers = new IndexingFilters(conf);
    
    ProtocolFactory factory = new ProtocolFactory(conf);
    Protocol protocol = factory.getProtocol(url);
    CrawlDatum datum = new CrawlDatum();
    
    Content content = protocol.getProtocolOutput(new Text(url), datum)
        .getContent();
    
    if (content == null) {
      System.out.println("No content for " + url);
      return 0;
    }
    
    contentType = content.getContentType();
    
    if (contentType == null) {
      return -1;
    }
    
    if (LOG.isInfoEnabled()) {
      LOG.info("parsing: " + url);
      LOG.info("contentType: " + contentType);
    }
    
    ParseResult parseResult = new ParseUtil(conf).parse(content);
    
    NutchDocument doc = new NutchDocument();
    Text urlText = new Text(url);

    Inlinks inlinks = null;
    Parse parse = parseResult.get(urlText);
    try {
      indexers.filter(doc, parse, urlText, datum, inlinks);
    } catch (IndexingException e) {
      e.printStackTrace();
    }

    for (String fname : doc.getFieldNames()) {
      List<Object> values = doc.getField(fname).getValues();
      if (values != null) {
        for (Object value : values) {
          String str = value.toString();
          int minText = Math.min(100, str.length());
          System.out.println(fname + " :\t" + str.substring(0, minText));
        }
      }
    }
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    final int res = ToolRunner.run(NutchConfiguration.create(),
        new IndexingFiltersChecker(), args);
    System.exit(res);
  }
  
  Configuration conf;
  
  public Configuration getConf() {
    return conf;
  }
  
  @Override
  public void setConf(Configuration arg0) {
    conf = arg0;
  }
}
