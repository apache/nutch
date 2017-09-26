package my.foo;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.plugin.URLStreamHandlerFactory;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.Protocol;
import org.apache.nutch.protocol.ProtocolOutput;
import org.apache.nutch.protocol.ProtocolStatus;
import org.apache.nutch.protocol.RobotRulesParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import crawlercommons.robots.BaseRobotRules;

public class Foo implements Protocol {
  protected static final Logger LOG = LoggerFactory
      .getLogger(Foo.class);
  
  private Configuration conf;

  @Override
  public Configuration getConf() {
    LOG.debug("getConf()");
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    LOG.debug("setConf(...)");
    this.conf = conf;
  }

  @Override
  public ProtocolOutput getProtocolOutput(Text url, CrawlDatum datum) {
    LOG.debug("getProtocolOutput("+url+", "+datum+")");
    return new ProtocolOutput(new Content(), ProtocolStatus.STATUS_SUCCESS);
  }

  @Override
  public BaseRobotRules getRobotRules(Text url, CrawlDatum datum,
      List<Content> robotsTxtContent) {
    LOG.debug("getRobotRules("+url+", "+datum+", "+robotsTxtContent+")");
    return RobotRulesParser.EMPTY_RULES;
  }
}
