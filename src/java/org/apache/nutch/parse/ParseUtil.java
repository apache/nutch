/**
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
package org.apache.nutch.parse;

// Commons Logging imports
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.avro.util.Utf8;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.StringUtils;
import org.apache.nutch.crawl.CrawlStatus;
import org.apache.nutch.crawl.Signature;
import org.apache.nutch.crawl.SignatureFactory;
import org.apache.nutch.crawl.URLWebPage;
import org.apache.nutch.fetcher.FetcherJob;
import org.apache.nutch.net.URLFilterException;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.ParseStatus;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.TableUtil;
import org.apache.nutch.util.URLUtil;

/**
 * A Utility class containing methods to simply perform parsing utilities such
 * as iterating through a preferred list of {@link Parser}s to obtain
 * {@link Parse} objects.
 *
 * @author mattmann
 * @author J&eacute;r&ocirc;me Charron
 * @author S&eacute;bastien Le Callonnec
 */
public class ParseUtil extends Configured {

  /* our log stream */
  public static final Log LOG = LogFactory.getLog(ParseUtil.class);

  private Configuration conf;

  private Signature sig;
  private URLFilters filters;
  private URLNormalizers normalizers;
  private int maxOutlinks;
  private boolean ignoreExternalLinks;
  private ParserFactory parserFactory;
  /** Parser timeout set to 30 sec by default. Set -1 to deactivate **/
  private int MAX_PARSE_TIME = 30;
  /**
   *
   * @param conf
   */
  public ParseUtil(Configuration conf) {
    super(conf);
    setConf(conf);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    parserFactory = new ParserFactory(conf);
    MAX_PARSE_TIME=conf.getInt("parser.timeout", 30);
    sig = SignatureFactory.getSignature(conf);
    filters = new URLFilters(conf);
    normalizers = new URLNormalizers(conf, URLNormalizers.SCOPE_OUTLINK);
    int maxOutlinksPerPage = conf.getInt("db.max.outlinks.per.page", 100);
    maxOutlinks = (maxOutlinksPerPage < 0) ? Integer.MAX_VALUE : maxOutlinksPerPage;
    ignoreExternalLinks = conf.getBoolean("db.ignore.external.links", false);
  }

  /**
   * Performs a parse by iterating through a List of preferred {@link Parser}s
   * until a successful parse is performed and a {@link Parse} object is
   * returned. If the parse is unsuccessful, a message is logged to the
   * <code>WARNING</code> level, and an empty parse is returned.
   *
   * @throws ParseException If no suitable parser is found to perform the parse.
   */
  public Parse parse(String url, WebPage page) throws ParseException {
    Parser[] parsers = null;

    String contentType = TableUtil.toString(page.getContentType());

    try {
      parsers = this.parserFactory.getParsers(contentType, url);
    } catch (ParserNotFound e) {
      LOG.warn("No suitable parser found when trying to parse content " + url +
          " of type " + contentType);
      throw new ParseException(e.getMessage());
    }

    for (int i=0; i<parsers.length; i++) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Parsing [" + url + "] with [" + parsers[i] + "]");
      }
      Parse parse = null;
      
      if (MAX_PARSE_TIME!=-1)
    	  parse = runParser(parsers[i], url, page);
      else 
    	  parse = parsers[i].getParse(url, page);
      
      if (ParseStatusUtils.isSuccess(parse.getParseStatus())) {
        return parse;
      }
    }

    LOG.warn("Unable to successfully parse content " + url +
        " of type " + contentType);
    return ParseStatusUtils.getEmptyParse(new ParseException("Unable to successfully parse content"), null);
  }
  
  private Parse runParser(Parser p, String url, WebPage page) {
	  ParseCallable pc = new ParseCallable(p, page, url);
	  FutureTask<Parse> task = new FutureTask<Parse>(pc);
	  Parse res = null;
	  Thread t = new Thread(task);
	  t.start();
	  try {
		  res = task.get(MAX_PARSE_TIME, TimeUnit.SECONDS);
	  } catch (TimeoutException e) {
		  LOG.warn("TIMEOUT parsing " + url + " with " + p);
	  } catch (Exception e) {
		  task.cancel(true);
		  res = null;
		  t.interrupt();
	  } finally {
		  t = null;
		  pc = null;
	  }
	  return res;
  }

  /**
   * Parses given web page and stores parsed content within page. Returns
   * a pair of <String, WebPage> if a meta-redirect is discovered
   * @param key
   * @param page
   * @return newly-discovered webpage (via a meta-redirect)
   */
  public URLWebPage process(String key, WebPage page) {
    URLWebPage redirectedPage = null;
    String url = TableUtil.unreverseUrl(key);
    byte status = (byte) page.getStatus();
    if (status != CrawlStatus.STATUS_FETCHED) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Skipping " + url + " as status is: " + CrawlStatus.getName(status));
      }
      return redirectedPage;
    }

    Parse parse;
    try {
      parse = parse(url, page);
    } catch (final Exception e) {
      LOG.warn("Error parsing: " + url + ": " + StringUtils.stringifyException(e));
      return redirectedPage;
    }

    if (parse == null) {
      return redirectedPage;
    }

    final byte[] signature = sig.calculate(page);

    org.apache.nutch.storage.ParseStatus pstatus = parse.getParseStatus();
    page.setParseStatus(pstatus);
    if (ParseStatusUtils.isSuccess(pstatus)) {
      if (pstatus.getMinorCode() == ParseStatusCodes.SUCCESS_REDIRECT) {
        String newUrl = ParseStatusUtils.getMessage(pstatus);
        int refreshTime = Integer.parseInt(ParseStatusUtils.getArg(pstatus, 1));
        try {
          newUrl = normalizers.normalize(newUrl, URLNormalizers.SCOPE_FETCHER);
          newUrl = filters.filter(newUrl);
        } catch (URLFilterException e) {
          return redirectedPage; // TODO: is this correct
        } catch (MalformedURLException e) {
          return redirectedPage;
        }
        if (newUrl == null || newUrl.equals(url)) {
          String reprUrl = URLUtil.chooseRepr(url, newUrl,
              refreshTime < FetcherJob.PERM_REFRESH_TIME);
          WebPage newWebPage = new WebPage();
          page.setReprUrl(new Utf8(reprUrl));
          page.putToMetadata(FetcherJob.REDIRECT_DISCOVERED, TableUtil.YES_VAL);
          redirectedPage = new URLWebPage(reprUrl, newWebPage);
        }
      } else {
        page.setText(new Utf8(parse.getText()));
        page.setTitle(new Utf8(parse.getTitle()));
        ByteBuffer prevSig = page.getSignature();
        if (prevSig != null) {
          page.setPrevSignature(prevSig);
        }
        page.setSignature(ByteBuffer.wrap(signature));
        if (page.getOutlinks() != null) {
          page.getOutlinks().clear();
        }
        final Outlink[] outlinks = parse.getOutlinks();
        final int count = 0;
        String fromHost;
        if (ignoreExternalLinks) {
          try {
            fromHost = new URL(url).getHost().toLowerCase();
          } catch (final MalformedURLException e) {
            fromHost = null;
          }
        } else {
          fromHost = null;
        }
        for (int i = 0; count < maxOutlinks && i < outlinks.length; i++) {
          String toUrl = outlinks[i].getToUrl();
          try {
            toUrl = normalizers.normalize(toUrl, URLNormalizers.SCOPE_OUTLINK);
            toUrl = filters.filter(toUrl);
          } catch (final URLFilterException e) {
            continue;
          }
          catch (MalformedURLException e2){
            continue;
          }
          if (toUrl == null) {
            continue;
          }
          String toHost;
          if (ignoreExternalLinks) {
            try {
              toHost = new URL(toUrl).getHost().toLowerCase();
            } catch (final MalformedURLException e) {
              toHost = null;
            }
            if (toHost == null || !toHost.equals(fromHost)) { // external links
              continue; // skip it
            }
          }

          page.putToOutlinks(new Utf8(toUrl), new Utf8(outlinks[i].getAnchor()));
        }
        Mark.PARSE_MARK.putMark(page, Mark.FETCH_MARK.checkMark(page));
      }
    }
    return redirectedPage;
  }
}
