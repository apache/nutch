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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.StringUtils;
import org.apache.nutch.crawl.CrawlStatus;
import org.apache.nutch.crawl.InjectType;
import org.apache.nutch.crawl.Signature;
import org.apache.nutch.crawl.SignatureFactory;
import org.apache.nutch.fetcher.FetcherJob;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.net.URLFilterException;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.ParseStatus;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.TableUtil;
import org.apache.nutch.util.URLUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

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

  public enum ChangeFrequency {
    ALWAYS, HOURLY, DAILY, WEEKLY, MONTHLY, YEARLY, NEVER
  }
  /* our log stream */
  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  private static final int DEFAULT_MAX_PARSE_TIME = 30;
  private static final int DEFAULT_OUTLINKS_MAX_TARGET_LENGTH = 3000;

  private Configuration conf;
  private int interval;
  private Signature sig;
  private URLFilters filters;
  private URLNormalizers normalizers;
  private int maxOutlinks;
  private boolean ignoreExternalLinks;
  private ParserFactory parserFactory;
  /** Parser timeout set to 30 sec by default. Set -1 to deactivate **/
  private int maxParseTime;
  private int maxTargetLength;
  private ExecutorService executorService;

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
    interval = conf.getInt("db.fetch.interval.default", 2592000);
    parserFactory = new ParserFactory(conf);
    maxTargetLength = conf.getInt("parser.html.outlinks.max.target.length", DEFAULT_OUTLINKS_MAX_TARGET_LENGTH);
    if (conf.getBoolean("parse.sitemap", false)) {
      maxParseTime = conf.getInt("parser.timeout", DEFAULT_MAX_PARSE_TIME);
    } else {
      maxParseTime = conf
          .getInt("sitemap.parser.timeout", DEFAULT_MAX_PARSE_TIME);
    }
    sig = SignatureFactory.getSignature(conf);
    filters = new URLFilters(conf);
    normalizers = new URLNormalizers(conf, URLNormalizers.SCOPE_OUTLINK);
    int maxOutlinksPerPage = conf.getInt("db.max.outlinks.per.page", 100);
    maxOutlinks = (maxOutlinksPerPage < 0) ? Integer.MAX_VALUE
        : maxOutlinksPerPage;
    ignoreExternalLinks = conf.getBoolean("db.ignore.external.links", false);
    executorService = Executors.newCachedThreadPool(new ThreadFactoryBuilder()
        .setNameFormat("parse-%d").setDaemon(true).build());
  }

  /**
   * Performs a parse by iterating through a List of preferred {@link Parser}s
   * until a successful parse is performed and a {@link Parse} object is
   * returned. If the parse is unsuccessful, a message is logged to the
   * <code>WARNING</code> level, and an empty parse is returned.
   * 
   * @throws ParserNotFound
   *           If there is no suitable parser found.
   * @throws ParseException
   *           If there is an error parsing.
   */
  public Parse parse(String url, WebPage page) throws ParseException {
    Parser[] parsers = null;
    Parse parse = null;

    String contentType = TableUtil.toString(page.getContentType());
    parsers = this.parserFactory.getParsers(contentType, url);

    for (int i = 0; i < parsers.length; i++) {
      parse = parse(url, page, parsers[i]);
      if (parse != null && ParseStatusUtils.isSuccess(parse.getParseStatus())) {
        return parse;
      }
    }

    LOG.warn("Unable to successfully parse content " + url + " of type "
        + contentType);
    return ParseStatusUtils.getEmptyParse(new ParseException(
        "Unable to successfully parse content"), null);
  }

  private Parse parse(String url, WebPage page, Parser parser) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Parsing [" + url + "] with [" + parser + "]");
    }
    if (maxParseTime != -1) {
      return runParser(parser, url, page);
    } else {
      return parser.getParse(url, page);
    }
  }

  private Parse runParser(Parser p, String url, WebPage page) {
    ParseCallable pc = new ParseCallable(p, page, url);
    Future<Parse> task = executorService.submit(pc);
    Parse res = null;
    try {
      res = task.get(maxParseTime, TimeUnit.SECONDS);
    } catch (Exception e) {
      LOG.warn("Error parsing " + url, e);
      task.cancel(true);
    } finally {
      pc = null;
    }
    return res;
  }

  public boolean status(String url, WebPage page) {
    byte status = page.getStatus().byteValue();
    if (status != CrawlStatus.STATUS_FETCHED) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Skipping " + url + " as status is: "
            + CrawlStatus.getName(status));
      }
      return true;
    }
    return false;
  }

  /**
   * Parses given sitemap page and returns the list of new rows
   * to add to the crawler store.
   */
  public List<WebPage> processSitemapParse(String url, WebPage page) {
    List<WebPage> newRows = new ArrayList<>();
    if (status(url, page)) {
      return newRows;
    }
    
    SitemapParse sitemapParse = null;
    try {
      for (SitemapParser parser : parserFactory.getSitemapParsers()) {
        sitemapParse = parser.getParse(url, page);
        if (sitemapParse != null && ParseStatusUtils.isSuccess(sitemapParse.getParseStatus())) {
          break;
        }
      }
    } catch (ParserNotFound e) {
    }

    if (sitemapParse == null) {
      return newRows;
    }

    ParseStatus pstatus = sitemapParse.getParseStatus();
    page.setParseStatus(pstatus);
    if (ParseStatusUtils.isSuccess(pstatus)) {
      final Map<Outlink, Metadata> outlinkMap = sitemapParse.getOutlinkMap();
      if (pstatus.getMinorCode() == ParseStatusCodes.SUCCESS_REDIRECT) {
        successRedirect(url, page, pstatus);
      } else if (outlinkMap != null) {
        setSignature(page);
        
        for (Map.Entry<Outlink, Metadata> entry : outlinkMap.entrySet()) {
          Outlink outlink = entry.getKey();
          Metadata metadata = entry.getValue();
          
          String toUrl = outlink.getToUrl();
          page.getOutlinks().put(new Utf8(toUrl), new Utf8(outlink.getAnchor()));

          try {
            toUrl = normalizers.normalize(toUrl, URLNormalizers.SCOPE_OUTLINK);
            toUrl = filters.filter(toUrl);
            if (toUrl == null) {
              continue;
            }
          } catch (MalformedURLException | URLFilterException e) {
            continue;
          }
          
          WebPage newRow = WebPage.newBuilder().build();
          newRow.setBaseUrl(toUrl);
          
          // if the link specified a change frequency, add it to the new row
          String changeFrequency = metadata.get("changeFrequency");
          if (changeFrequency != null) {;
            newRow.setFetchInterval(calculateFetchInterval(changeFrequency));
          } else {
            newRow.setFetchInterval(interval);
          }
          // if the link specified a modified time, add it to the new row
          String modifiedTime = metadata.get("lastModified");
          if (modifiedTime != null) {
            newRow.setModifiedTime(Long.valueOf(modifiedTime));
          }
          // if the link specified a priority, add it to the new row
          String priority = metadata.get("priority");
          if (priority != null) {
            newRow.setStmPriority(Float.parseFloat(priority));
          }
          // if the link is another sitemap, mark it as such
          String sitemap = metadata.get("sitemap");
          if (sitemap != null) {
            Mark.INJECT_MARK.putMark(newRow, InjectType.SITEMAP_INJECT.getTypeString());
          }
          
          newRow.getSitemaps().put(new Utf8(url), new Utf8("parser"));

          newRows.add(newRow);
        }

        parseMark(page);
      }
    }
    
    return newRows;
  }

  private int calculateFetchInterval(String changeFrequency) {
    if (changeFrequency.equals(ChangeFrequency.ALWAYS.toString())
        || changeFrequency.equals(ChangeFrequency.HOURLY.toString())) {
      return 3600; // 60 * 60
    } else if (changeFrequency.equals(ChangeFrequency.DAILY.toString())) {
      return 86400; // 24 * 60 * 60
    } else if (changeFrequency.equals(ChangeFrequency.WEEKLY.toString())) {
      return 604800; // 7 * 24 * 60 * 60
    } else if (changeFrequency.equals(ChangeFrequency.MONTHLY.toString())) {
      return 2628000; // average seconds in one month
    } else if (changeFrequency.equals(ChangeFrequency.YEARLY.toString())
        || changeFrequency.equals(ChangeFrequency.NEVER.toString())) {
      return 31536000; // average seconds in one year
    } else {
      return Integer.MAX_VALUE; // other intervals are larger than Integer.MAX_VALUE
    }
  }

  private void parseMark(WebPage page) {
    Utf8 fetchMark = Mark.FETCH_MARK.checkMark(page);
    if (fetchMark != null) {
      Mark.PARSE_MARK.putMark(page, fetchMark);
    }
  }

  private void putOutlink(WebPage page, Outlink outlink, String toUrl) {
    try {
      toUrl = normalizers.normalize(toUrl, URLNormalizers.SCOPE_OUTLINK);
      toUrl = filters.filter(toUrl);
    } catch (MalformedURLException e2) {
      return;
    } catch (URLFilterException e) {
      return;
    }
    if (toUrl == null) {
      return;
    }
    Utf8 utf8ToUrl = new Utf8(toUrl);
    if (page.getOutlinks().get(utf8ToUrl) != null) {
      // skip duplicate outlinks
      return;
    }
    page.getOutlinks().put(utf8ToUrl, new Utf8(outlink.getAnchor()));
  }

  /**
   * Parses given web page and stores parsed content within page. Puts a
   * meta-redirect to outlinks.
   *
   * @param url
   * @param page
   */
  public void process(String url, WebPage page) {
    if (status(url, page)) {
      return;
    }
    Parse parse;
    try {
      parse = parse(url, page);
    } catch (ParserNotFound e) {
      // do not print stacktrace for the fact that some types are not mapped.
      LOG.warn("No suitable parser found: " + e.getMessage());
      return;
    } catch (final Exception e) {
      LOG.warn("Error parsing: " + url + ": "
          + StringUtils.stringifyException(e));
      return;
    }

    if (parse == null) {
      return;
    }

    ParseStatus pstatus = parse.getParseStatus();
    page.setParseStatus(pstatus);
    if (ParseStatusUtils.isSuccess(pstatus)) {
      if (pstatus.getMinorCode() == ParseStatusCodes.SUCCESS_REDIRECT) {
        successRedirect(url, page, pstatus);
      } else {
        page.setText(new Utf8(parse.getText()));
        page.setTitle(new Utf8(parse.getTitle()));

        setSignature(page);

        if (page.getOutlinks() != null) {
          page.getOutlinks().clear();
        }
        String fromHost;
        if (ignoreExternalLinks) {
          try {
            fromHost = new URL(url).getHost().toLowerCase(Locale.ROOT);
          } catch (final MalformedURLException e) {
            fromHost = null;
          }
        } else {
          fromHost = null;
        }
        int validCount = 0;

        final Outlink[] outlinks = parse.getOutlinks();
        int outlinksToStore = Math.min(maxOutlinks, outlinks.length);
        for (int i = 0; validCount < outlinksToStore
            && i < outlinks.length; i++, validCount++) {
          String toUrl = outlinks[i].getToUrl();
          if (toUrl.length() > maxTargetLength) {
            continue; // skip it
          }
          String toHost;
          if (ignoreExternalLinks) {
            try {
              toHost = new URL(toUrl).getHost().toLowerCase(Locale.ROOT);
            } catch (final MalformedURLException e) {
              toHost = null;
            }
            if (toHost == null || !toHost.equals(fromHost)) { // external links
              continue; // skip it
            }
          }
          putOutlink(page, outlinks[i], toUrl);
        }
        parseMark(page);
      }
    }
  }

  private void successRedirect(String url, WebPage page, ParseStatus pstatus) {
    String newUrl = ParseStatusUtils.getMessage(pstatus);
    int refreshTime = Integer.parseInt(ParseStatusUtils.getArg(pstatus, 1));
    try {
      newUrl = normalizers.normalize(newUrl, URLNormalizers.SCOPE_FETCHER);
      if (newUrl == null) {
        LOG.warn("redirect normalized to null " + url);
        return;
      }
      try {
        newUrl = filters.filter(newUrl);
      } catch (URLFilterException e) {
        return;
      }
      if (newUrl == null) {
        LOG.warn("redirect filtered to null " + url);
        return;
      }
    } catch (MalformedURLException e) {
      LOG.warn("malformed url exception parsing redirect " + url);
      return;
    }
    page.getOutlinks().put(new Utf8(newUrl), new Utf8());
    page.getMetadata().put(FetcherJob.REDIRECT_DISCOVERED,
        TableUtil.YES_VAL);
    if (newUrl == null || newUrl.equals(url)) {
      String reprUrl = URLUtil.chooseRepr(url, newUrl,
          refreshTime < FetcherJob.PERM_REFRESH_TIME);
      if (reprUrl == null) {
        LOG.warn("reprUrl==null for " + url);
        return;
      } else {
        page.setReprUrl(new Utf8(reprUrl));
      }
    }
  }

  private void setSignature(WebPage page) {
    ByteBuffer prevSig = page.getSignature();
    if (prevSig != null) {
      page.setPrevSignature(prevSig);
    }
    final byte[] signature = sig.calculate(page);
    page.setSignature(ByteBuffer.wrap(signature));
  }
}
