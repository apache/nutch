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
package org.apache.nutch.parse.feed;

// JDK imports
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

// APACHE imports
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;
import org.apache.nutch.metadata.Feed;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.Parser;
import org.apache.nutch.parse.ParserFactory;
import org.apache.nutch.parse.ParserNotFound;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.EncodingDetector;
import org.apache.nutch.util.NutchConfiguration;
import org.xml.sax.InputSource;

// ROME imports
import com.sun.syndication.feed.synd.SyndCategory;
import com.sun.syndication.feed.synd.SyndContent;
import com.sun.syndication.feed.synd.SyndEntry;
import com.sun.syndication.feed.synd.SyndFeed;
import com.sun.syndication.feed.synd.SyndPerson;
import com.sun.syndication.io.SyndFeedInput;

/**
 * 
 * @author dogacan
 * @author mattmann
 * @since NUTCH-444
 * 
 * <p>
 * A new RSS/ATOM Feed{@link Parser} that rapidly parses all referenced links
 * and content present in the feed.
 * </p>
 * 
 */
public class FeedParser implements Parser {

  public static final String CHARSET_UTF8 = "charset=UTF-8";

  public static final String TEXT_PLAIN_CONTENT_TYPE = "text/plain; "
      + CHARSET_UTF8;

  public static final Logger LOG = LoggerFactory
      .getLogger("org.apache.nutch.parse.feed");

  private Configuration conf;

  private ParserFactory parserFactory;

  private URLNormalizers normalizers;

  private URLFilters filters;

  private String defaultEncoding;

  /**
   * Parses the given feed and extracts out and parsers all linked items within
   * the feed, using the underlying ROME feed parsing library.
   * 
   * @param content
   *          A {@link Content} object representing the feed that is being
   *          parsed by this {@link Parser}.
   * 
   * @return A {@link ParseResult} containing all {@link Parse}d feeds that
   *         were present in the feed file that this {@link Parser} dealt with.
   * 
   */
  public ParseResult getParse(Content content) {
    SyndFeed feed = null;
    ParseResult parseResult = new ParseResult(content.getUrl());

    EncodingDetector detector = new EncodingDetector(conf);
    detector.autoDetectClues(content, true);
    String encoding = detector.guessEncoding(content, defaultEncoding);
    try {
      InputSource input = new InputSource(new ByteArrayInputStream(content
          .getContent()));
      input.setEncoding(encoding);
      SyndFeedInput feedInput = new SyndFeedInput();
      feed = feedInput.build(input);
    } catch (Exception e) {
      // return empty parse
      LOG.warn("Parse failed: url: " + content.getUrl() + ", exception: "
          + StringUtils.stringifyException(e));
      return new ParseStatus(e)
          .getEmptyParseResult(content.getUrl(), getConf());
    }

    List entries = feed.getEntries();
    String feedLink = feed.getLink();
    try {
      feedLink = normalizers.normalize(feedLink, URLNormalizers.SCOPE_OUTLINK);
      if (feedLink != null)
        feedLink = filters.filter(feedLink);
    } catch (Exception e) {
      feedLink = null;
    }

    for (Iterator i = entries.iterator(); i.hasNext();) {
      SyndEntry entry = (SyndEntry) i.next();
      addToMap(parseResult, feed, feedLink, entry, content);
    }

    String feedDesc = stripTags(feed.getDescriptionEx());
    String feedTitle = stripTags(feed.getTitleEx());

    parseResult.put(content.getUrl(), new ParseText(feedDesc), new ParseData(
        new ParseStatus(ParseStatus.SUCCESS), feedTitle, new Outlink[0],
        content.getMetadata()));

    return parseResult;
  }

  /**
   * 
   * Sets the {@link Configuration} object for this {@link Parser}. This
   * {@link Parser} expects the following configuration properties to be set:
   * 
   * <ul>
   * <li>URLNormalizers - properties in the configuration object to set up the
   * default url normalizers.</li>
   * <li>URLFilters - properties in the configuration object to set up the
   * default url filters.</li>
   * </ul>
   * 
   * @param conf
   *          The Hadoop {@link Configuration} object to use to configure this
   *          {@link Parser}.
   * 
   */
  public void setConf(Configuration conf) {
    this.conf = conf;
    this.parserFactory = new ParserFactory(conf);
    this.normalizers = new URLNormalizers(conf, URLNormalizers.SCOPE_OUTLINK);
    this.filters = new URLFilters(conf);
    this.defaultEncoding =
      conf.get("parser.character.encoding.default", "windows-1252");
  }

  /**
   * 
   * @return The {@link Configuration} object used to configure this
   *         {@link Parser}.
   */
  public Configuration getConf() {
    return this.conf;
  }

  /**
   * Runs a command line version of this {@link Parser}.
   * 
   * @param args
   *          A single argument (expected at arg[0]) representing a path on the
   *          local filesystem that points to a feed file.
   * 
   * @throws Exception
   *           If any error occurs.
   */
  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      System.err.println("Usage: FeedParser <feed>");
      System.exit(1);
    }
    String name = args[0];
    String url = "file:" + name;
    Configuration conf = NutchConfiguration.create();
    FeedParser parser = new FeedParser();
    parser.setConf(conf);
    File file = new File(name);
    byte[] bytes = new byte[(int) file.length()];
    DataInputStream in = new DataInputStream(new FileInputStream(file));
    in.readFully(bytes);
    ParseResult parseResult = parser.getParse(new Content(url, url, bytes,
        "application/rss+xml", new Metadata(), conf));
    for (Entry<Text, Parse> entry : parseResult) {
      System.out.println("key: " + entry.getKey());
      Parse parse = entry.getValue();
      System.out.println("data: " + parse.getData());
      System.out.println("text: " + parse.getText() + "\n");
    }
  }

  private void addToMap(ParseResult parseResult, SyndFeed feed,
      String feedLink, SyndEntry entry, Content content) {
    String link = entry.getLink(), text = null, title = null;
    Metadata parseMeta = new Metadata(), contentMeta = content.getMetadata();
    Parse parse = null;
    SyndContent description = entry.getDescription();

    try {
      link = normalizers.normalize(link, URLNormalizers.SCOPE_OUTLINK);

      if (link != null)
        link = filters.filter(link);
    } catch (Exception e) {
      e.printStackTrace();
      return;
    }

    if (link == null)
      return;

    title = stripTags(entry.getTitleEx());

    if (feedLink != null)
      parseMeta.set("feed", feedLink);

    addFields(parseMeta, contentMeta, feed, entry);

    // some item descriptions contain markup text in them,
    // so we temporarily set their content-type to parse them
    // with another plugin
    String contentType = contentMeta.get(Response.CONTENT_TYPE);

    if (description != null)
      text = description.getValue();

    if (text == null) {
      List contents = entry.getContents();
      StringBuilder buf = new StringBuilder();
      for (Iterator i = contents.iterator(); i.hasNext();) {
        SyndContent syndContent = (SyndContent) i.next();
        buf.append(syndContent.getValue());
      }
      text = buf.toString();
    }

    try {
      Parser parser = parserFactory.getParsers(contentType, link)[0];
      parse = parser.getParse(
          new Content(link, link, text.getBytes(), contentType, contentMeta,
              conf)).get(link);
    } catch (ParserNotFound e) { /* ignore */
    }

    if (parse != null) {
      ParseData data = parse.getData();
      data.getContentMeta().remove(Response.CONTENT_TYPE);
      mergeMetadata(data.getParseMeta(), parseMeta);
      parseResult.put(link, new ParseText(parse.getText()), new ParseData(
          ParseStatus.STATUS_SUCCESS, title, data.getOutlinks(), data
              .getContentMeta(), data.getParseMeta()));
    } else {
      contentMeta.remove(Response.CONTENT_TYPE);
      parseResult.put(link, new ParseText(text), new ParseData(
          ParseStatus.STATUS_FAILURE, title, new Outlink[0], contentMeta,
          parseMeta));
    }

  }

  private static String stripTags(SyndContent c) {
    if (c == null)
      return "";

    String value = c.getValue();

    String[] parts = value.split("<[^>]*>");
    StringBuffer buf = new StringBuffer();

    for (String part : parts)
      buf.append(part);

    return buf.toString().trim();
  }

  private void addFields(Metadata parseMeta, Metadata contentMeta,
      SyndFeed feed, SyndEntry entry) {
    List authors = entry.getAuthors(), categories = entry.getCategories();
    Date published = entry.getPublishedDate(), updated = entry.getUpdatedDate();
    String contentType = null;

    if (authors != null) {
      for (Object o : authors) {
        SyndPerson author = (SyndPerson) o;
        String authorName = author.getName();
        if (checkString(authorName)) {
          parseMeta.add(Feed.FEED_AUTHOR, authorName);
        }
      }
    } else {
      // getAuthors may return null if feed is non-atom
      // if so, call getAuthor to get Dublin Core module creator.
      String authorName = entry.getAuthor();
      if (checkString(authorName)) {
        parseMeta.set(Feed.FEED_AUTHOR, authorName);
      }
    }

    for (Iterator i = categories.iterator(); i.hasNext();) {
      parseMeta.add(Feed.FEED_TAGS, ((SyndCategory) i.next()).getName());
    }

    if (published != null) {
      parseMeta.set(Feed.FEED_PUBLISHED, Long.toString(published.getTime()));
    }
    if (updated != null) {
      parseMeta.set(Feed.FEED_UPDATED, Long.toString(updated.getTime()));
    }

    SyndContent description = entry.getDescription();
    if (description != null) {
      contentType = description.getType();
    } else {
      // TODO: What to do if contents.size() > 1?
      List contents = entry.getContents();
      if (contents.size() > 0) {
        contentType = ((SyndContent) contents.get(0)).getType();
      }
    }

    if (checkString(contentType)) {
      // ROME may return content-type as html
      if (contentType.equals("html"))
        contentType = "text/html";
      else if (contentType.equals("xhtml"))
        contentType = "text/xhtml";
      contentMeta.set(Response.CONTENT_TYPE, contentType + "; " + CHARSET_UTF8);
    } else {
      contentMeta.set(Response.CONTENT_TYPE, TEXT_PLAIN_CONTENT_TYPE);
    }

  }

  private void mergeMetadata(Metadata first, Metadata second) {
    for (String name : second.names()) {
      String[] values = second.getValues(name);
      for (String value : values) {
        first.add(name, value);
      }
    }
  }

  private boolean checkString(String s) {
    return s != null && !s.equals("");
  }

}
