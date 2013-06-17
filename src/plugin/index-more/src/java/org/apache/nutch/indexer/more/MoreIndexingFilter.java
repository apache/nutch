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
package org.apache.nutch.indexer.more;

import java.text.ParseException;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;

import org.apache.avro.util.Utf8;
import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.IndexingFilter;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.metadata.HttpHeaders;
import org.apache.nutch.net.protocols.HttpDateFormat;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.storage.WebPage.Field;
import org.apache.nutch.util.MimeUtil;
import org.apache.oro.text.regex.MalformedPatternException;
import org.apache.oro.text.regex.MatchResult;
import org.apache.oro.text.regex.PatternMatcher;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.Perl5Matcher;
import org.apache.oro.text.regex.Perl5Pattern;
import org.apache.solr.common.util.DateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Add (or reset) a few metaData properties as respective fields (if they are
 * available), so that they can be accurately used within the search index.
 * 
 * 'lastModifed' is indexed to support query by date, 'contentLength' obtains content length from the HTTP
 * header, 'type' field is indexed to support query by type and finally the 'title' field is an attempt 
 * to reset the title if a content-disposition hint exists. The logic is that such a presence is indicative 
 * that the content provider wants the filename therein to be used as the title.
 * 
 * Still need to make content-length searchable!
 * 
 * @author John Xing
 */

public class MoreIndexingFilter implements IndexingFilter {
  public static final Logger LOG = LoggerFactory.getLogger(MoreIndexingFilter.class);

  /** Get the MimeTypes resolver instance. */
  private MimeUtil MIME;

  private static Collection<WebPage.Field> FIELDS = new HashSet<WebPage.Field>();

  static {
    FIELDS.add(WebPage.Field.HEADERS);
    FIELDS.add(WebPage.Field.CONTENT_TYPE);
    FIELDS.add(WebPage.Field.MODIFIED_TIME);
  }

  @Override
  public NutchDocument filter(NutchDocument doc, String url, WebPage page)
      throws IndexingException {
    addTime(doc, page, url);
    addLength(doc, page, url);
    addType(doc, page, url);
    resetTitle(doc, page, url);
    return doc;
  }

  // Add time related meta info. Add last-modified if present. Index date as
  // last-modified, or, if that's not present, use fetch time.
  private NutchDocument addTime(NutchDocument doc, WebPage page, String url) {
    long time = -1;
    Utf8 lastModified = page
        .getFromHeaders(new Utf8(HttpHeaders.LAST_MODIFIED));
    // String lastModified = data.getMeta(Metadata.LAST_MODIFIED);
    if (lastModified != null) { // try parse last-modified
      time = getTime(lastModified.toString(), url); // use as time
      String formlastModified = DateUtil.getThreadLocalDateFormat().format(new Date(time));
      // store as string
      doc.add("lastModified", formlastModified);
    }

    if (time == -1) { // if no last-modified
      time = page.getModifiedTime(); // use Modified time
    }

    String dateString = DateUtil.getThreadLocalDateFormat().format(new Date(time));

    // un-stored, indexed and un-tokenized
    doc.add("date", dateString);

    return doc;
  }

  private long getTime(String date, String url) {
    long time = -1;
    try {
      time = HttpDateFormat.toLong(date);
    } catch (ParseException e) {
      // try to parse it as date in alternative format
      try {
        Date parsedDate = DateUtils.parseDate(date, new String[] {
            "EEE MMM dd HH:mm:ss yyyy", "EEE MMM dd HH:mm:ss yyyy zzz",
            "EEE MMM dd HH:mm:ss zzz yyyy", "EEE, dd MMM yyyy HH:mm:ss zzz",
            "EEE,dd MMM yyyy HH:mm:ss zzz", "EEE, dd MMM yyyy HH:mm:sszzz",
            "EEE, dd MMM yyyy HH:mm:ss", "EEE, dd-MMM-yy HH:mm:ss zzz",
            "yyyy/MM/dd HH:mm:ss.SSS zzz", "yyyy/MM/dd HH:mm:ss.SSS",
            "yyyy/MM/dd HH:mm:ss zzz", "yyyy/MM/dd", "yyyy.MM.dd HH:mm:ss",
            "yyyy-MM-dd HH:mm", "MMM dd yyyy HH:mm:ss. zzz",
            "MMM dd yyyy HH:mm:ss zzz", "dd.MM.yyyy HH:mm:ss zzz",
            "dd MM yyyy HH:mm:ss zzz", "dd.MM.yyyy; HH:mm:ss",
            "dd.MM.yyyy HH:mm:ss", "dd.MM.yyyy zzz", "yyyy-MM-dd'T'HH:mm:ss'Z'" });
        time = parsedDate.getTime();
        // if (LOG.isWarnEnabled()) {
        // LOG.warn(url + ": parsed date: " + date +" to:"+time);
        // }
      } catch (Exception e2) {
        if (LOG.isWarnEnabled()) {
          LOG.warn(url + ": can't parse erroneous date: " + date);
        }
      }
    }
    return time;
  }

  // Add Content-Length
  private NutchDocument addLength(NutchDocument doc, WebPage page, String url) {
    Utf8 contentLength = page.getFromHeaders(new Utf8(
        HttpHeaders.CONTENT_LENGTH));
    if (contentLength != null) {
      // NUTCH-1010 ContentLength not trimmed
      String trimmed = contentLength.toString().trim();
      if (!trimmed.isEmpty())
        doc.add("contentLength", trimmed);
    }

    return doc;
  }

  /**
   * <p>
   * Add Content-Type and its primaryType and subType add contentType,
   * primaryType and subType to field "type" as un-stored, indexed and
   * un-tokenized, so that search results can be confined by contentType or its
   * primaryType or its subType.
   * </p>
   * <p>
   * For example, if contentType is application/vnd.ms-powerpoint, search can be
   * done with one of the following qualifiers
   * type:application/vnd.ms-powerpoint type:application type:vnd.ms-powerpoint
   * all case insensitive. The query filter is implemented in
   * {@link TypeQueryFilter}.
   * </p>
   * 
   * @param doc
   * @param data
   * @param url
   * @return
   */
  private NutchDocument addType(NutchDocument doc, WebPage page, String url) {
    String mimeType = null;
    Utf8 contentType = page.getContentType();
    if (contentType == null)
    	contentType = page.getFromHeaders(new Utf8(HttpHeaders.CONTENT_TYPE));
    if (contentType == null) {
      // Note by Jerome Charron on 20050415:
      // Content Type not solved by a previous plugin
      // Or unable to solve it... Trying to find it
      // Should be better to use the doc content too
      // (using MimeTypes.getMimeType(byte[], String), but I don't know
      // which field it is?
      // if (MAGIC) {
      // contentType = MIME.getMimeType(url, content);
      // } else {
      // contentType = MIME.getMimeType(url);
      // }
      mimeType = MIME.getMimeType(url);
    } else {
      mimeType = MIME.forName(MimeUtil.cleanMimeType(contentType.toString()));
    }

    // Checks if we solved the content-type.
    if (mimeType == null) {
      return doc;
    }

    doc.add("type", mimeType);

    // Check if we need to split the content type in sub parts
    if (conf.getBoolean("moreIndexingFilter.indexMimeTypeParts", true)) {
      String[] parts = getParts(mimeType);

      for(String part: parts) {
        doc.add("type", part);
      }
    }

    // leave this for future improvement
    // MimeTypeParameterList parameterList = mimeType.getParameters()

    return doc;
  }

  /**
   * Utility method for splitting mime type into type and subtype.
   * 
   * @param mimeType
   * @return
   */
  static String[] getParts(String mimeType) {
    return mimeType.split("/");
  }

  // Reset title if we see non-standard HTTP header "Content-Disposition".
  // It's a good indication that content provider wants filename therein
  // be used as the title of this url.

  // Patterns used to extract filename from possible non-standard
  // HTTP header "Content-Disposition". Typically it looks like:
  // Content-Disposition: inline; filename="foo.ppt"
  private PatternMatcher matcher = new Perl5Matcher();

  private Configuration conf;
  static Perl5Pattern patterns[] = { null, null };
  static {
    Perl5Compiler compiler = new Perl5Compiler();
    try {
      // order here is important
      patterns[0] = (Perl5Pattern) compiler
          .compile("\\bfilename=['\"](.+)['\"]");
      patterns[1] = (Perl5Pattern) compiler.compile("\\bfilename=(\\S+)\\b");
    } catch (MalformedPatternException e) {
      // just ignore
    }
  }

  private NutchDocument resetTitle(NutchDocument doc, WebPage page, String url) {
    Utf8 contentDisposition = page.getFromHeaders(new Utf8(
        HttpHeaders.CONTENT_DISPOSITION));
    if (contentDisposition == null)
      return doc;

    MatchResult result;
    for (int i = 0; i < patterns.length; i++) {
      if (matcher.contains(contentDisposition.toString(), patterns[i])) {
        result = matcher.getMatch();
        doc.add("title", result.group(1));
        break;
      }
    }

    return doc;
  }

  public void addIndexBackendOptions(Configuration conf) {
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
    MIME = new MimeUtil(conf);
  }

  public Configuration getConf() {
    return this.conf;
  }

  @Override
  public Collection<Field> getFields() {
    return FIELDS;
  }

}
