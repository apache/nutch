/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.Perl5Matcher;
import org.apache.oro.text.regex.Perl5Pattern;
import org.apache.oro.text.regex.PatternMatcher;
import org.apache.oro.text.regex.MatchResult;
import org.apache.oro.text.regex.MalformedPatternException;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

import org.apache.nutch.net.protocols.HttpDateFormat;

import org.apache.nutch.parse.Parse;

import org.apache.nutch.indexer.IndexingFilter;
import org.apache.nutch.indexer.IndexingException;

import org.apache.nutch.fetcher.FetcherOutput;

import org.apache.nutch.util.NutchConf;

import org.apache.nutch.util.LogFormatter;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.activation.MimetypesFileTypeMap;
import javax.activation.MimeType;
import javax.activation.MimeTypeParseException;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import java.util.Date;
import java.util.TimeZone;
import java.util.Enumeration;
import java.util.Properties;

import java.io.InputStream;
import java.io.IOException;

/**
 * Add (or reset) a few metaData properties as respective fields
 * (if they are available), so that they can be displayed by more.jsp
 * (called by search.jsp).
 *
 * content-type is indexed to support query by type:
 * last-modifed is indexed to support query by date:
 *
 * Still need to make content-legnth searchable!
 *
 * @author John Xing
 */

public class MoreIndexingFilter implements IndexingFilter {
  public static final Logger LOG
    = LogFormatter.getLogger(MoreIndexingFilter.class.getName());

  // Filename extension to mime-type map.
  // Used by addType().
  static MimetypesFileTypeMap TYPE_MAP = null;
  static {
    try {
      // read mime types from config file
      InputStream is =
        NutchConf.get().getConfResourceAsInputStream
        (NutchConf.get().get("mime.types.file"));
      if (is == null) {
        LOG.warning
          ("no mime.types.file: content-type won't be indexed.");
        TYPE_MAP = null;
      } else {
        TYPE_MAP = new MimetypesFileTypeMap(is);
      }

      if (is != null)
        is.close();
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Unexpected error", e);
    }
  }

  public Document filter(Document doc, Parse parse, FetcherOutput fo)
    throws IndexingException {

    String url = fo.getUrl().toString();

    // normalize metaData (see note in the method below).
    Properties metaData = normalizeMeta(parse.getData().getMetadata());

    addTime(doc, metaData, url);

    addLength(doc, metaData, url);

    if (TYPE_MAP != null)
      addType(doc, metaData, url);

    resetTitle(doc, metaData, url);

    return doc;
  }
    
  // Add time related meta info, now Last-Modified only
  // Others for consideration: Date, Expires
  private Document addTime(Document doc, Properties metaData, String url) {

    String lastModified = metaData.getProperty("last-modified");
    if (lastModified == null)
      return doc;

    // try to figure out last-modified as long value
    DateFormat df = new SimpleDateFormat("EEE MMM dd HH:mm:ss yyyy zzz");
    long time = -1;
    try {
      time = HttpDateFormat.toLong(lastModified);
    } catch  (ParseException e) {
      // try to parse it as date in alternative format
      try {
        Date d = df.parse(lastModified);
        time = d.getTime();
      } catch (Exception e1) {
        LOG.warning(url+": can't parse erroneous last-modified: "+lastModified);
      }
    }

    if (time == -1) {
      // or instead set it to current time at indexing?
      //time = System.currentTimeMillis();
      // for now, we just do nothing
      return doc;
    }

    // store last-modified as string
    doc.add(Field.UnIndexed("lastModified", new Long(time).toString()));

    // add support for query syntax date: using last-modified
    // query filter is implemented in DateQueryFilter.java
    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
    sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
    String dateString = sdf.format(new Date(time));

    // un-stored, indexed and un-tokenized
    doc.add(new Field("date", dateString, false, true, false));

    return doc;
  }

  // Add Content-Length
  private Document addLength(Document doc, Properties metaData, String url) {
    String contentLength = metaData.getProperty("content-length");

    if (contentLength != null)
      doc.add(Field.UnIndexed("contentLength", contentLength));

    return doc;
  }

  // Add Content-Type and its primaryType and subType
  private Document addType(Document doc, Properties metaData, String url) {
    String contentType = metaData.getProperty("content-type");
    if (contentType == null)
      return doc;

    MimeType mimeType;
    try {
      mimeType = new MimeType(contentType);
    } catch (MimeTypeParseException e) {
      LOG.warning(url+": can't parse erroneous content-type: "+contentType);
      return doc;
    }

    String primaryType = mimeType.getPrimaryType();
    String subType = mimeType.getSubType();
    // leave this for future improvement
    //MimeTypeParameterList parameterList = mimeType.getParameters()

    // add contentType, primaryType and subType to field "type"
    // as un-stored, indexed and un-tokenized, so that search results
    // can be confined by contentType or its primaryType or its subType.
    // For example, if contentType is application/vnd.ms-powerpoint,
    // search can be done with one of the following qualifiers
    // type:application/vnd.ms-powerpoint
    // type:application
    // type:vnd.ms-powerpoint
    // all case insensitive.
    // The query filter is implemented in TypeQueryFilter.java
    doc.add(new Field("type", contentType, false, true, false));
    doc.add(new Field("type", primaryType, false, true, false));
    doc.add(new Field("type", subType, false, true, false));

    // add its primaryType and subType to respective fields
    // as stored, indexed and un-tokenized
    doc.add(new Field("primaryType", primaryType, true, true, false));
    doc.add(new Field("subType", subType, true, true, false));

    return doc;
  }

  // Reset title if we see non-standard HTTP header "Content-Disposition".
  // It's a good indication that content provider wants filename therein
  // be used as the title of this url.

  // Patterns used to extract filename from possible non-standard
  // HTTP header "Content-Disposition". Typically it looks like:
  // Content-Disposition: inline; filename="foo.ppt"
  private PatternMatcher matcher = new Perl5Matcher();
  static Perl5Pattern patterns[] = {null, null};
  static {
    Perl5Compiler compiler = new Perl5Compiler();
    try {
      // order here is important
      patterns[0] =
        (Perl5Pattern) compiler.compile("\\bfilename=['\"](.+)['\"]");
      patterns[1] =
        (Perl5Pattern) compiler.compile("\\bfilename=(\\S+)\\b");
    } catch (MalformedPatternException e) {
      // just ignore
    }
  }

  private Document resetTitle(Document doc, Properties metaData, String url) {
    String contentDisposition = metaData.getProperty("content-disposition");
    if (contentDisposition == null)
      return doc;

    MatchResult result;
    for (int i=0; i<patterns.length; i++) {
      if (matcher.contains(contentDisposition,patterns[i])) {
        result = matcher.getMatch();
        doc.add(Field.UnIndexed("title", result.group(1)));
        break;
      }
    }

    return doc;
  }

  // Meta info in nutch metaData are saved in raw form, i.e.,
  // whatever the fetcher sees. To facilitate further processing,
  // a "normalization" is necessary.
  // This includes fixing http server oddities, such as:
  // (*) non-uniform casing of header names
  // (*) empty header value
  // Note: the original metaData should be kept intact,
  // because there is a benefit to preserve whatever comes from server.
  private Properties normalizeMeta(Properties old) {
    Properties normalized = new Properties();

    for (Enumeration e = old.propertyNames(); e.hasMoreElements();) {
      String key = (String) e.nextElement();
      String value = old.getProperty(key).trim();
      // some http server sends out header with empty value! if so, skip it
      if (value == null || value.equals(""))
        continue;
      // convert key (but, not value) to lower-case
      normalized.setProperty(key.toLowerCase(),value);
    }

    return normalized;
  }

}
