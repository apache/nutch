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


import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.Perl5Matcher;
import org.apache.oro.text.regex.Perl5Pattern;
import org.apache.oro.text.regex.PatternMatcher;
import org.apache.oro.text.regex.MatchResult;
import org.apache.oro.text.regex.MalformedPatternException;
import org.apache.tika.mime.MimeType;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

import org.apache.nutch.metadata.Metadata;

import org.apache.nutch.net.protocols.HttpDateFormat;
import org.apache.nutch.net.protocols.Response;

import org.apache.nutch.parse.Parse;

import org.apache.nutch.indexer.IndexingFilter;
import org.apache.nutch.indexer.IndexingException;

import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.util.MimeUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

import java.text.ParseException;
import java.text.SimpleDateFormat;

import java.util.Date;
import java.util.TimeZone;

import org.apache.commons.lang.time.DateUtils;


/**
 * Add (or reset) a few metaData properties as respective fields
 * (if they are available), so that they can be displayed by more.jsp
 * (called by search.jsp).
 *
 * content-type is indexed to support query by type:
 * last-modifed is indexed to support query by date:
 *
 * Still need to make content-length searchable!
 *
 * @author John Xing
 */

public class MoreIndexingFilter implements IndexingFilter {
  public static final Log LOG = LogFactory.getLog(MoreIndexingFilter.class);

  /** A flag that tells if magic resolution must be performed */
  private boolean MAGIC;

  /** Get the MimeTypes resolver instance. */
  private MimeUtil MIME; 
  
  public Document filter(Document doc, Parse parse, Text url, CrawlDatum datum, Inlinks inlinks)
    throws IndexingException {

    String url_s = url.toString();

    addTime(doc, parse.getData(), url_s, datum);
    addLength(doc, parse.getData(), url_s);
    addType(doc, parse.getData(), url_s);
    resetTitle(doc, parse.getData(), url_s);

    return doc;
  }
    
  // Add time related meta info.  Add last-modified if present.  Index date as
  // last-modified, or, if that's not present, use fetch time.
  private Document addTime(Document doc, ParseData data,
                           String url, CrawlDatum datum) {
    long time = -1;

    String lastModified = data.getMeta(Metadata.LAST_MODIFIED);
    if (lastModified != null) {                   // try parse last-modified
      time = getTime(lastModified,url);           // use as time
                                                  // store as string
      doc.add(new Field("lastModified", new Long(time).toString(), Field.Store.YES, Field.Index.NO));
    }

    if (time == -1) {                             // if no last-modified
      time = datum.getFetchTime();                // use fetch time
    }

    // add support for query syntax date:
    // query filter is implemented in DateQueryFilter.java
    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
    sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
    String dateString = sdf.format(new Date(time));

    // un-stored, indexed and un-tokenized
    doc.add(new Field("date", dateString, Field.Store.NO, Field.Index.UN_TOKENIZED));

    return doc;
  }

  private long getTime(String date, String url) {
    long time = -1;
    try {
      time = HttpDateFormat.toLong(date);
    } catch (ParseException e) {
	// try to parse it as date in alternative format
	try {
	    Date parsedDate = DateUtils.parseDate(date,
		  new String [] {
		      "EEE MMM dd HH:mm:ss yyyy",
		      "EEE MMM dd HH:mm:ss yyyy zzz",
		      "EEE, MMM dd HH:mm:ss yyyy zzz",
		      "EEE, dd MMM yyyy HH:mm:ss zzz",
		      "EEE,dd MMM yyyy HH:mm:ss zzz",
		      "EEE, dd MMM yyyy HH:mm:sszzz",
		      "EEE, dd MMM yyyy HH:mm:ss",
		      "EEE, dd-MMM-yy HH:mm:ss zzz",
		      "yyyy/MM/dd HH:mm:ss.SSS zzz",
		      "yyyy/MM/dd HH:mm:ss.SSS",
		      "yyyy/MM/dd HH:mm:ss zzz",
		      "yyyy/MM/dd",
		      "yyyy.MM.dd HH:mm:ss",
		      "yyyy-MM-dd HH:mm",
		      "MMM dd yyyy HH:mm:ss. zzz",
		      "MMM dd yyyy HH:mm:ss zzz",
		      "dd.MM.yyyy HH:mm:ss zzz",
		      "dd MM yyyy HH:mm:ss zzz",
		      "dd.MM.yyyy; HH:mm:ss",
		      "dd.MM.yyyy HH:mm:ss",
		      "dd.MM.yyyy zzz"
		  });
	    time = parsedDate.getTime();
            // if (LOG.isWarnEnabled()) {
	    //   LOG.warn(url + ": parsed date: " + date +" to:"+time);
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
  private Document addLength(Document doc, ParseData data, String url) {
    String contentLength = data.getMeta(Response.CONTENT_LENGTH);

    if (contentLength != null)
      doc.add(new Field("contentLength", contentLength, Field.Store.YES, Field.Index.NO));

    return doc;
  }

  // Add Content-Type and its primaryType and subType
  private Document addType(Document doc, ParseData data, String url) {
    MimeType mimeType = null;
    String contentType = data.getMeta(Response.CONTENT_TYPE);
    if (contentType == null) {
	// Note by Jerome Charron on 20050415:
        // Content Type not solved by a previous plugin
        // Or unable to solve it... Trying to find it
        // Should be better to use the doc content too
        // (using MimeTypes.getMimeType(byte[], String), but I don't know
        // which field it is?
        // if (MAGIC) {
        //   contentType = MIME.getMimeType(url, content);
        // } else {
        //   contentType = MIME.getMimeType(url);
        // }
        mimeType = MIME.getMimeType(url);
    } else {
            mimeType = MIME.forName(contentType);
    }
        
    // Checks if we solved the content-type.
    if (mimeType == null) {
      return doc;
    }

    contentType = mimeType.getName();
    String primaryType = mimeType.getSuperType().getName();
    String subType = mimeType.getSubTypes().first().getName();
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
    doc.add(new Field("type", contentType, Field.Store.NO, Field.Index.UN_TOKENIZED));
    doc.add(new Field("type", primaryType, Field.Store.NO, Field.Index.UN_TOKENIZED));
    doc.add(new Field("type", subType, Field.Store.NO, Field.Index.UN_TOKENIZED));

    // add its primaryType and subType to respective fields
    // as stored, indexed and un-tokenized
    doc.add(new Field("primaryType", primaryType, Field.Store.YES, Field.Index.UN_TOKENIZED));
    doc.add(new Field("subType", subType, Field.Store.YES, Field.Index.UN_TOKENIZED));

    return doc;
  }

  // Reset title if we see non-standard HTTP header "Content-Disposition".
  // It's a good indication that content provider wants filename therein
  // be used as the title of this url.

  // Patterns used to extract filename from possible non-standard
  // HTTP header "Content-Disposition". Typically it looks like:
  // Content-Disposition: inline; filename="foo.ppt"
  private PatternMatcher matcher = new Perl5Matcher();

  private Configuration conf;
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

  private Document resetTitle(Document doc, ParseData data, String url) {
    String contentDisposition = data.getMeta(Metadata.CONTENT_DISPOSITION);
    if (contentDisposition == null)
      return doc;

    MatchResult result;
    for (int i=0; i<patterns.length; i++) {
      if (matcher.contains(contentDisposition,patterns[i])) {
        result = matcher.getMatch();
        doc.add(new Field("title", result.group(1), Field.Store.YES, Field.Index.NO));
        break;
      }
    }

    return doc;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
    MIME = new MimeUtil(conf);
  }

  public Configuration getConf() {
    return this.conf;
  }

}
