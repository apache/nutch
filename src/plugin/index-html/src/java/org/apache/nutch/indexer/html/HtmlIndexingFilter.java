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
package org.apache.nutch.indexer.html;

import java.util.Scanner;
import java.nio.ByteBuffer;
import java.io.ByteArrayInputStream;

import java.text.ParseException;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;

import org.apache.avro.util.Utf8;
import org.apache.commons.lang.StringUtils;
import org.apache.nutch.util.StringUtil;

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
import org.apache.nutch.util.TableUtil;
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
 * Add HTML of page the document element so it can be indexed in scheme.xml
 *
 * @author Mohamed Meabed <mo.meabed@gmail.com>
 */

public class HtmlIndexingFilter implements IndexingFilter {
    public static final Logger LOG = LoggerFactory.getLogger(HtmlIndexingFilter.class);
    private Configuration conf;

    /**
     * Get the MimeTypes resolver instance.
     */
    private MimeUtil MIME;

    private static Collection<WebPage.Field> FIELDS = new HashSet<WebPage.Field>();

    static {
        FIELDS.add(WebPage.Field.CONTENT);
    }

    @Override
    public NutchDocument filter(NutchDocument doc, String url, WebPage page) throws IndexingException {
        addRawContent(doc, page, url);
        return doc;
    }


    private NutchDocument addRawContent(NutchDocument doc, WebPage page, String url) {
        ByteBuffer raw = page.getContent();
        if (raw != null) {
            if (LOG.isInfoEnabled()) {
                LOG.info("Html indexing for: " + url.toString());
            }
            ByteArrayInputStream arrayInputStream = new ByteArrayInputStream(raw.array(), raw.arrayOffset() + raw.position(), raw.remaining());
            Scanner scanner = new Scanner(arrayInputStream);
            scanner.useDelimiter("\\Z");//To read all scanner content in one String
            String data = "";
            if (scanner.hasNext()) {
                data = scanner.next();
            }
            doc.add("rawcontent", StringUtil.cleanField(data));
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
