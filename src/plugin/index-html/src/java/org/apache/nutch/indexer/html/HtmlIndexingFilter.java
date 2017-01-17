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

import java.lang.invoke.MethodHandles;
import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashSet;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.IndexingFilter;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.storage.WebPage.Field;
import org.apache.nutch.util.MimeUtil;
import org.apache.nutch.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Add raw HTML content of a document to the index.
 */

public class HtmlIndexingFilter implements IndexingFilter {
    private static final Logger LOG = LoggerFactory
        .getLogger(MethodHandles.lookup().lookupClass());
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
            Scanner scanner = new Scanner(arrayInputStream, StandardCharsets.UTF_8.name());
            scanner.useDelimiter("\\Z");//To read all scanner content in one String
            String data = "";
            if (scanner.hasNext()) {
                data = scanner.next();
            }
            doc.add("rawcontent", StringUtil.cleanField(data));
            scanner.close();
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
