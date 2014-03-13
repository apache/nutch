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

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.avro.util.Utf8;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.storage.WebPage.Field;
import org.apache.nutch.util.Bytes;
import org.w3c.dom.DocumentFragment;

/**
 * Parse HTML meta tags (keywords, description) and store them in the parse
 * metadata so that they can be indexed with the index-metadata plugin with the
 * prefix 'metatag.'
 ***/

public class MetaTagsParser implements ParseFilter {

  private static final Log LOG = LogFactory.getLog(MetaTagsParser.class
      .getName());

  private Configuration conf;

  public static final String PARSE_META_PREFIX = "meta_";

  private Set<String> metatagset = new HashSet<String>();

  public void setConf(Configuration conf) {
    this.conf = conf;
    // specify whether we want a specific subset of metadata
    // by default take everything we can find
    String metatags = conf.get("metatags.names", "*");
    String[] values = metatags.split(";");
    for (String val : values)
      metatagset.add(val.toLowerCase());
    if(metatagset.size()==0){
      metatagset.add("*");
    }
  }

  public Configuration getConf() {
    return this.conf;
  }

  public Parse filter(String url, WebPage page, Parse parse,
      HTMLMetaTags metaTags, DocumentFragment doc) {

    Map<Utf8, ByteBuffer> metadata = new HashMap<Utf8, ByteBuffer>();

    // check in the metadata first : the tika-parser
    // might have stored the values there already
    Iterator<Entry<Utf8, ByteBuffer>> iterator = page.getMetadata().entrySet()
        .iterator();
    while (iterator.hasNext()) {
      Entry<Utf8, ByteBuffer> entry = iterator.next();
      String mdName = entry.getKey().toString();
      String value = Bytes.toStringBinary(entry.getValue());
      if (metatagset.contains("*") || metatagset.contains(mdName.toLowerCase())) {
        // now add the metadata
        LOG.debug("Found meta tag: '" + mdName + "', with value: '" + value
            + "'");
        metadata.put(new Utf8(PARSE_META_PREFIX + mdName.toLowerCase()),
            ByteBuffer.wrap(value.getBytes()));
      }
    }
    Iterator<Entry<Utf8, ByteBuffer>> itm = metadata.entrySet().iterator();
    while (iterator.hasNext()) {
      Entry<Utf8, ByteBuffer> entry = itm.next();
      page.putToMetadata(entry.getKey(), entry.getValue());
    }

    Properties generalMetaTags = metaTags.getGeneralTags();
    Iterator<Object> it = generalMetaTags.keySet().iterator();
    while (it.hasNext()) {
      StringBuilder sb = new StringBuilder();
      String name = (String) it.next();
      String[] values = new String[] { (String) generalMetaTags.get(name) };
      // The multivalues of a metadata field are saved with a separator '\t'
      // in the storage
      // unless there is only one entry, where no \t is appended.
      for (String value : values) {
        if (values.length > 1) {
          sb.append(value).append("\t");
        } else {
          sb.append(value);
        }
      }
      // check whether the name is in the list of what we want or if
      // specified *
      if (metatagset.contains("*") || metatagset.contains(name.toLowerCase())) {
        // Add the recently parsed value of multiValued array to metadata
        LOG.debug("Found meta tag : " + name + "\t" + sb.toString());
        page.putToMetadata(new Utf8(PARSE_META_PREFIX + name.toLowerCase()),
            ByteBuffer.wrap(Bytes.toBytes(sb.toString())));
      }
    }

    Properties httpequiv = metaTags.getHttpEquivTags();
    Enumeration<?> tagNames = httpequiv.propertyNames();
    while (tagNames.hasMoreElements()) {
      String name = (String) tagNames.nextElement();
      String value = httpequiv.getProperty(name);
      // check whether the name is in the list of what we want or if
      // specified *
      if (metatagset.contains("*") || metatagset.contains(name.toLowerCase())) {
        LOG.debug("Found meta tag : " + name + "\t" + value);
        page.putToMetadata(new Utf8(PARSE_META_PREFIX + name.toLowerCase()),
            ByteBuffer.wrap(value.getBytes()));
      }
    }

    return parse;
  }

  @Override
  public Collection<Field> getFields() {
    return null;
  }

}
