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

package org.apache.nutch.util;

// JDK imports
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;

// Hadoop imports
import org.apache.hadoop.conf.Configuration;

// Tika imports
import org.apache.tika.Tika;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MimeType;
import org.apache.tika.mime.MimeTypeException;
import org.apache.tika.mime.MimeTypes;
import org.apache.tika.mime.MimeTypesFactory;

// Slf4j logging imports
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// imported for Javadoc
import org.apache.nutch.protocol.ProtocolOutput;

/**
 * @author mattmann
 * @since NUTCH-608
 * 
 *        <p>
 *        This is a facade class to insulate Nutch from its underlying Mime Type
 *        substrate library, <a href="http://incubator.apache.org/tika/">Apache
 *        Tika</a>. Any mime handling code should be placed in this utility
 *        class, and hidden from the Nutch classes that rely on it.
 *
 */
public final class MimeUtil {

  private static final String SEPARATOR = ";";

  /* our Tika mime type registry */
  private MimeTypes mimeTypes;

  /* the tika detectors */
  private Tika tika;

  /* whether or not magic should be employed or not */
  private boolean mimeMagic;

  /* our log stream */
  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  public MimeUtil(Configuration conf) {
    tika = new Tika();
    ObjectCache objectCache = ObjectCache.get(conf);
    MimeTypes mimeTypez = (MimeTypes) objectCache.getObject(MimeTypes.class
        .getName());
    if (mimeTypez == null) {
      try {
        String customMimeTypeFile = conf.get("mime.types.file");
        if (customMimeTypeFile != null
            && customMimeTypeFile.equals("") == false) {
          try {
            mimeTypez = MimeTypesFactory.create(conf
                .getConfResourceAsInputStream(customMimeTypeFile));
          } catch (Exception e) {
            LOG.error("Can't load mime.types.file : " + customMimeTypeFile
                + " using Tika's default");
          }
        }
        if (mimeTypez == null)
          mimeTypez = MimeTypes.getDefaultMimeTypes();
      } catch (Exception e) {
        LOG.error("Exception in MimeUtil " + e.getMessage());
        throw new RuntimeException(e);
      }
      objectCache.setObject(MimeTypes.class.getName(), mimeTypez);
    }

    this.mimeTypes = mimeTypez;
    this.mimeMagic = conf.getBoolean("mime.type.magic", true);
  }

  /**
   * Cleans a {@link MimeType} name by removing out the actual {@link MimeType},
   * from a string of the form:
   * 
   * <pre>
   *      &lt;primary type&gt;/&lt;sub type&gt; ; &lt; optional params
   * </pre>
   * 
   * @param origType
   *          The original mime type string to be cleaned.
   * @return The primary type, and subtype, concatenated, e.g., the actual mime
   *         type.
   */
  public static String cleanMimeType(String origType) {
    if (origType == null)
      return null;

    // take the origType and split it on ';'
    String[] tokenizedMimeType = origType.split(SEPARATOR);
    if (tokenizedMimeType.length > 1) {
      // there was a ';' in there, take the first value
      return tokenizedMimeType[0];
    } else {
      // there wasn't a ';', so just return the orig type
      return origType;
    }
  }

  /**
   * A facade interface to trying all the possible mime type resolution
   * strategies available within Tika. First, the mime type provided in
   * <code>typeName</code> is cleaned, with {@link #cleanMimeType(String)}. Then
   * the cleaned mime type is looked up in the underlying Tika {@link MimeTypes}
   * registry, by its cleaned name. If the {@link MimeType} is found, then that
   * mime type is used, otherwise URL resolution is used to try and determine
   * the mime type. However, if <code>mime.type.magic</code> is enabled in
   * {@link NutchConfiguration}, then mime type magic resolution is used to try
   * and obtain a better-than-the-default approximation of the {@link MimeType}.
   * 
   * @param typeName
   *          The original mime type, returned from a {@link ProtocolOutput}.
   * @param url
   *          The given @see url, that Nutch was trying to crawl.
   * @param data
   *          The byte data, returned from the crawl, if any.
   * @return The correctly, automatically guessed {@link MimeType} name.
   */
  public String autoResolveContentType(String typeName, String url, byte[] data) {
    String retType = null;
    MimeType type = null;
    String cleanedMimeType = null;

    cleanedMimeType = MimeUtil.cleanMimeType(typeName);
    // first try to get the type from the cleaned type name
    if (cleanedMimeType != null) {
      try {
        type = mimeTypes.forName(cleanedMimeType);
        cleanedMimeType = type.getName();
      } catch (MimeTypeException mte) {
        // Seems to be a malformed mime type name...
        cleanedMimeType = null;
      }
    }

    // if returned null, or if it's the default type then try url resolution
    if (type == null
        || (type != null && type.getName().equals(MimeTypes.OCTET_STREAM))) {
      // If no mime-type header, or cannot find a corresponding registered
      // mime-type, then guess a mime-type from the url pattern

      try {
        retType = tika.detect(url) != null ? tika.detect(url) : null;
      } catch (Exception e) {
        String message = "Problem loading default Tika configuration";
        LOG.error(message, e);
        throw new RuntimeException(e);
      }
    } else {
      retType = type.getName();
    }

    // if magic is enabled use mime magic to guess if the mime type returned
    // from the magic guess is different than the one that's already set so far
    // if it is, and it's not the default mime type, then go with the mime type
    // returned by the magic
    if (this.mimeMagic) {
      String magicType = null;
      // pass URL (file name) and (cleansed) content type from protocol to Tika
      Metadata tikaMeta = new Metadata();
      tikaMeta.add(Metadata.RESOURCE_NAME_KEY, url);
      tikaMeta.add(Metadata.CONTENT_TYPE,
          (cleanedMimeType != null ? cleanedMimeType : typeName));
      try {
        InputStream stream = TikaInputStream.get(data);
        try {
          magicType = tika.detect(stream, tikaMeta);
        } finally {
          stream.close();
        }
      } catch (IOException ignore) {
      }

      if (magicType != null && !magicType.equals(MimeTypes.OCTET_STREAM)
          && !magicType.equals(MimeTypes.PLAIN_TEXT) && retType != null
          && !retType.equals(magicType)) {

        // If magic enabled and the current mime type differs from that of the
        // one returned from the magic, take the magic mimeType
        retType = magicType;
      }

      // if type is STILL null after all the resolution strategies, go for the
      // default type
      if (retType == null) {
        try {
          retType = MimeTypes.OCTET_STREAM;
        } catch (Exception ignore) {
        }
      }
    }

    return retType;
  }

  /**
   * Facade interface to Tika's underlying {@link MimeTypes#getMimeType(String)}
   * method.
   * 
   * @param url
   *          A string representation of the document. URL to sense the
   *          {@link MimeType} for.
   * @return An appropriate {@link MimeType}, identified from the given Document
   *         url in string form.
   */
  public String getMimeType(String url) {
    return tika.detect(url);
  }

  /**
   * A facade interface to Tika's underlying {@link MimeTypes#forName(String)}
   * method.
   * 
   * @param name
   *          The name of a valid {@link MimeType} in the Tika mime registry.
   * @return The object representation of the {@link MimeType}, if it exists, or
   *         null otherwise.
   */
  public String forName(String name) {
    try {
      return this.mimeTypes.forName(name).toString();
    } catch (MimeTypeException e) {
      LOG.error("Exception getting mime type by name: [" + name
          + "]: Message: " + e.getMessage());
      return null;
    }
  }

  /**
   * Facade interface to Tika's underlying {@link MimeTypes#getMimeType(File)}
   * method.
   * 
   * @param f
   *          The {@link File} to sense the {@link MimeType} for.
   * @return The {@link MimeType} of the given {@link File}, or null if it
   *         cannot be determined.
   */
  public String getMimeType(File f) {
    try {
      return tika.detect(f);
    } catch (Exception e) {
      LOG.error("Exception getting mime type for file: [" + f.getPath()
          + "]: Message: " + e.getMessage());
      return null;
    }
  }
}
