/*
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
package org.commoncrawl.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.Set;

import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.protocol.Content;
import org.apache.tika.detect.AutoDetectReader;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.commoncrawl.langdetect.cld2.CLDHints;
import org.commoncrawl.langdetect.cld2.Cld2;
import org.commoncrawl.langdetect.cld2.Flags;
import org.commoncrawl.util.LanguageDetector.Result.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LanguageDetector {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  protected static Set<String> SUPPORTED_CONTENT_TYPES = new HashSet<>();
  static {
    SUPPORTED_CONTENT_TYPES.add("text/html");
    SUPPORTED_CONTENT_TYPES.add("application/xhtml+xml");
  }

  protected Flags flags = new Flags();

  public void setBestEffort(boolean bestEffort) {
    flags.setBestEffort(bestEffort);
  }

  public static class Result {
    Charset charset;
    org.commoncrawl.langdetect.cld2.Result languages;
    String errorReason;
    enum Status {
      EMPTY_CONTENT("empty content"),
      CHARSET_DETECTION_FAILED("failed to detect charset"),
      UNSUPPORTED_MIME_TYPE("MIME type not supported");
      String name;
      Status(String name) {
        this.name = name;
      }
    };
    Status errorStatus;
  }

  protected Result detectLanguage(URI uri, Content content) {

    LanguageDetector.Result result = new Result();

    if (content.getContent().length == 0) {
      // empty content, nothing to detect
      LOG.debug("Skipping empty document for language and charset detection");
      result.errorStatus = Status.EMPTY_CONTENT;
      return result;
    }

    String detectedContentType = content.getContentType();
    boolean isPlainText = false;
    if (!SUPPORTED_CONTENT_TYPES.contains(detectedContentType)) {
      // TODO: as an improvement, parse documents of non-HTML content types and
      // do the language detection on extracted text, for now skip them
      LOG.debug("Skipping document of Content-Type {} for language detection",
          detectedContentType);
      result.errorReason = "Content-Type " + detectedContentType + " not supported";
      result.errorStatus = Status.UNSUPPORTED_MIME_TYPE;
      return result;
    }

    String httpContentLanguage = content.getMetadata()
        .get(Response.CONTENT_LANGUAGE);
    String httpContentType = content.getMetadata().get(Response.CONTENT_TYPE);

    Metadata metadata = new Metadata();
    if (httpContentType != null) {
      LOG.info("  Content-Type: {}", httpContentType);
      metadata.add(Metadata.CONTENT_TYPE, httpContentType);
    }
    String text;
    byte[] bytes = content.getContent();
    try (AutoDetectReader charsetDetectReader = new AutoDetectReader(
        new ByteArrayInputStream(bytes), metadata);) {
      result.charset = charsetDetectReader.getCharset();
      LOG.debug("Recoding from {}: {}", result.charset, uri);
      // NOTE: need also recode from UTF-8 to make sure it's valid
      text = new String(bytes, result.charset);
      bytes = Cld2.encodeNative(text);
    } catch (IOException | TikaException e) {
      LOG.error("Failed to convert charset:", e);
      result.errorReason = "Failed to convert charset " + e.getMessage();
      result.errorStatus = Status.CHARSET_DETECTION_FAILED;
      return result;
    }
    CLDHints hints = new CLDHints();
    hints.setEncodingHint(result.charset);
    hints.setTopLevelDomainHint(uri);
    if (httpContentLanguage != null) {
      hints.setContentLanguageHint(httpContentLanguage);
    }
    result.languages = Cld2.detect(bytes, hints, flags, isPlainText);
    result.languages.configurePruning(10, 2, 0.0);

    return result;
  }

}
