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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import org.apache.nutch.protocol.Content;
import org.junit.Test;

public class TestLanguageDetector {

  private String[][] languageData = { // test data from https://tatoeba.org/
      { "zho", "我不知道應該說什麼才好。", "我只是不知道應該說什麼而已…… 我法语说得不够好！" }, //
      { "deu", "Ich finde keine Worte.",
          "Ich weiß einfach nicht, was ich sagen soll. Mein Französisch ist nicht gut genug." }, //
      { "eng", "I have no words.",
          "I just don't know what to say.<br/>I simply don't know what to tell... I don't speak French well enough!" }, //
      { "fra", "J'en perds mes mots.",
          "Je ne sais simplement pas quoi dire... Je ne parle pas assez bien français !" }, //
      { "jpn", "何と言ったら良いか分かりません。", "何と言ったらいいか・・・。 私はフランス語がそんなにきちんとは話せない。" }, //
      { "nld", "Ik heb er geen woorden voor.",
          "Ik weet gewoon niet wat ik moet zeggen..." }, //
      { "rus", "У меня нет слов.",
          "Просто не знаю, что и сказать... Я не говорю по-французски настолько хорошо." }, //
      { "spa", "No tengo palabras.",
          "Simplemente no sé qué decir... ¡No hablo francés lo suficientemente bien!" }, //
      { "vie", "Tôi hết lời để nói.", "Tôi không biết nên nói gì cả..." }, //
  };

  protected void testHtml(LanguageDetector detector,
      String language, String title, String body, Charset charset,
      boolean addCharsetHint, boolean addLanguageHint) {
    String headerStart = "<!DOCTYPE html>\n<html lang=\"\">\n<head>";
    String headerEnd = "\n</head><body>\n<p>";
    String footer = "</p>\n</body></html>";
    URI uri = URI.create("http://www.example.com/");

    Content content = new Content();
    content.setContentType("text/html");
    String doc = headerStart + "<title>" + title + "</title>";
    if (addLanguageHint) {
      doc = doc.replace("\"\"", "\"" + language + "\"");
    }
    if (addCharsetHint) {
      doc += "\n<meta charset=\"" + charset.name() + "\">";
    }
    doc += headerEnd + body + footer;
    content.setContent(doc.getBytes(charset));
    LanguageDetector.Result res = detector.detectLanguage(uri, content);
    assertEquals("charset detection failed for " + language, charset, res.charset);
    org.commoncrawl.langdetect.cld2.Result lr = res.languages;
    assertEquals("language detection failed for " + language, language, lr.getLanguageCodeISO639_3());
  }

  @Test
  public void testLanguageDetector() throws IOException, URISyntaxException {
    LanguageDetector langDetect = new LanguageDetector();
    langDetect.setBestEffort(true);
    for (String[] data : languageData) {
      testHtml(langDetect, data[0], data[1], data[2], StandardCharsets.UTF_8, true, true);
    }
  }

  @Test
  public void testCharsetDetector() throws IOException {
    LanguageDetector langDetect = new LanguageDetector();
    langDetect.setBestEffort(true);
    for (String[] data : languageData) {
      Charset charset = StandardCharsets.UTF_8;
      boolean needHint = false;
      switch (data[0]) {
      case "eng":
        charset = StandardCharsets.US_ASCII;
        needHint = true; // subset of UTF-8 or ISO-8859-*
        break;
      case "fra":
        charset = StandardCharsets.ISO_8859_1;
        break;
      case "deu":
      case "nld":
        charset = StandardCharsets.ISO_8859_1;
        break;
      case "spa":
        needHint = true; // subset of UTF-8 or ISO-8859-*
        charset = Charset.forName("x-MacRoman");
        break;
      case "rus":
        charset = Charset.forName("KOI8-R");
        break;
      case "jpn":
        charset = Charset.forName("SHIFT_JIS");
        break;
      case "zho":
        charset = Charset.forName("GB2312");
        needHint = true; // could be also GB18030
        break;
      }
      testHtml(langDetect, data[0], data[1], data[2], charset, needHint, true);
    }
  }
}
