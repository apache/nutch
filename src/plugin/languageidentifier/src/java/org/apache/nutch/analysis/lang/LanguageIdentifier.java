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
package org.apache.nutch.analysis.lang;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.Vector;
import java.util.logging.Logger;

import org.apache.nutch.fetcher.FetcherOutput;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.IndexingFilter;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseException;
import org.apache.nutch.parse.Parser;
import org.apache.nutch.parse.ParserFactory;
import org.apache.nutch.parse.ParserNotFound;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.Protocol;
import org.apache.nutch.protocol.ProtocolException;
import org.apache.nutch.protocol.ProtocolFactory;
import org.apache.nutch.protocol.ProtocolNotFound;
import org.apache.nutch.util.LogFormatter;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import java.util.Properties;
import java.util.Enumeration;

/**
 * 
 * @author Sami Siren
 *  
 */
public class LanguageIdentifier implements IndexingFilter {
  public static final Logger LOG = LogFormatter.getLogger("org.apache.nutch.analysis.lang.LanguageIdentifier");

  private Vector languages = new Vector();

  private Vector supportedLanguages = new Vector();

  private static LanguageIdentifier identifier = new LanguageIdentifier(true);

  private static float SCORE_THRESOLD = 0.00F;

  //public constructor needed for extension mechanism
  public LanguageIdentifier() {}

  private LanguageIdentifier(boolean fake) {
    Properties p = new Properties();
    try {
      p.load(this.getClass().getResourceAsStream("langmappings.properties"));

      Enumeration alllanguages = p.keys();

      StringBuffer list = new StringBuffer("Language identifier plugin supports:");
      while (alllanguages.hasMoreElements()) {
        String lang = (String) (alllanguages.nextElement());

        InputStream is = this.getClass().getClassLoader().getResourceAsStream(
                "org/apache/nutch/analysis/lang/" + lang + "." + NGramProfile.NGRAM_FILE_EXTENSION);

        if (is != null) {
          NGramProfile profile = new NGramProfile(lang);
          try {
            profile.load(is);
            languages.add(profile);
            supportedLanguages.add(lang);
            list.append(" " + lang);
            is.close();
          } catch (IOException e1) {
            LOG.severe(e1.toString());
          }
        }
      }
      LOG.info(list.toString());
    } catch (Exception e) {
      LOG.severe(e.toString());
    }
  }

  /**
   * return handle to singleton instance
   */
  public static LanguageIdentifier getInstance() {
    return identifier;
  }

  /**
   * main method used for testing
   * 
   * @param args
   */
  public static void main(String args[]) {

    String usage = "Usage: LanguageIdentifier [-identifyrows filename maxlines] [-identifyfile filename] [-identifyfileset files] [-identifytext text] [-identifyurl url]";
    int command = 0;

    final int IDFILE = 1;
    final int IDTEXT = 2;
    final int IDURL = 3;
    final int IDFILESET = 4;
    final int IDROWS = 5;

    Vector fileset = new Vector();
    String filename = "";
    String url = "";
    String text = "";
    int max = 0;

    if (args.length == 0) {
      System.err.println(usage);
      System.exit(-1);
    }

    for (int i = 0; i < args.length; i++) { // parse command line
      if (args[i].equals("-identifyfile")) {
        command = IDFILE;
        filename = args[++i];
      }

      if (args[i].equals("-identifyurl")) {
        command = IDURL;
        filename = args[++i];
      }

      if (args[i].equals("-identifyrows")) {
        command = IDROWS;
        filename = args[++i];
        max = Integer.parseInt(args[++i]);
      }

      if (args[i].equals("-identifytext")) {
        command = IDTEXT;
        for (i++; i < args.length - 1; i++)
          text += args[i] + " ";
      }

      if (args[i].equals("-identifyfileset")) {
        command = IDFILESET;
        for (i++; i < args.length; i++) {
          fileset.add(args[i]);
          System.out.println(args[i]);
        }
      }

    }

    String lang = null;
    LanguageIdentifier idfr = LanguageIdentifier.getInstance();
    File f;
    FileInputStream fis;
    try {
      switch (command) {

        case IDTEXT:
          lang = idfr.identify(text);
          break;

        case IDFILE:
          f = new File(filename);
          fis = new FileInputStream(f);
          lang = idfr.identify(fis);
          fis.close();
          break;

        case IDURL:
          text = getUrlContent(filename);
          lang = idfr.identify(text);
          break;

        case IDROWS:
          f = new File(filename);
          BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(f)));
          String line;
          while (max > 0 && (line = br.readLine()) != null) {
            line = line.trim();
            if (line.length() > 2) {
              max--;
              lang = idfr.identify(line);
              System.out.println("R=" + lang + ":" + line);
            }
          }

          br.close();
          System.exit(0);
          break;

        case IDFILESET:
          System.out.println("FILESET");
          Iterator i = fileset.iterator();

          while (i.hasNext()) {
            try {
              filename = (String) i.next();
              f = new File(filename);
              fis = new FileInputStream(f);
              lang = idfr.identify(fis);
              fis.close();
            } catch (Exception e) {
              System.out.println(e);
            }

            System.out.println(filename + " was identified as " + lang);
          }
          System.exit(0);
          break;

      }
    } catch (Exception e) {
      System.out.println(e);
    }
    System.out.println("text was identified as " + lang);
  }

  /**
   * @param url
   * @return contents of url
   */
  private static String getUrlContent(String url) {
    Protocol protocol;
    try {
      protocol = ProtocolFactory.getProtocol(url);
      Content content = protocol.getContent(url);
      String contentType = content.getContentType();
      Parser parser = ParserFactory.getParser(contentType, url);
      Parse parse = parser.getParse(content);
      System.out.println("text:" + parse.getText());
      return parse.getText();

    } catch (ProtocolNotFound e) {
      e.printStackTrace();
    } catch (ProtocolException e) {
      e.printStackTrace();
    } catch (ParserNotFound e) {
      e.printStackTrace();
    } catch (ParseException e) {
      e.printStackTrace();
    }
    return null;
  }

  /**
   * Identify language based on submitted content
   * 
   * @param text text of doc
   * @return 2 letter ISO639 code of language (en, fi, sv...) , or null if
   *         unknown
   */
  public String identify(String text) {

    return identify(new StringBuffer(text));
  }

  public String identify(StringBuffer text) {

    NGramProfile p = new NGramProfile("suspect");
    p.analyze(text);

    float topscore = Float.MAX_VALUE;
    String lang = "";

    Iterator i = languages.iterator();
    while (i.hasNext()) {

      NGramProfile profile = (NGramProfile) i.next();
      float score = profile.getSimilarity(p);

      //LOG.fine(profile.getName() + ":" + score);

      if (score < topscore) {
        topscore = score;
        lang = profile.getName();
      }
    }

    p.ngrams.clear();
    p = null;

    LOG.finest("TOPSCORE: " + lang + " with " + topscore);

    if (topscore > SCORE_THRESOLD)
      return lang;

    else return null;
  }

  /**
   * Identify language from inputstream
   * 
   * @param is
   * @return language code
   * @throws IOException
   */
  public String identify(InputStream is) throws IOException {

    StringBuffer text = new StringBuffer();
    byte buffer[] = new byte[2000];
    int len = 0;

    while ((len = is.read(buffer)) != -1) {
      text.append(new String(buffer, 0, len));
    }

    return identify(text.toString());
  }

  public Document filter(Document doc, Parse parse, FetcherOutput fo) throws IndexingException {

    //check if X-meta-lang found, possibly put there by HTMLLanguageParser
    String lang = parse.getData().get(HTMLLanguageParser.META_LANG_NAME);

    //check if HTTP-header tels us the language
    if (lang == null) lang = parse.getData().get("Content-Language");

    if (lang == null) {
      StringBuffer text = new StringBuffer();
      /*
       * String[] anchors = fo.getAnchors(); for (int i = 0; i < anchors.length;
       * i++) { text+=anchors[i] + " "; }
       */
      text.append(parse.getData().getTitle()).append(" ");
      text.append(parse.getText());
      lang = LanguageIdentifier.getInstance().identify(text);
    }

    if (lang == null) {
      lang = "unknown";
    }

    doc.add(Field.Keyword("lang", lang));

    return doc;
  }

}
