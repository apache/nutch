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

// JDK imports
import java.io.File;
import java.io.InputStream;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Vector;
import java.util.HashMap;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Enumeration;
import java.util.logging.Logger;

// Nutch imports
import org.apache.nutch.analysis.lang.NGramProfile.NGramEntry;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.Parser;
import org.apache.nutch.parse.ParserFactory;
import org.apache.nutch.parse.ParseException;
import org.apache.nutch.parse.ParserNotFound;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.Protocol;
import org.apache.nutch.protocol.ProtocolFactory;
import org.apache.nutch.protocol.ProtocolNotFound;
import org.apache.nutch.protocol.ProtocolException;
import org.apache.nutch.util.NutchConf;
import org.apache.nutch.util.LogFormatter;


/**
 * 
 * @author Sami Siren
 * @author Jerome Charron
 */
public class LanguageIdentifier {
  
 
  private final static int DEFAULT_ANALYSIS_LENGTH = 0;    // 0 means full content
  
  private final static float SCORE_THRESOLD = 0.00F;

  public final static Logger LOG = LogFormatter.getLogger(LanguageIdentifier.class.getName());

  
  private ArrayList languages = new ArrayList();

  private ArrayList supportedLanguages = new ArrayList();

  /** Minimum size of NGrams */
  private int minLength = NGramProfile.DEFAULT_MIN_NGRAM_LENGTH;
  
  /** Maximum size of NGrams */
  private int maxLength = NGramProfile.DEFAULT_MAX_NGRAM_LENGTH;
  
  /** The maximum amount of data to analyze */
  private int analyzeLength = DEFAULT_ANALYSIS_LENGTH;
  
  /** A global index of ngrams of all supported languages */
  private HashMap ngramsIdx = new HashMap();

  /** The NGramProfile used for identification */
  private NGramProfile suspect = null;

  /** My singleton instance */
  private static LanguageIdentifier identifier = null;


  /**
   * Constructs a new Language Identifier.
   */
  private LanguageIdentifier() {

    // Gets ngram sizes to take into account from the Nutch Config
    minLength = NutchConf.get().getInt("lang.ngram.min.length",
                                       NGramProfile.DEFAULT_MIN_NGRAM_LENGTH);
    maxLength = NutchConf.get().getInt("lang.ngram.max.length",
                                       NGramProfile.DEFAULT_MAX_NGRAM_LENGTH);
    // Ensure the min and max values are in an acceptale range
    // (ie min >= DEFAULT_MIN_NGRAM_LENGTH and max <= DEFAULT_MAX_NGRAM_LENGTH)
    maxLength = Math.min(maxLength, NGramProfile.ABSOLUTE_MAX_NGRAM_LENGTH);
    maxLength = Math.max(maxLength, NGramProfile.ABSOLUTE_MIN_NGRAM_LENGTH);
    minLength = Math.max(minLength, NGramProfile.ABSOLUTE_MIN_NGRAM_LENGTH);
    minLength = Math.min(minLength, maxLength);

    // Gets the value of the maximum size of data to analyze
    analyzeLength = NutchConf.get().getInt("lang.analyze.max.length",
                                           DEFAULT_ANALYSIS_LENGTH);
    
    Properties p = new Properties();
    try {
      p.load(this.getClass().getResourceAsStream("langmappings.properties"));

      Enumeration alllanguages = p.keys();
      
      LOG.info(new StringBuffer()
                .append("Language identifier configuration [")
                .append(minLength).append("-").append(maxLength)
                .append("/").append(analyzeLength).append("]").toString());

      StringBuffer list = new StringBuffer("Language identifier plugin supports:");
      HashMap tmpIdx = new HashMap();
      while (alllanguages.hasMoreElements()) {
        String lang = (String) (alllanguages.nextElement());

        InputStream is = this.getClass().getClassLoader().getResourceAsStream(
                "org/apache/nutch/analysis/lang/" + lang + "." + NGramProfile.FILE_EXTENSION);

        if (is != null) {
          NGramProfile profile = new NGramProfile(lang, minLength, maxLength);
          try {
            profile.load(is);
            languages.add(profile);
            supportedLanguages.add(lang);
            List ngrams = profile.getSorted();
            for (int i=0; i<ngrams.size(); i++) {
                NGramEntry entry = (NGramEntry) ngrams.get(i);
                List registered = (List) tmpIdx.get(entry);
                if (registered == null) {
                    registered = new ArrayList();
                    tmpIdx.put(entry, registered);
                }
                registered.add(entry);
                entry.setProfile(profile);
            }
            list.append(" " + lang + "(" + ngrams.size() + ")");
            is.close();
          } catch (IOException e1) {
            LOG.severe(e1.toString());
          }
        }
      }
      // transform all ngrams lists to arrays for performances
      Iterator keys = tmpIdx.keySet().iterator();
      while (keys.hasNext()) {
        NGramEntry entry = (NGramEntry) keys.next();
        List l = (List) tmpIdx.get(entry);
        if (l != null) {
          NGramEntry[] array = (NGramEntry[]) l.toArray(new NGramEntry[l.size()]);
          ngramsIdx.put(entry.getSeq(), array);
        }
      }
      LOG.info(list.toString());
      // Create the suspect profile
      suspect = new NGramProfile("suspect", minLength, maxLength);
    } catch (Exception e) {
      LOG.severe(e.toString());
    }
  }

  /**
   * return handle to singleton instance
   */
  public static LanguageIdentifier getInstance() {
    if (identifier == null) {
        synchronized(LanguageIdentifier.class) {
            if (identifier == null) {
                identifier = new LanguageIdentifier();
            }
        }
    }
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
          File[] files = null;
          File f = new File(args[i]);
          if (f.isDirectory()) {
              files = f.listFiles();
          } else {
              files = new File[] { f };
          }
          for (int j=0; j<files.length; j++) {
            fileset.add(files[j].getAbsolutePath());
          }
        }
      }

    }

    String lang = null;
    //LanguageIdentifier idfr = LanguageIdentifier.getInstance();
    LanguageIdentifier idfr = new LanguageIdentifier();
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
          /* used for benchs
          for (int j=128; j<=524288; j*=2) {
            long start = System.currentTimeMillis();
            idfr.analyzeLength = j; */
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
          /* used for benchs
            System.out.println(j + "/" + (System.currentTimeMillis()-start));
          } */
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
      Content content = protocol.getProtocolOutput(url).getContent();
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
   * @param text to analyze
   * @return 2 letter ISO639 code of language (en, fi, sv...) , or null if
   *         unknown
   */
  public String identify(String text) {
    return identify(new StringBuffer(text));
  }

  /**
   * Identify language based on submitted content
   * 
   * @param text to analyze
   * @return 2 letter ISO639 code of language (en, fi, sv...) , or null if
   *         unknown
   */
  public String identify(StringBuffer content) {

    StringBuffer text = content;
    if ((analyzeLength > 0) && (content.length() > analyzeLength)) {
        text = new StringBuffer().append(content);
        text.setLength(analyzeLength);
    }

    suspect.analyze(text);
    Iterator iter = suspect.getSorted().iterator();
    float topscore = Float.MIN_VALUE;
    String lang = "";
    HashMap scores = new HashMap();
    NGramEntry searched = null;
    
    while (iter.hasNext()) {
        searched = (NGramEntry) iter.next();
        NGramEntry[] ngrams = (NGramEntry[]) ngramsIdx.get(searched.getSeq());
        if (ngrams != null) {
            for (int j=0; j<ngrams.length; j++) {
                NGramProfile profile = ngrams[j].getProfile();
                Float pScore = (Float) scores.get(profile);
                if (pScore == null) {
                    pScore = new Float(0);
                }
                float plScore = pScore.floatValue();
                plScore += ngrams[j].getFrequency() + searched.getFrequency();
                scores.put(profile, new Float(plScore));
                if (plScore > topscore) {
                    topscore = plScore;
                    lang = profile.getName();
                }
            }
        }
    }
    return lang;
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
    byte[] buffer = new byte[2048];
    int len = 0;

    while (((len = is.read(buffer)) != -1) &&
           ((analyzeLength == 0) || (text.length() < analyzeLength))) {
      if (analyzeLength != 0) {
          len = Math.min(len, analyzeLength - text.length());
      }
      text.append(new String(buffer, 0, len));
    }
    return identify(text);
  }

}
