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

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.Date;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;
import java.util.logging.Logger;

import org.apache.nutch.util.LogFormatter;

import org.apache.lucene.analysis.Token;

/**
 * This class runs a ngram analysis over submitted text, results might be used
 * for automatic language identifiaction.
 * 
 * The similarity calculation is at experimental level. You have been warned.
 * 
 * Methods are provided to build new NGramProfiles profiles.
 * 
 * @author Sami Siren
 */
public class NGramProfile {

  public static final Logger LOG = LogFormatter
      .getLogger("org.apache.nutch.analysis.lang.NGramProfile");

  private String name;

  private Vector sorted = null;

  private StringBuffer tokensb = new StringBuffer();

  private int min_ngram_length = DEFAULT_MIN_NGRAM_LENGTH;

  private int max_ngram_length = DEFAULT_MAX_NGRAM_LENGTH;

  private int ngramcount = 0;

  static final String NGRAM_FILE_EXTENSION = "ngp";

  static final int NGRAM_LENGTH = 1000;

  //separator char
  static final char SEPARATOR = '_';

  //default min length of ngram
  static final int DEFAULT_MIN_NGRAM_LENGTH = 1;

  //default max length of ngram
  static final int DEFAULT_MAX_NGRAM_LENGTH = 4;

  //table to store ngrams
  Hashtable ngrams = null;

  /**
   * private class used to store NGramEntry
   */
  class NGramEntry implements Comparable {
    private CharSequence seq;

    private int count;

    private float normalized_count;

    public NGramEntry(CharSequence seq) {
      this.seq = seq;
    }

    /**
     * @param ngramsequence
     * @param ngramcount
     */
    public NGramEntry(String ngramsequence, int ngramcount) {
      seq = new StringBuffer(ngramsequence).subSequence(0, ngramsequence
          .length());
      this.count = ngramcount;
    }

    public int getCount() {
      return count;
    }

    public CharSequence getSeq() {
      return seq;
    }

    public int compareTo(Object o) {
      if (((NGramEntry) o).count - count != 0)
        return ((NGramEntry) o).count - count;
      else
        return (seq.toString().compareTo(((NGramEntry) o).seq.toString()));
    }

    public void inc() {
      count++;
    }
  }

  /**
   * Construct a new ngram profile
   * 
   * @param name
   *          Name of profile
   */
  public NGramProfile(String name) {
    this(name, DEFAULT_MIN_NGRAM_LENGTH, DEFAULT_MAX_NGRAM_LENGTH);
  }

  /**
   * Construct a new ngram profile
   * 
   * @param name
   *          Name of profile
   * @param minlen
   *          min length of ngram sequences
   * @param maxlen
   *          max length of ngram sequences
   */
  public NGramProfile(String name, int minlen, int maxlen) {
    ngrams = new Hashtable();
    this.max_ngram_length = maxlen;
    this.min_ngram_length = minlen;
    this.name = name;
  }

  /**
   * Add ngrams from a token to this profile
   * 
   * @param t
   *          Token to be added
   */
  public void addFromToken(Token t) {
    tokensb.setLength(0);
    tokensb.append(SEPARATOR).append(t.termText()).append(SEPARATOR);
    addNGrams(tokensb);
  }

  /**
   * Analyze a piece of text
   * 
   * @param text
   *          the text to be analyzed
   */
  public void analyze(StringBuffer text) {
    StringBuffer word;
    int i;

    if (ngrams != null) {
      ngrams.clear();
    }

    word = new StringBuffer().append(SEPARATOR);
    for (i = 0; i < text.length(); i++) {
      char c = Character.toLowerCase(text.charAt(i));

      if (Character.isLetter(c)) {
        word.append(c);
      } else {
        //found word boundary
        if (word.length() > 1) {
          //we have a word!
          word.append(SEPARATOR);
          addNGrams(word);
          word.delete(0, word.length());
        }
      }
    }

    if (word.length() > 1) {
      //we have a last word
      word.append(SEPARATOR);
      addNGrams(word);
    }
    normalize();
  }

  /**
   * Normalize profile
   */
  protected void normalize() {
    Vector sorted = getSorted();
    int sum = 0;

    //only calculate ngramcount if it was not available in profile
    if (ngramcount == 0) {
      for (int i = 0; i < sorted.size(); i++) {
        ngramcount += ((NGramEntry) sorted.get(i)).count;
      }
    }

    if (sorted.size() > 0) {
      Iterator i = sorted.iterator();

      while (i.hasNext()) {
        NGramEntry e = (NGramEntry) i.next();
        e.normalized_count = e.count / (float)ngramcount;
      }
    }
  }

  /**
   * Add ngrams from a single word to this profile
   * 
   * @param word
   */
  public void addNGrams(StringBuffer word) {
    int i;

    for (i = min_ngram_length; i <= max_ngram_length && i < word.length(); i++) {
      addNGrams(word, i);
    }
  }

  /**
   * @param word
   * @param n
   *          sequence length
   */
  private void addNGrams(StringBuffer word, int n) {
    NGramEntry nge;
    StringBuffer sb;
    int i;

    for (i = 0; i <= word.length() - n; i++) {

      CharSequence cs = word.subSequence(i, i + n);

      if (ngrams.containsKey(cs)) {
        nge = (NGramEntry) ngrams.get(cs);
      } else {
        nge = new NGramEntry(cs);
      }
      nge.inc();
      ngrams.put(cs, nge);
    }
  }

  /**
   * Return sorted vector of ngrams (sort done by 1. count 2. sequence)
   * 
   * @return sorted vector of ngrams
   */
  public Vector getSorted() {
    //make sure srting is done only once
    if (sorted == null) {
      sorted = new Vector(ngrams.values());
      Collections.sort(sorted);

      //trim at NGRAM_LENGTH entries
      if (sorted.size() > NGRAM_LENGTH)
        sorted.setSize(NGRAM_LENGTH);
    }

    return sorted;
  }

  /**
   * Return ngramprofile as text
   * 
   * @return ngramprofile as text
   */
  public String toString() {
    StringBuffer s = new StringBuffer();

    Iterator i = getSorted().iterator();

    s.append("NGramProfile: ").append(name).append("\n");
    while (i.hasNext()) {
      NGramEntry entry = (NGramEntry) i.next();
      s.append(entry.count).append(':').append(entry.seq).append(" ").append(
          entry.normalized_count).append("\n");
    }
    return s.toString();
  }

  /**
   * Calculate a score how well NGramProfiles match each other
   * 
   * @param another
   *          ngram profile to compare against
   * @return similarity 0=exact match
   */
  public float getSimilarity(NGramProfile another) {
    float sum = 0;

    try {
      Iterator i = another.getSorted().iterator();
      while (i.hasNext()) {
        NGramEntry other = (NGramEntry) i.next();
        if (ngrams.containsKey(other.seq)) {
          sum += Math.abs((other.normalized_count - ((NGramEntry) ngrams
              .get(other.seq)).normalized_count)) / 2;
        } else {
          sum += other.normalized_count;
        }
      }
      i = getSorted().iterator();
      while (i.hasNext()) {
        NGramEntry other = (NGramEntry) i.next();
        if (another.ngrams.containsKey(other.seq)) {
          sum += Math
              .abs((other.normalized_count - ((NGramEntry) another.ngrams
                  .get(other.seq)).normalized_count)) / 2;
        } else {
          sum += other.normalized_count;
        }
      }
    } catch (Exception e) {
      LOG.severe(e.toString());
    }
    return sum;
  }

  /**
   * Loads a ngram profile from InputStream (assumes UTF-8 encoded content)
   */
  public void load(InputStream is) throws IOException {
    BufferedReader bis = new BufferedReader(new InputStreamReader(is, "UTF-8"));
    String line;

    ngrams.clear();

    while ((line = bis.readLine()) != null) {

      // # starts a comment line
      if (line.charAt(0) != '#') {
        int spacepos = line.indexOf(' ');
        String ngramsequence = line.substring(0, spacepos).trim();
        int ngramcount = Integer.parseInt(line.substring(spacepos + 1));

        if (!line.startsWith("ngram_count")) {
          NGramEntry en = new NGramEntry(ngramsequence, ngramcount);
          ngrams.put(en.getSeq(), en);
        } else {
          this.ngramcount = ngramcount;
        }
      }
    }
    normalize();
  }

  /**
   * Create a new Language profile from (preferably quite large) text file
   * 
   * @param name
   *          name of profile
   * @param is
   * @param encoding
   *          encoding of stream
   */
  public static NGramProfile createNgramProfile(String name, InputStream is,
      String encoding) {

    NGramProfile newProfile = new NGramProfile(name);
    BufferedInputStream bis = new BufferedInputStream(is);

    byte buffer[] = new byte[4096];
    StringBuffer text = new StringBuffer();
    int len;

    try {
      while ((len = bis.read(buffer)) != -1) {
        text.append(new String(buffer, 0, len, encoding));
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    newProfile.analyze(text);

    return newProfile;
  }

  /**
   * Writes NGramProfile content into OutputStream, content is outputted with
   * UTF-8 encoding
   * 
   * @param os
   *          Stream to output to
   * @throws IOException
   */

  public void save(OutputStream os) throws IOException {
    Vector v = getSorted();
    Iterator i = v.iterator();
    os
        .write(("# NgramProfile generated at " + new Date() + " for Nutch Language Identification\n")
            .getBytes());
    os.write(("ngram_count " + ngramcount + "\n").getBytes());

    while (i.hasNext()) {
      NGramEntry e = (NGramEntry) i.next();
      String line = e.getSeq().toString() + " " + e.getCount() + "\n";
      os.write(line.getBytes("UTF-8"));
    }

    os.flush();
  }

  /**
   * main method used for testing only
   * 
   * @param args
   */
  public static void main(String args[]) {

    String usage = "Usage: NGramProfile [-create profilename filename encoding] [-similarity file1 file2] [-score profile-name filename encoding]";
    int command = 0;

    final int CREATE = 1;
    final int SIMILARITY = 2;
    final int SCORE = 3;

    String profilename = "";
    String filename = "";
    String filename2 = "";
    String encoding = "";

    if (args.length == 0) {
      System.err.println(usage);
      System.exit(-1);
    }

    for (int i = 0; i < args.length; i++) { // parse command line
      if (args[i].equals("-create")) { // found -create option
        command = CREATE;
        profilename = args[++i];
        filename = args[++i];
        encoding = args[++i];
      }

      if (args[i].equals("-similarity")) { // found -similarity option
        command = SIMILARITY;
        filename = args[++i];
        filename2 = args[++i];
        encoding = args[++i];
      }

      if (args[i].equals("-score")) { // found -Score option
        command = SCORE;
        profilename = args[++i];
        filename = args[++i];
        encoding = args[++i];
      }
    }

    try {

      switch (command) {

      case CREATE:

        File f = new File(filename);
        FileInputStream fis = new FileInputStream(f);
        NGramProfile newProfile = NGramProfile.createNgramProfile(profilename,
            fis, encoding);
        fis.close();
        f = new File(profilename + "." + NGRAM_FILE_EXTENSION);
        FileOutputStream fos = new FileOutputStream(f);
        newProfile.save(fos);
        System.out.println("new profile " + profilename + "."
            + NGRAM_FILE_EXTENSION + " was created.");
        break;

      case SIMILARITY:

        f = new File(filename);
        fis = new FileInputStream(f);
        newProfile = NGramProfile.createNgramProfile(filename, fis, encoding);
        newProfile.normalize();

        f = new File(filename2);
        fis = new FileInputStream(f);
        NGramProfile newProfile2 = NGramProfile.createNgramProfile(filename2,
            fis, encoding);
        newProfile2.normalize();
        System.out.println("Similarity is "
            + newProfile.getSimilarity(newProfile2));
        break;

      case SCORE:
        f = new File(filename);
        fis = new FileInputStream(f);
        newProfile = NGramProfile.createNgramProfile(filename, fis, encoding);

        f = new File(profilename + "." + NGRAM_FILE_EXTENSION);
        fis = new FileInputStream(f);
        NGramProfile compare = new NGramProfile(profilename);
        compare.load(fis);
        System.out.println("Score is " + compare.getSimilarity(newProfile));

        break;

      }

    } catch (Exception e) {
      LOG.severe("Caught an exception:" + e);
    }
  }

  /**
   * @return Returns the name.
   */
  public String getName() {
    return name;
  }

  /**
   * @param name
   *          The name to set.
   */
  public void setName(String name) {
    this.name = name;
  }
}
