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

package org.apache.nutch.searcher;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.ArrayList;

// Commons Logging imports
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.nutch.analysis.AnalyzerFactory;

import org.apache.nutch.analysis.NutchAnalysis;
import org.apache.nutch.util.NutchConfiguration;

/** A Nutch query. */
public final class Query implements Writable, Cloneable, Configurable {
  public static final Log LOG = LogFactory.getLog(Query.class);

  /** A query clause. */
  public static class Clause implements Cloneable {
    public static final String DEFAULT_FIELD = "DEFAULT";

    private static final byte REQUIRED_BIT = 1;
    private static final byte PROHIBITED_BIT = 2;
    private static final byte PHRASE_BIT = 4;

    private boolean isRequired;
    private boolean isProhibited;
    private String field = DEFAULT_FIELD;
    private float weight = 1.0f;
    private Object termOrPhrase;

    private Configuration conf; 

    public Clause(Term term, String field,
                  boolean isRequired, boolean isProhibited, Configuration conf) {
      this(term, isRequired, isProhibited, conf);
      this.field = field;
    }

    public Clause(Term term, boolean isRequired, boolean isProhibited, Configuration conf) {
      this.isRequired = isRequired;
      this.isProhibited = isProhibited;
      this.termOrPhrase = term;
      this.conf = conf;
    }

    public Clause(Phrase phrase, String field,
                  boolean isRequired, boolean isProhibited, Configuration conf) {
      this(phrase, isRequired, isProhibited, conf);
      this.field = field;
    }

    public Clause(Phrase phrase, boolean isRequired, boolean isProhibited, Configuration conf) {
      this.isRequired = isRequired;
      this.isProhibited = isProhibited;
      this.termOrPhrase = phrase;
      this.conf = conf;
    }

    public boolean isRequired() { return isRequired; }
    public boolean isProhibited() { return isProhibited; }

    public String getField() { return field; }

    public float getWeight() { return weight; }
    public void setWeight(float weight) {  this.weight = weight; }

    public boolean isPhrase() { return termOrPhrase instanceof Phrase; }

    public Phrase getPhrase() { return (Phrase)termOrPhrase; }
    public Term getTerm() { return (Term)termOrPhrase; }

    public void write(DataOutput out) throws IOException {
      byte bits = 0;
      if (isPhrase())
        bits |= PHRASE_BIT;
      if (isRequired)
        bits |= REQUIRED_BIT;
      if (isProhibited)
        bits |= PROHIBITED_BIT;
      out.writeByte(bits);
      out.writeUTF(field);
      out.writeFloat(weight);
      
      if (isPhrase())
        getPhrase().write(out);
      else
        getTerm().write(out);
    }

    public static Clause read(DataInput in, Configuration conf) throws IOException {
      byte bits = in.readByte();
      boolean required = ((bits & REQUIRED_BIT) != 0);
      boolean prohibited = ((bits & PROHIBITED_BIT) != 0);

      String field = in.readUTF();
      float weight = in.readFloat();

      Clause clause;
      if ((bits & PHRASE_BIT) == 0) {
        clause = new Clause(Term.read(in), field, required, prohibited, conf);
      } else {
        clause = new Clause(Phrase.read(in), field, required, prohibited, conf);
      }
      clause.weight = weight;
      return clause;
    }

    public String toString() {
      StringBuffer buffer = new StringBuffer();
//       if (isRequired)
//         buffer.append("+");
//       else
      if (isProhibited)
        buffer.append ("-");

      if (!DEFAULT_FIELD.equals(field)) {
        buffer.append(field);
        buffer.append(":");
      }

      if (!isPhrase() && new QueryFilters(conf).isRawField(field)) {
        buffer.append('"');                        // quote raw terms
        buffer.append(termOrPhrase.toString());
        buffer.append('"');
      } else {
        buffer.append(termOrPhrase.toString());
      }

      return buffer.toString();
    }

    public boolean equals(Object o) {
      if (!(o instanceof Clause)) return false;
      Clause other = (Clause)o;
      return
        (this.isRequired == other.isRequired) &&
        (this.isProhibited == other.isProhibited) &&
        (this.weight == other.weight) &&
        (this.termOrPhrase == null ? other.termOrPhrase == null :
         this.termOrPhrase.equals(other.termOrPhrase));
    }
        
    public int hashCode() {
      return
        (this.isRequired ? 0 : 1) ^
        (this.isProhibited ? 2 : 4) ^
        Float.floatToIntBits(this.weight) ^
        (this.termOrPhrase != null ? termOrPhrase.hashCode() : 0);
    }
    
    public Object clone() {
      try {
        return super.clone();
      } catch (CloneNotSupportedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /** A single-term query clause. */
  public static class Term {
    private String text;

    public Term(String text) {
      this.text = text;
    }

    public void write(DataOutput out) throws IOException {
      out.writeUTF(text);
    }

    public static Term read(DataInput in) throws IOException {
      String text = in.readUTF();
      return new Term(text);
    }

    public String toString() {
      return text;
    }

    public boolean equals(Object o) {
      if (!(o instanceof Term)) return false;
      Term other = (Term)o;
      return text == null ? other.text == null : text.equals(other.text);
    }

    public int hashCode() {
      return text != null ? text.hashCode() : 0;
    }
  }

  /** A phrase query clause. */
  public static class Phrase {
    private Term[] terms;

    public Phrase(Term[] terms) {
      this.terms = terms;
    }

    public Phrase(String[] terms) {
      this.terms = new Term[terms.length];
      for (int i = 0; i < terms.length; i++) {
        this.terms[i] = new Term(terms[i]);
      }
    }

    public Term[] getTerms() { return terms; }

    public void write(DataOutput out) throws IOException {
      out.writeByte(terms.length);
      for (int i = 0; i < terms.length; i++)
        terms[i].write(out);
    }

    public static Phrase read(DataInput in) throws IOException {
      int length = in.readByte();
      Term[] terms = new Term[length];
      for (int i = 0; i < length; i++)
        terms[i] = Term.read(in);
      return new Phrase(terms);
    }

    public String toString() {
      StringBuffer buffer = new StringBuffer();
      buffer.append("\"");
      for (int i = 0; i < terms.length; i++) {
        buffer.append(terms[i].toString());
        if (i != terms.length-1)
          buffer.append(" ");
      }
      buffer.append("\"");
      return buffer.toString();
    }

    public boolean equals(Object o) {
      if (!(o instanceof Phrase)) return false;
      Phrase other = (Phrase)o;
      if (!(this.terms.length == this.terms.length))
        return false;
      for (int i = 0; i < terms.length; i++) {
        if (!this.terms[i].equals(other.terms[i]))
          return false;
      }
      return true;
    }

    public int hashCode() {
      int hashCode = terms.length;
      for (int i = 0; i < terms.length; i++) {
        hashCode ^= terms[i].hashCode();
      }
      return hashCode;
    }

  }


  private ArrayList clauses = new ArrayList();

  private Configuration conf;

  private static final Clause[] CLAUSES_PROTO = new Clause[0];
  
  public Query() {
  }
  
  public Query(Configuration conf) {
      this.conf = conf;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }
  
  public Configuration getConf() {
    return conf;
  }
  
  /** Return all clauses. */
  public Clause[] getClauses() {
    return (Clause[])clauses.toArray(CLAUSES_PROTO);
  }

  /** Add a required term in the default field. */
  public void addRequiredTerm(String term) {
    addRequiredTerm(term, Clause.DEFAULT_FIELD);
  }

  /** Add a required term in a specified field. */
  public void addRequiredTerm(String term, String field) {
    clauses.add(new Clause(new Term(term), field, true, false, this.conf));
  }

  /** Add a prohibited term in the default field. */
  public void addProhibitedTerm(String term) {
    addProhibitedTerm(term, Clause.DEFAULT_FIELD);
  }

  /** Add a prohibited term in the specified field. */
  public void addProhibitedTerm(String term, String field) {
    clauses.add(new Clause(new Term(term), field, false, true, this.conf));
  }

  /** Add a required phrase in the default field. */
  public void addRequiredPhrase(String[] terms) {
    addRequiredPhrase(terms, Clause.DEFAULT_FIELD);
  }

  /** Add a required phrase in the specified field. */
  public void addRequiredPhrase(String[] terms, String field) {
    if (terms.length == 0) {                      // ignore empty phrase
    } else if (terms.length == 1) {
      addRequiredTerm(terms[0], field);           // optimize to term query
    } else {
      clauses.add(new Clause(new Phrase(terms), field, true, false, this.conf));
    }
  }

  /** Add a prohibited phrase in the default field. */
  public void addProhibitedPhrase(String[] terms) {
    addProhibitedPhrase(terms, Clause.DEFAULT_FIELD);
  }

  /** Add a prohibited phrase in the specified field. */
  public void addProhibitedPhrase(String[] terms, String field) {
    if (terms.length == 0) {                      // ignore empty phrase
    } else if (terms.length == 1) {
      addProhibitedTerm(terms[0], field);         // optimize to term query
    } else {
      clauses.add(new Clause(new Phrase(terms), field, false, true, this.conf));
    }
  }

  public void write(DataOutput out) throws IOException {
    out.writeByte(clauses.size());
    for (int i = 0; i < clauses.size(); i++)
      ((Clause)clauses.get(i)).write(out);
  }
  
  public static Query read(DataInput in, Configuration conf) throws IOException {
    Query result = new Query(conf);
    result.readFields(in);
    return result;
  }

  public void readFields(DataInput in) throws IOException {
    clauses.clear();
    int length = in.readByte();
    for (int i = 0; i < length; i++)
      clauses.add(Clause.read(in, this.conf));
  }

  public String toString() {
    StringBuffer buffer = new StringBuffer();
    for (int i = 0; i < clauses.size(); i++) {
      buffer.append(clauses.get(i).toString());
      if (i != clauses.size()-1)
        buffer.append(" ");
    }
    return buffer.toString();
  }

  public boolean equals(Object o) {
    if (!(o instanceof Query)) return false;
    Query other = (Query)o;
    return this.clauses.equals(other.clauses);
  }
  
  public int hashCode() {
    return this.clauses.hashCode();
  }

  public Object clone() {
    Query clone = null;
    try {
      clone = (Query)super.clone();
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
    clone.clauses = (ArrayList)clauses.clone();
    return clone;
  }


  /** Flattens a query into the set of text terms that it contains.  These are
   * terms which should be higlighted in matching documents. */
  public String[] getTerms() {
    ArrayList result = new ArrayList();
    for (int i = 0; i < clauses.size(); i++) {
      Clause clause = (Clause)clauses.get(i);
      if (!clause.isProhibited()) {
        if (clause.isPhrase()) {
          Term[] terms = clause.getPhrase().getTerms();
          for (int j = 0; j < terms.length; j++) {
            result.add(terms[j].toString());
          }
        } else {
          result.add(clause.getTerm().toString());
        }
      }
    }
    return (String[])result.toArray(new String[result.size()]);
  }

  /**
   * Parse a query from a string using a language specific analyzer.
   *
   * @param queryString is the raw query string to parse
   * @param queryLang is a two-letters language code used to identify which
   *        {@link org.apache.nutch.analysis.NutchAnalyzer} should be used
   *        to parse the query string.
   * @see org.apache.nutch.analysis.AnalyzerFactory
   */
  public static Query parse(String queryString, String queryLang, Configuration conf)
  throws IOException {
    return fixup(NutchAnalysis.parseQuery(
            queryString, AnalyzerFactory.get(conf).get(queryLang), conf), conf);
  }

  /** Parse a query from a string. */
  public static Query parse(String queryString, Configuration conf) throws IOException {
    return parse(queryString, null, conf);
  }

  /** Convert clauses in unknown fields to the default field. */
  private static Query fixup(Query input, Configuration conf) {
    // walk the query
    Query output = new Query(conf);
    Clause[] clauses = input.getClauses();
    for (int i = 0; i < clauses.length; i++) {
      Clause c = clauses[i];
      if (!new QueryFilters(conf).isField(c.getField())) {  // unknown field
        ArrayList terms = new ArrayList();        // add name to query
        if (c.isPhrase()) {                       
          terms.addAll(Arrays.asList(c.getPhrase().getTerms()));
        } else {
          terms.add(c.getTerm());
        }
        terms.add(0, new Term(c.getField()));     // add to front of phrase
        c = (Clause)c.clone();
        c.field = Clause.DEFAULT_FIELD;           // use default field instead
        c.termOrPhrase
          = new Phrase((Term[])terms.toArray(new Term[terms.size()]));
      }
      output.clauses.add(c);                    // copy clause to output
    }
    return output;
  }

  /** For debugging. */
  public static void main(String[] args) throws Exception {
    BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
    Configuration conf = NutchConfiguration.create();
    while (true) {
      System.out.print("Query: ");
      String line = in.readLine();
      Query query = parse(line, conf);
      System.out.println("Parsed: " + query);
      System.out.println("Translated: " + new QueryFilters(conf).filter(query));
    }
  }
}
