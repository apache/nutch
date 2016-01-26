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

package org.apache.nutch.indexwriter.csv;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.List;
import java.util.ListIterator;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.indexer.IndexWriter;
import org.apache.nutch.indexer.IndexingJob;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.indexer.NutchField;
import org.apache.nutch.util.NutchConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Write Nutch documents to a CSV file (comma separated values), i.e., dump
 * index as CSV or tab-separated plain text table. Format (encoding, separators,
 * etc.) is configurable by a couple of options, see output of
 * {@link #describe()}.
 */
public class CSVIndexWriter implements IndexWriter {

  public static final Logger LOG = LoggerFactory
      .getLogger(CSVIndexWriter.class);

  private Configuration config;

  /** ordered list of fields (columns) in the CSV file */
  private String[] fields;
  public static final String CSV_FIELDS = "indexer.csv.fields";

  /** encoding of CSV file */
  protected Charset encoding = Charset.forName("UTF-8");
  public static final String CSV_CHARSET = "indexer.csv.charset";

  /**
   * represent separators (also quote and escape characters) as char(s) and
   * byte(s) in the output encoding for efficiency.
   */
  protected class Separator {
    protected String sepStr;
    protected char[] chars;
    protected byte[] bytes;

    protected Separator(String sep) {
      set(sep);
    }

    protected void set(String str) {
      if (str != null) {
        sepStr = str;
        if (str.length() == 0) {
          // empty separator
          chars = new char[0];
        } else {
          chars = str.toCharArray();
        }
      }
      // always convert to bytes (encoding may have changed)
      bytes = sepStr.getBytes(encoding);
    }

    public String toString() {
      StringBuilder sb = new StringBuilder();
      for (char c : chars) {
        if (c == '\n') {
          sb.append("\\n");
        } else if (c == '\r') {
          sb.append("\\r");
        } else if (c == '\t') {
          sb.append("\\t");
        } else if (c >= 0x7f || c <= 0x20) {
          sb.append(String.format("\\u%04x", (int) c));
        } else {
          sb.append(c);
        }
      }
      return sb.toString();
    }

    protected void setFromConf(Configuration conf, String property) {
      setFromConf(conf, property, false);
    }

    protected void setFromConf(Configuration conf, String property,
        boolean isChar) {
      String str = conf.get(property);
      if (isChar && str != null && !str.isEmpty()) {
        LOG.warn("Separator " + property
            + " must be a char, only the first character '" + str.charAt(0)
            + "' of \"" + str + "\" is used");
        str = str.substring(0, 1);
      }
      set(str);
      LOG.info(property + " = " + toString());
    }

    /**
     * Get index of first occurrence of any separator characters.
     *
     * @param value
     *          String to scan
     * @param start
     *          position/index to start scan from
     * @return position of first occurrence or -1 (not found or empty separator)
     */
    protected int find(String value, int start) {
      if (chars.length == 0)
        return -1;
      if (chars.length == 1)
        return value.indexOf(chars[0], start);
      int index;
      for (char c : chars) {
        if ((index = value.indexOf(c, start)) >= 0) {
          return index;
        }
      }
      return -1;
    }
  }

  /** separator between records (rows) resp. documents */
  private Separator recordSeparator = new Separator("\r\n");
  public static final String CSV_RECORDSEPARATOR = "indexer.csv.recordsep";

  /** separator between fields (columns) */
  private Separator fieldSeparator = new Separator(",");
  public static final String CSV_FIELD_SEPARATOR = "indexer.csv.separator";

  /**
   * separator between multiple values of one field ({@link NutchField} allows
   * multiple values). Note: there is no escape for a valueSeparator, a character
   * not present in field data should be chosen.
   */
  private Separator valueSeparator = new Separator("|");
  public static final String CSV_VALUESEPARATOR = "indexer.csv.valuesep";

  /** quote character used to quote fields containing separators or quotes */
  private Separator quoteCharacter = new Separator("\"");
  public static final String CSV_QUOTECHARACTER = "indexer.csv.quotechar";

  /** escape character used to escape a quote character */
  private Separator escapeCharacter = quoteCharacter;
  public static final String CSV_ESCAPECHARACTER = "indexer.csv.escapechar";

  /** max. length of a field value */
  private int maxFieldLength = 4096;
  public static final String CSV_MAXFIELDLENGTH = "indexer.csv.maxfieldlength";

  /**
   * max. number of values of one field, useful for fields with potentially many
   * variant values, e.g., the "anchor" texts field
   */
  private int maxFieldValues = 12;
  public static final String CSV_MAXFIELDVALUES = "indexer.csv.maxfieldvalues";

  /** max. length of a field value */
  private boolean withHeader = true;
  public static final String CSV_WITHHEADER = "indexer.csv.header";

  /** output path / directory */
  private String outputPath = "csvindexwriter";
  public static final String CSV_OUTPATH = "indexer.csv.outpath";


  private static final String description =
      " - write index as CSV file (comma separated values)"
      + String.format("\n  %-24s : %s", CSV_FIELDS,
          "ordered list of fields (columns) in the CSV file")
      + String.format("\n  %-24s : %s", CSV_FIELD_SEPARATOR,
          "separator between fields (columns), default: , (U+002C, comma)")
      + String.format("\n  %-24s : %s", CSV_QUOTECHARACTER,
          "quote character used to quote fields containing separators or quotes, "
              + "default: \" (U+0022, quotation mark)")
      + String.format("\n  %-24s : %s", CSV_ESCAPECHARACTER,
          "escape character used to escape a quote character, "
              + "default: \" (U+0022, quotation mark)")
      + String.format("\n  %-24s : %s", CSV_RECORDSEPARATOR,
          "separator between records (rows) resp. documents, "
              + "default: \\r\\n (DOS-style line breaks)")
      + String.format("\n  %-24s : %s", CSV_VALUESEPARATOR,
          "separator between multiple values of one field, "
              + "default: | (U+007C)")
      + String.format("\n  %-24s : %s", CSV_MAXFIELDVALUES,
          "max. number of values of one field, useful for, "
              + " e.g., the anchor texts field, default: 12")
      + String.format("\n  %-24s : %s", CSV_MAXFIELDLENGTH,
          "max. length of a single field value in characters, default: 4096.")
      + String.format("\n  %-24s : %s", CSV_CHARSET,
          "encoding of CSV file, default: UTF-8")
      + String.format("\n  %-24s : %s", CSV_WITHHEADER,
          "write CSV column headers, default: true")
      + String.format("\n  %-24s : %s", CSV_OUTPATH,
          "output path / directory, default: csvindexwriter. "
          + "\n    CAVEAT: existing output directories are removed!") + "\n";


  private FileSystem fs;

  protected FSDataOutputStream csvout;

  private Path csvFSOutFile;
  private Path csvLocalOutFile;

  @Override
  public void open(JobConf job, String name) throws IOException {
    fs = FileSystem.get(job);
    LOG.info("Writing output to {}", outputPath);
    csvLocalOutFile = job.getLocalPath("tmp_" + System.currentTimeMillis() + "-"
        + Integer.toString(new Random().nextInt()) + "/nutch.csv");
    csvFSOutFile = new Path(outputPath, "nutch.csv");
    if (fs.exists(csvFSOutFile)) {
      // clean-up
      LOG.warn("Removing existing output path {}", csvFSOutFile);
      fs.delete(csvFSOutFile, true);
      FileOutputFormat.setOutputPath(job, csvFSOutFile);
      job.getOutputFormat().checkOutputSpecs(fs, job);

    }
    Path file = fs.startLocalOutput(csvFSOutFile, csvLocalOutFile);
    csvout = fs.create(file);
    if (withHeader) {
      for (int i = 0; i < fields.length; i++) {
        if (i > 0)
          csvout.write(fieldSeparator.bytes);
        csvout.write(fields[i].getBytes(encoding));
      }
    }
    csvout.write(recordSeparator.bytes);
  }

  @Override
  public void write(NutchDocument doc) throws IOException {
    for (int i = 0; i < fields.length; i++) {
      if (i > 0) {
        csvout.write(fieldSeparator.bytes);
      }
      NutchField field = doc.getField(fields[i]);
      if (field != null) {
        List<Object> values = field.getValues();
        int nValues = values.size();
        if (nValues > maxFieldValues) {
          nValues = maxFieldValues;
        }
        if (nValues > 1) {
          // always quote multi-value fields
          csvout.write(quoteCharacter.bytes);
        }
        ListIterator<Object> it = values.listIterator();
        int j = 0;
        while (it.hasNext() && j <= nValues) {
          Object objval = it.next();
          String value;
          if (objval == null) {
            continue;
          } else if (objval instanceof Date) {
            // date: format as "dow mon dd hh:mm:ss zzz yyyy"
            value = ((Date) objval).toString();
          } else {
            value = (String) objval;
          }
          if (nValues > 1) {
            // multi-value field
            writeEscaped(value);
            if (it.hasNext()) {
              csvout.write(valueSeparator.bytes);
            }
          } else {
            writeQuoted(value);
          }
        }
        if (nValues > 1) {
          // closing quote of multi-value fields
          csvout.write(quoteCharacter.bytes);
        }
      }
    }
    csvout.write(recordSeparator.bytes);
  }

  /** (deletion of documents is not supported) */
  @Override
  public void delete(String key) throws IOException {
  }

  @Override
  public void update(NutchDocument doc) throws IOException {
    write(doc);
  }

  @Override
  public void close() throws IOException {
    csvout.close();
    fs.completeLocalOutput(csvFSOutFile, csvLocalOutFile);
    LOG.info("Finished CSV index in {}", csvFSOutFile);
  }

  /** (nothing to commit) */
  @Override
  public void commit() throws IOException {
  }

  @Override
  public Configuration getConf() {
    return config;
  }

  @Override
  public String describe() {
    return getClass().getSimpleName() + description;
  }

  @Override
  public void setConf(Configuration conf) {
    config = conf;
    outputPath = conf.get(CSV_OUTPATH, outputPath);
    String charset = conf.get(CSV_CHARSET);
    if (charset != null) {
      encoding = Charset.forName(charset);
    }
    fieldSeparator.setFromConf(conf, CSV_FIELD_SEPARATOR);
    quoteCharacter.setFromConf(conf, CSV_QUOTECHARACTER, true);
    escapeCharacter.setFromConf(conf, CSV_ESCAPECHARACTER, true);
    recordSeparator.setFromConf(conf, CSV_RECORDSEPARATOR);
    valueSeparator.setFromConf(conf, CSV_VALUESEPARATOR);
    withHeader = conf.getBoolean(CSV_WITHHEADER, true);
    maxFieldLength = conf.getInt(CSV_MAXFIELDLENGTH, maxFieldLength);
    LOG.info(CSV_MAXFIELDLENGTH + " = " + maxFieldLength);
    maxFieldValues = conf.getInt(CSV_MAXFIELDVALUES, maxFieldValues);
    LOG.info(CSV_MAXFIELDVALUES + " = " + maxFieldValues);
    fields = conf.getStrings(CSV_FIELDS, "id", "title", "content");
    LOG.info("fields =");
    for (String f : fields) {
      LOG.info("\t" + f);
    }
  }

  /** Write a value to output stream. If necessary use quote characters. */
  private void writeQuoted (String value) throws IOException {
    int nextQuoteChar;
    if (quoteCharacter.chars.length > 0
        && (((nextQuoteChar = quoteCharacter.find(value, 0)) >= 0) 
            || (fieldSeparator.find(value, 0) >= 0) 
            || (recordSeparator.find(value, 0) >= 0))) {
      // need quotes
      csvout.write(quoteCharacter.bytes);
      writeEscaped(value, nextQuoteChar);
      csvout.write(quoteCharacter.bytes);
    } else {
      if (value.length() > maxFieldLength) {
        csvout.write(value.substring(0, maxFieldLength).getBytes(encoding));
      } else {
        csvout.write(value.getBytes(encoding));
      }
    }
  }

  /**
   * Write a value to output stream. Escape quote characters.
   * Clip value after <code>indexer.csv.maxfieldlength</code> characters.
   *
   * @param value
   *          String to write
   * @param nextQuoteChar
   *          (first) occurrence of the quote character
   */
  private void writeEscaped (String value, int nextQuoteChar) throws IOException {
    int start = 0;
    int max = value.length();
    if (max > maxFieldLength) {
      max = maxFieldLength;
    }
    while (nextQuoteChar > 0 && nextQuoteChar < max) {
      csvout.write(value.substring(start, nextQuoteChar).getBytes(encoding));
      csvout.write(escapeCharacter.bytes);
      csvout.write(quoteCharacter.bytes);
      start = nextQuoteChar + 1;
      nextQuoteChar = quoteCharacter.find(value, start);
      if (nextQuoteChar > max) break;
    }
    csvout.write(value.substring(start, max).getBytes(encoding));
  }

  /**
   * Write a value to output stream. Escape quote characters. Clip value after
   * <code>indexer.csv.maxfieldlength</code> characters.
   */
  private void writeEscaped (String value) throws IOException {
    int nextQuoteChar = quoteCharacter.find(value, 0);
    writeEscaped(value, nextQuoteChar);
  }

  public static void main(String[] args) throws Exception {
    final int res = ToolRunner.run(NutchConfiguration.create(),
            new IndexingJob(), args);
    System.exit(res);
  }

  @Override
  public String describe() {
    return getClass().getSimpleName() + description;
  }

}
