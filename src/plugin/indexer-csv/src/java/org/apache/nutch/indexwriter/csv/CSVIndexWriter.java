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
import java.util.AbstractMap;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.indexer.IndexWriter;
import org.apache.nutch.indexer.IndexWriterParams;
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
 * 
 * <p>
 * Note: works only in local mode, to be used with index option
 * <code>-noCommit</code>.
 * </p>
 */
public class CSVIndexWriter implements IndexWriter {

  public static final Logger LOG = LoggerFactory
      .getLogger(CSVIndexWriter.class);

  private Configuration config;

  /** ordered list of fields (columns) in the CSV file */
  private String[] fields;

  /** encoding of CSV file */
  protected Charset encoding = Charset.forName("UTF-8");

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

    protected void setFromConf(IndexWriterParams parameters, String property) {
      setFromConf(parameters, property, false);
    }

    protected void setFromConf(IndexWriterParams parameters, String property,
        boolean isChar) {
      String str = parameters.get(property);
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

  /** separator between fields (columns) */
  private Separator fieldSeparator = new Separator(",");

  /**
   * separator between multiple values of one field ({@link NutchField} allows
   * multiple values). Note: there is no escape for a valueSeparator, a character
   * not present in field data should be chosen.
   */
  private Separator valueSeparator = new Separator("|");

  /** quote character used to quote fields containing separators or quotes */
  private Separator quoteCharacter = new Separator("\"");

  /** escape character used to escape a quote character */
  private Separator escapeCharacter = quoteCharacter;

  /** max. length of a field value */
  private int maxFieldLength = 4096;

  /**
   * max. number of values of one field, useful for fields with potentially many
   * variant values, e.g., the "anchor" texts field
   */
  private int maxFieldValues = 12;

  /** max. length of a field value */
  private boolean withHeader = true;

  /** output path / directory */
  private String outputPath = "csvindexwriter";


  private FileSystem fs;

  protected FSDataOutputStream csvout;

  private Path csvLocalOutFile;

  @Override
  public void open(Configuration conf, String name) throws IOException {

  }

  /**
   * Initializes the internal variables from a given index writer configuration.
   *
   * @param parameters Params from the index writer configuration.
   * @throws IOException Some exception thrown by writer.
   */
  @Override
  public void open(IndexWriterParams parameters) throws IOException {
    outputPath = parameters.get(CSVConstants.CSV_OUTPATH, outputPath);
    String charset = parameters.get(CSVConstants.CSV_CHARSET);
    if (charset != null) {
      encoding = Charset.forName(charset);
    }
    fieldSeparator.setFromConf(parameters, CSVConstants.CSV_FIELD_SEPARATOR);
    quoteCharacter.setFromConf(parameters, CSVConstants.CSV_QUOTECHARACTER, true);
    escapeCharacter.setFromConf(parameters, CSVConstants.CSV_ESCAPECHARACTER, true);
    valueSeparator.setFromConf(parameters, CSVConstants.CSV_VALUESEPARATOR);
    withHeader = parameters.getBoolean(CSVConstants.CSV_WITHHEADER, true);
    maxFieldLength = parameters.getInt(CSVConstants.CSV_MAXFIELDLENGTH, maxFieldLength);
    LOG.info(CSVConstants.CSV_MAXFIELDLENGTH + " = " + maxFieldLength);
    maxFieldValues = parameters.getInt(CSVConstants.CSV_MAXFIELDVALUES, maxFieldValues);
    LOG.info(CSVConstants.CSV_MAXFIELDVALUES + " = " + maxFieldValues);
    fields = parameters.getStrings(CSVConstants.CSV_FIELDS, "id", "title", "content");
    LOG.info("fields =");
    for (String f : fields) {
      LOG.info("\t" + f);
    }

    fs = FileSystem.get(config);
    LOG.info("Writing output to {}", outputPath);
    Path outputDir = new Path(outputPath);
    fs = outputDir.getFileSystem(config);
    csvLocalOutFile = new Path(outputDir, "nutch.csv");
    if (!fs.exists(outputDir)) {
      fs.mkdirs(outputDir);
    }
    if (fs.exists(csvLocalOutFile)) {
      // clean-up
      LOG.warn("Removing existing output path {}", csvLocalOutFile);
      fs.delete(csvLocalOutFile, true);
    }
    csvout = fs.create(csvLocalOutFile);
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
            value = objval.toString();
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
  public void delete(String key) {
  }

  @Override
  public void update(NutchDocument doc) throws IOException {
    write(doc);
  }

  @Override
  public void close() throws IOException {
    csvout.close();
    LOG.info("Finished CSV index in {}", csvLocalOutFile);
  }

  /** (nothing to commit) */
  @Override
  public void commit() {
  }

  @Override
  public Configuration getConf() {
    return config;
  }

  /**
   * Returns {@link Map} with the specific parameters the IndexWriter instance can take.
   *
   * @return The values of each row. It must have the form <KEY,<DESCRIPTION,VALUE>>.
   */
  @Override
  public Map<String, Map.Entry<String, Object>> describe() {
    Map<String, Map.Entry<String, Object>> properties = new LinkedHashMap<>();

    properties.put(CSVConstants.CSV_FIELDS, new AbstractMap.SimpleEntry<>(
        "Ordered list of fields (columns) in the CSV file",
        this.fields == null ? "" : String.join(",", this.fields)));
    properties.put(CSVConstants.CSV_FIELD_SEPARATOR, new AbstractMap.SimpleEntry<>(
        "Separator between fields (columns), default: , (U+002C, comma)",
        this.fieldSeparator));
    properties.put(CSVConstants.CSV_QUOTECHARACTER, new AbstractMap.SimpleEntry<>(
        "Quote character used to quote fields containing separators or quotes, default: \" (U+0022, quotation mark)",
        this.quoteCharacter));
    properties.put(CSVConstants.CSV_ESCAPECHARACTER, new AbstractMap.SimpleEntry<>(
        "Escape character used to escape a quote character, default: \" (U+0022, quotation mark)",
        this.escapeCharacter));
    properties.put(CSVConstants.CSV_VALUESEPARATOR, new AbstractMap.SimpleEntry<>(
        "Separator between multiple values of one field, default: | (U+007C)",
        this.valueSeparator));
    properties.put(CSVConstants.CSV_MAXFIELDVALUES, new AbstractMap.SimpleEntry<>(
        "Max. number of values of one field, useful for, e.g., the anchor texts field, default: 12",
        this.maxFieldValues));
    properties.put(CSVConstants.CSV_MAXFIELDLENGTH, new AbstractMap.SimpleEntry<>(
        "Max. length of a single field value in characters, default: 4096",
        this.maxFieldLength));
    properties.put(CSVConstants.CSV_CHARSET, new AbstractMap.SimpleEntry<>(
        "Encoding of CSV file, default: UTF-8",
        this.encoding));
    properties.put(CSVConstants.CSV_WITHHEADER, new AbstractMap.SimpleEntry<>(
        "Write CSV column headers, default: true",
        this.withHeader));
    properties.put(CSVConstants.CSV_OUTPATH, new AbstractMap.SimpleEntry<>(
        "Output path / directory, default: csvindexwriter. ",
        this.outputPath));

    return properties;
  }

  @Override
  public void setConf(Configuration conf) {
    config = conf;
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
   * Clip value after <code>maxfieldlength</code> characters.
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
   * <code>maxfieldlength</code> characters.
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

}
