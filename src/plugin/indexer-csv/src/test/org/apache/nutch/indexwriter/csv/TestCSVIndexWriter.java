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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.HashMap;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.nutch.indexer.IndexWriterParams;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.util.NutchConfiguration;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test CSVIndexWriter. Focus is on CSV-specific potential issues, mainly quoting and escaping.
 */
public class TestCSVIndexWriter {

  protected static final Logger LOG = LoggerFactory
      .getLogger(TestCSVIndexWriter.class);

  /**
   * Dummy IndexWriter which stores the indexed documents as CSV string in a
   * {@link ByteArrayOutputStream} which can be easily accessed in test cases.
   */
  public class CSVByteArrayIndexWriter extends CSVIndexWriter {

    ByteArrayOutputStream byteBuffer;
    FileSystem.Statistics fsStats;

    @Override
    public void open(IndexWriterParams parameters) throws IOException {
      super.open(parameters);
      byteBuffer = new ByteArrayOutputStream();
      fsStats = new FileSystem.Statistics("testCSVIndexWriter");
      csvout = new FSDataOutputStream(byteBuffer, fsStats);
    }

    @Override
    public void close() throws IOException {
    }

    /** get the indexed documents as CSV */
    public String getData() {
      try {
        return byteBuffer.toString(encoding.name());
      } catch (UnsupportedEncodingException e) {
        return "";
      }
    }
  }

  /**
   * write one NutchDocument as CSV record
   *
   * @param configParams configuration parameters: array (property => value, prop2 => value)
   * @param docs         NutchDocument
   * @return CSV string representing the document
   */
  private String getCSV(final String[] configParams, NutchDocument[] docs)
      throws IOException {
    Configuration conf = NutchConfiguration.create();
    IndexWriterParams params = new IndexWriterParams(new HashMap<>());
    for (int i = 0; i < configParams.length; i += 2) {
      params.put(configParams[i], configParams[i + 1]);
    }
    CSVByteArrayIndexWriter out = new CSVByteArrayIndexWriter();
    out.setConf(conf);
    out.open(params);
    for (NutchDocument doc : docs) {
      out.write(doc);
    }
    out.close();
    String csv = out.getData();
    LOG.info(csv);
    return csv;
  }

  /**
   * write one document as CSV record
   *
   * @param configParams configuration parameters: array (property => value, prop2 => value)
   * @param fieldContent array of {field => value} maps
   * @return CSV string representing the document
   */
  private String getCSV(final String[] configParams, final String[] fieldContent)
      throws IOException {
    NutchDocument[] docs = new NutchDocument[1];
    docs[0] = new NutchDocument();
    for (int i = 0; i < fieldContent.length; i += 2) {
      docs[0].add(fieldContent[i], fieldContent[i + 1]);
    }
    return getCSV(configParams, docs);
  }

  /** defaults, no quoting necessary */
  @Test
  public void testCSVdefault() throws IOException {
    String[] fields = { "id", "http://nutch.apache.org/", "title",
        "Welcome to Apache Nutch", "content",
        "Apache Nutch is an open source web-search software project. ..." };
    String csv = getCSV(new String[0], fields);
    for (int i = 0; i < fields.length; i += 2) {
      assertTrue("Testing field " + i + " (" + fields[i] + ")",
          csv.contains(fields[i + 1]));
    }
  }

  @Test
  public void testCSVquoteFieldSeparators() throws IOException {
    String[] params = { CSVConstants.CSV_FIELDS, "test,test2" };
    String[] fields = { "test", "a,b", "test2", "c,d" };
    String csv = getCSV(params, fields);
    assertEquals("If field contains a fields separator, it must be quoted",
        "\"a,b\",\"c,d\"", csv.trim());
  }

  @Test
  public void testCSVquoteRecordSeparators() throws IOException {
    String[] params = { CSVConstants.CSV_FIELDS, "test" };
    String[] fields = { "test", "a\nb" };
    String csv = getCSV(params, fields);
    assertEquals("If field contains a fields separator, it must be quoted",
        "\"a\nb\"", csv.trim());
  }

  @Test
  public void testCSVescapeQuotes() throws IOException {
    String[] params = { CSVConstants.CSV_FIELDS, "test" };
    String[] fields = { "test", "a,b:\"quote\",c" };
    String csv = getCSV(params, fields);
    assertEquals("Quotes inside a quoted field must be escaped",
        "\"a,b:\"\"quote\"\",c\"", csv.trim());
  }

  @Test
  public void testCSVclipMaxLength() throws IOException {
    String[] params = { CSVConstants.CSV_FIELDS, "test",
        CSVConstants.CSV_MAXFIELDLENGTH, "8" };
    String[] fields = { "test", "0123456789" };
    String csv = getCSV(params, fields);
    assertEquals("Field clipped to max. length = 8", "01234567", csv.trim());
  }

  @Test
  public void testCSVclipMaxLengthQuote() throws IOException {
    String[] params = { CSVConstants.CSV_FIELDS, "test",
        CSVConstants.CSV_MAXFIELDLENGTH, "7" };
    String[] fields = { "test", "1,\"2\",3,\"4\"" };
    String csv = getCSV(params, fields);
    assertEquals("Field clipped to max. length = 7", "\"1,\"\"2\"\",3\"",
        csv.trim());
  }

  @Test
  public void testCSVmultiValueFields() throws IOException {
    String[] params = { CSVConstants.CSV_FIELDS, "test",
        CSVConstants.CSV_VALUESEPARATOR, "|",
        CSVConstants.CSV_QUOTECHARACTER, "" };
    String[] fields = { "test", "abc", "test", "def" };
    String csv = getCSV(params, fields);
    assertEquals("Values of multi-value fields are concatenated by |",
        "abc|def", csv.trim());
  }

  @Test
  public void testCSVEncoding() throws IOException {
    String[] charsets = { "iso-8859-1",
        "\u00e4\u00f6\u00fc\u00df\u00e9\u00f4\u00ee", // äöüßéôî
        "iso-8859-2", "\u0161\u010d\u0159\u016f", // ščřů
        "iso-8859-5", "\u0430\u0441\u0434\u0444", // асдф
    };
    for (int i = 0; i < charsets.length; i += 2) {
      String charset = charsets[i];
      String test = charsets[i + 1];
      String[] params = { CSVConstants.CSV_FIELDS, "test",
          CSVConstants.CSV_CHARSET, charset };
      String[] fields = { "test", test };
      String csv = getCSV(params, fields);
      assertEquals("wrong charset conversion", test, csv.trim());
    }
  }

  /** test non-ASCII separator */
  @Test
  public void testCSVEncodingSeparator() throws IOException {
    String[] params = { CSVConstants.CSV_FIELDS, "test",
        CSVConstants.CSV_CHARSET, "iso-8859-1",
        CSVConstants.CSV_VALUESEPARATOR, "\u00a6", // ¦ (broken bar)
        CSVConstants.CSV_QUOTECHARACTER, ""
    };
    String[] fields = { "test", "abc", "test", "def" };
    String csv = getCSV(params, fields);
    assertEquals("Values of multi-value fields are concatenated by ¦",
        "abc\u00a6def", csv.trim());
  }

  @Test
  public void testCSVtabSeparated() throws IOException {
    String[] params = { CSVConstants.CSV_FIELDS, "1,2,3",
        CSVConstants.CSV_FIELD_SEPARATOR, "\t",
        CSVConstants.CSV_QUOTECHARACTER, ""
    };
    NutchDocument[] docs = new NutchDocument[2];
    docs[0] = new NutchDocument();
    docs[0].add("1", "a");
    docs[0].add("1", "b");
    docs[0].add("2", "a\"2\"b");
    docs[0].add("3", "c,d");
    docs[1] = new NutchDocument();
    docs[1].add("1", "A");
    docs[1].add("2", "B");
    docs[1].add("3", "C");
    String csv = getCSV(params, docs);
    String[] records = csv.trim().split("\\r\\n");
    assertEquals("tab-separated output", "a|b\ta\"2\"b\tc,d", records[0]);
    assertEquals("tab-separated output", "A\tB\tC", records[1]);
  }

  @Test
  public void testCSVdateField() throws IOException {
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    String[] params = { CSVConstants.CSV_FIELDS, "date" };
    NutchDocument[] docs = new NutchDocument[1];
    docs[0] = new NutchDocument();
    docs[0].add("date", new Date(0)); // 1970-01-01
    String csv = getCSV(params, docs);
    assertTrue("date conversion", csv.contains("1970"));
  }
}

