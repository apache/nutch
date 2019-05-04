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
package org.apache.nutch.indexer;

import de.vandermeer.asciitable.AT_ColumnWidthCalculator;
import de.vandermeer.asciitable.AT_Row;
import de.vandermeer.asciitable.AsciiTable;
import de.vandermeer.skb.interfaces.document.TableRowType;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.exchange.Exchanges;
import org.apache.nutch.plugin.Extension;
import org.apache.nutch.plugin.ExtensionPoint;
import org.apache.nutch.plugin.PluginRepository;
import org.apache.nutch.plugin.PluginRuntimeException;
import org.apache.nutch.util.NutchConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.util.*;

/**
 * Creates and caches {@link IndexWriter} implementing plugins.
 */
public class IndexWriters {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  private static final WeakHashMap<String, IndexWriters> CACHE = new WeakHashMap<>();

  public static synchronized IndexWriters get(Configuration conf) {
    String uuid = NutchConfiguration.getUUID(conf);
    if (uuid == null) {
      uuid = "nonNutchConf@" + conf.hashCode(); // fallback
    }
    return CACHE.computeIfAbsent(uuid, k -> new IndexWriters(conf));
  }

  private HashMap<String, IndexWriterWrapper> indexWriters;

  private Exchanges exchanges;

  private IndexWriters(Configuration conf) {
    //It's not cached yet
    if (this.indexWriters == null) {
      try {
        ExtensionPoint point = PluginRepository.get(conf)
            .getExtensionPoint(IndexWriter.X_POINT_ID);

        if (point == null) {
          throw new RuntimeException(IndexWriter.X_POINT_ID + " not found.");
        }

        Extension[] extensions = point.getExtensions();

        HashMap<String, Extension> extensionMap = new HashMap<>();
        for (Extension extension : extensions) {
          LOG.info("Index writer {} identified.", extension.getClazz());
          extensionMap.putIfAbsent(extension.getClazz(), extension);
        }

        IndexWriterConfig[] indexWriterConfigs = loadWritersConfiguration(conf);
        this.indexWriters = new HashMap<>();

        for (IndexWriterConfig indexWriterConfig : indexWriterConfigs) {
          final String clazz = indexWriterConfig.getClazz();

          //If was enabled in plugin.includes property
          if (extensionMap.containsKey(clazz)) {
            IndexWriterWrapper writerWrapper = new IndexWriterWrapper();
            writerWrapper.setIndexWriterConfig(indexWriterConfig);
            writerWrapper.setIndexWriter(
                (IndexWriter) extensionMap.get(clazz).getExtensionInstance());

            indexWriters.put(indexWriterConfig.getId(), writerWrapper);
          }
        }

        this.exchanges = new Exchanges(conf);
        this.exchanges.open();
      } catch (PluginRuntimeException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Loads the configuration of index writers.
   *
   * @param conf Nutch configuration instance.
   */
  private IndexWriterConfig[] loadWritersConfiguration(Configuration conf) {
    InputStream ssInputStream = conf
        .getConfResourceAsInputStream("index-writers.xml");
    InputSource inputSource = new InputSource(ssInputStream);

    try {
      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      DocumentBuilder builder = factory.newDocumentBuilder();
      Document document = builder.parse(inputSource);
      Element rootElement = document.getDocumentElement();
      NodeList writerList = rootElement.getElementsByTagName("writer");

      IndexWriterConfig[] indexWriterConfigs = new IndexWriterConfig[writerList
          .getLength()];

      for (int i = 0; i < writerList.getLength(); i++) {
        indexWriterConfigs[i] = IndexWriterConfig
            .getInstanceFromElement((Element) writerList.item(i));
      }

      return indexWriterConfigs;
    } catch (SAXException | IOException | ParserConfigurationException e) {
      LOG.warn(e.toString());
      return new IndexWriterConfig[0];
    }
  }

  /**
   * Maps the fields of a given document.
   *
   * @param document The document to map.
   * @param mapping  The mapping to apply.
   * @return The mapped document.
   */
  private NutchDocument mapDocument(final NutchDocument document,
      final Map<MappingReader.Actions, Map<String, List<String>>> mapping) {
    try {
      NutchDocument mappedDocument = document.clone();

      mapping.get(MappingReader.Actions.COPY).forEach((key, value) -> {
        //Checking whether the field to copy exists or not
        if (mappedDocument.getField(key) != null) {
          for (String field : value) {
            //To avoid duplicate the values
            if (!key.equals(field)) {
              for (Object val : mappedDocument.getField(key).getValues()) {
                mappedDocument.add(field, val);
              }
            }
          }
        }
      });

      mapping.get(MappingReader.Actions.RENAME).forEach((key, value) -> {
        //Checking whether the field to rename exists or not
        if (mappedDocument.getField(key) != null) {
          NutchField field = mappedDocument.removeField(key);
          mappedDocument.add(value.get(0), field.getValues());
          mappedDocument.getField(value.get(0)).setWeight(field.getWeight());
        }
      });

      mapping.get(MappingReader.Actions.REMOVE)
          .forEach((key, value) -> mappedDocument.removeField(key));

      return mappedDocument;
    } catch (CloneNotSupportedException e) {
      LOG.warn("An instance of class {} can't be cloned.",
          document.getClass().getName());
      return document;
    }
  }

  /**
   * Ensures if there are not available exchanges, the document will be routed to all configured index writers.
   *
   * @param doc Document to process.
   * @return Index writers IDs.
   */
  private Collection<String> getIndexWriters(NutchDocument doc) {
    if (this.exchanges.areAvailableExchanges()) {
      return Arrays.asList(this.exchanges.indexWriters(doc));
    }
    return this.indexWriters.keySet();
  }

  /**
   * Initializes the internal variables of index writers.
   *
   * @param conf Nutch configuration.
   * @param name
   * @throws IOException Some exception thrown by some writer.
   */
  public void open(Configuration conf, String name) throws IOException {
    for (Map.Entry<String, IndexWriterWrapper> entry : this.indexWriters
        .entrySet()) {
      entry.getValue().getIndexWriter().open(conf, name);
      entry.getValue().getIndexWriter()
          .open(entry.getValue().getIndexWriterConfig().getParams());
    }
  }

  public void write(NutchDocument doc) throws IOException {
    for (String indexWriterId : getIndexWriters(doc)) {
      NutchDocument mappedDocument = mapDocument(doc,
          this.indexWriters.get(indexWriterId).getIndexWriterConfig()
              .getMapping());
      this.indexWriters.get(indexWriterId).getIndexWriter()
          .write(mappedDocument);
    }
  }

  public void update(NutchDocument doc) throws IOException {
    for (String indexWriterId : getIndexWriters(doc)) {
      NutchDocument mappedDocument = mapDocument(doc,
          this.indexWriters.get(indexWriterId).getIndexWriterConfig()
              .getMapping());
      this.indexWriters.get(indexWriterId).getIndexWriter()
          .update(mappedDocument);
    }
  }

  public void delete(String key) throws IOException {
    for (IndexWriterWrapper iww : indexWriters.values()) {
      iww.getIndexWriter().delete(key);
    }
  }

  public void close() throws IOException {
    for (Map.Entry<String, IndexWriterWrapper> entry : this.indexWriters
        .entrySet()) {
      entry.getValue().getIndexWriter().close();
    }
  }

  public void commit() throws IOException {
    for (Map.Entry<String, IndexWriterWrapper> entry : this.indexWriters
        .entrySet()) {
      entry.getValue().getIndexWriter().commit();
    }
  }

  /**
   * Lists the active IndexWriters and their configuration.
   *
   * @return The full description.
   */
  public String describe() {
    StringBuilder builder = new StringBuilder();
    if (this.indexWriters.size() == 0)
      builder.append("No IndexWriters activated - check your configuration\n");
    else
      builder.append("Active IndexWriters :\n");

    for (IndexWriterWrapper indexWriterWrapper : this.indexWriters.values()) {
      // Getting the class name
      builder.append(
          indexWriterWrapper.getIndexWriter().getClass().getSimpleName())
          .append(":\n");

      // Building the table
      AsciiTable at = new AsciiTable();
      at.getRenderer().setCWC((rows, colNumbers, tableWidth) -> {
        int maxLengthFirstColumn = 0;
        int maxLengthLastColumn = 0;
        for (AT_Row row : rows) {
          if (row.getType() == TableRowType.CONTENT) {
            // First column
            int lengthFirstColumn = row.getCells().get(0).toString().length();
            if (lengthFirstColumn > maxLengthFirstColumn) {
              maxLengthFirstColumn = lengthFirstColumn;
            }

            // Last column
            int lengthLastColumn = row.getCells().get(2).toString().length();
            if (lengthLastColumn > maxLengthLastColumn) {
              maxLengthLastColumn = lengthLastColumn;
            }
          }
        }
        return new int[] { maxLengthFirstColumn,
            tableWidth - maxLengthFirstColumn - maxLengthLastColumn,
            maxLengthLastColumn };
      });

      // Getting the properties
      Map<String, Map.Entry<String, Object>> properties = indexWriterWrapper
          .getIndexWriter().describe();

      // Adding the rows
      properties.forEach((key, value) -> {
        at.addRule();
        at.addRow(key, value.getKey(),
            value.getValue() != null ? value.getValue() : "");
      });

      // Last rule
      at.addRule();

      // Rendering the table
      builder.append(at.render(150)).append("\n\n");
    }

    return builder.toString();
  }

  public class IndexWriterWrapper {
    private IndexWriterConfig indexWriterConfig;

    private IndexWriter indexWriter;

    IndexWriterConfig getIndexWriterConfig() {
      return indexWriterConfig;
    }

    void setIndexWriterConfig(IndexWriterConfig indexWriterConfig) {
      this.indexWriterConfig = indexWriterConfig;
    }

    IndexWriter getIndexWriter() {
      return indexWriter;
    }

    void setIndexWriter(IndexWriter indexWriter) {
      this.indexWriter = indexWriter;
    }
  }
}
