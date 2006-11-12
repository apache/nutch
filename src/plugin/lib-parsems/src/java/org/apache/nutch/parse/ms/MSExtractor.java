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
package org.apache.nutch.parse.ms;

// JDK imports
import java.io.InputStream;
import java.util.Date;
import java.util.Properties;

// Commons Logging imports
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

// Nutch imports
import org.apache.nutch.metadata.DublinCore;
import org.apache.nutch.metadata.HttpHeaders;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.metadata.Office;
import org.apache.nutch.net.protocols.HttpDateFormat;
import org.apache.nutch.util.StringUtil;

// Jakarta POI imports
import org.apache.poi.hpsf.PropertySetFactory;
import org.apache.poi.hpsf.SummaryInformation;
import org.apache.poi.poifs.eventfilesystem.POIFSReader;
import org.apache.poi.poifs.eventfilesystem.POIFSReaderEvent;
import org.apache.poi.poifs.eventfilesystem.POIFSReaderListener;


/**
 * Defines a Microsoft document content extractor.
 *
 * @author J&eacute;r&ocirc;me Charron
 */
public abstract class MSExtractor {
  
  protected final static Log LOG = LogFactory.getLog(MSExtractor.class);

  private String text = null;
  private POIFSReader reader = null;
  private PropertiesBroker properties = null;
  

  /** Constructs a new Microsoft document extractor. */
  protected MSExtractor() { }

  
  /**
   * Extracts properties and text from an MS Document input stream
   */
  protected void extract(InputStream input) throws Exception {
    // First, extract properties
    this.reader = new POIFSReader();
    this.properties = new PropertiesBroker();
    this.reader.registerListener(
            new PropertiesReaderListener(this.properties),
            SummaryInformation.DEFAULT_STREAM_NAME);
    input.reset();
    if (input.available() > 0) {
      reader.read(input);
    }
    // Then, extract text
    input.reset();
    this.text = extractText(input);
  }

  /**
   * Extracts the text content from a Microsoft document input stream.
   */
  protected abstract String extractText(InputStream input) throws Exception;
  
  
  /**
   * Get the content text of the Microsoft document.
   * @return the content text of the document
   */
  protected String getText() {
    return this.text;
  }
  

  /**
   * Get the <code>Properties</code> of the Microsoft document.
   * @return the properties of the document
   */
  protected Properties getProperties() {
    return properties.getProperties();
  }

  
  private final static class PropertiesBroker {

    private final static int TIMEOUT = 2 * 1000;
    private Properties properties = null;

    public synchronized Properties getProperties() {

      final long start = new Date().getTime();
      long now = start;

      while (this.properties == null && now - start < TIMEOUT) {
        try {
          wait(TIMEOUT / 10);
        } catch (InterruptedException e) {
        }
        now = new Date().getTime();
      }
      notifyAll();
      return this.properties;
    }

    public synchronized void setProperties(Properties properties) {
      this.properties = properties;
      notifyAll();
    }
  }
  
  
  private class PropertiesReaderListener implements POIFSReaderListener {
    
    private PropertiesBroker propertiesBroker;
    private Properties metadata = new Properties();
    
    PropertiesReaderListener(PropertiesBroker propertiesBroker) {
      this.propertiesBroker = propertiesBroker;
    }
    
    public void processPOIFSReaderEvent(POIFSReaderEvent event) {
      if (!event.getName().startsWith(SummaryInformation.DEFAULT_STREAM_NAME)) {
        return;
      }
      
      try {
        SummaryInformation si = (SummaryInformation)
                                  PropertySetFactory.create(event.getStream());
        setProperty(DublinCore.TITLE, si.getTitle());
        setProperty(Office.APPLICATION_NAME, si.getApplicationName());
        setProperty(Office.AUTHOR, si.getAuthor());
        setProperty(Office.CHARACTER_COUNT, si.getCharCount());
        setProperty(Office.COMMENTS, si.getComments());
        setProperty(DublinCore.DATE, si.getCreateDateTime());
//        setProperty(Office.EDIT_TIME, si.getEditTime());
        setProperty(HttpHeaders.LAST_MODIFIED, si.getLastSaveDateTime());
        setProperty(Office.KEYWORDS, si.getKeywords());
        setProperty(Office.LAST_AUTHOR, si.getLastAuthor());
        setProperty(Office.LAST_PRINTED, si.getLastPrinted());
        setProperty(Office.LAST_SAVED, si.getLastSaveDateTime());
        setProperty(Office.PAGE_COUNT, si.getPageCount());
        setProperty(Office.REVISION_NUMBER, si.getRevNumber());
        setProperty(DublinCore.RIGHTS, si.getSecurity());
        setProperty(DublinCore.SUBJECT, si.getSubject());
        setProperty(Office.TEMPLATE, si.getTemplate());
        setProperty(Office.WORD_COUNT, si.getWordCount());
      } catch (Exception ex) {
      }
      propertiesBroker.setProperties(metadata);
    }
    
    private final void setProperty(String name, String value) {
      if (!StringUtil.isEmpty(name) && !StringUtil.isEmpty(value)) {
        metadata.setProperty(name, value);
      }
    }

    private final void setProperty(String name, int value) {
      if (value != 0) {
        setProperty(name, String.valueOf(value));
      }
    }

    private final void setProperty(String name, long value) {
      if (value != 0) {
        setProperty(name, String.valueOf(value));
      }
    }

    private final void setProperty(String name, Date date) {
      if (date != null) {
        setProperty(name, HttpDateFormat.toString(date));
      }
    }

  }
  
}
