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
package org.apache.nutch.parse.mspowerpoint;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.TimeZone;
import java.util.logging.Logger;

import org.apache.nutch.parse.mspowerpoint.PPTExtractor.PropertiesBroker;
import org.apache.hadoop.util.LogFormatter;
import org.apache.poi.hpsf.PropertySetFactory;
import org.apache.poi.hpsf.SummaryInformation;
import org.apache.poi.poifs.eventfilesystem.POIFSReaderEvent;
import org.apache.poi.poifs.eventfilesystem.POIFSReaderListener;

/**
 * Listener for retrieving the properties of document.
 * 
 * @author Stephan Strittmatter
 * 
 * @version 1.0
 */
class PropertiesReaderListener implements POIFSReaderListener {
  private static final Logger LOG = LogFormatter
      .getLogger(PropertiesReaderListener.class.getName());

  private static final String TIME_ZONE_ID = "GTM";

  private final transient PropertiesBroker propertiesBroker;

  /** DateFormatter for transfereing dates do strings. */
  private final transient SimpleDateFormat dateFormatter = new SimpleDateFormat();

  /** Properties of the powerpoint Document */
  private final transient Properties properties;

  /**
   * Listener for retrieving the properties of document.
   * 
   * @param propertiesBroker
   */
  public PropertiesReaderListener(final PropertiesBroker propertiesBroker) {
    this.propertiesBroker = propertiesBroker;
    this.dateFormatter.setTimeZone(TimeZone.getTimeZone(TIME_ZONE_ID));
    this.properties = new Properties();
  }

  /**
   * Process the properties of the document and adds them to property object.
   * 
   * @param event
   *          contains the document to be parsed
   */
  public void processPOIFSReaderEvent(POIFSReaderEvent event) {

    if (event.getName().startsWith(SummaryInformation.DEFAULT_STREAM_NAME)) {

      try {
        final SummaryInformation sInfo = (SummaryInformation) PropertySetFactory
            .create(event.getStream());

        addProperty("Title", sInfo.getTitle());
        addProperty("Subject", sInfo.getSubject());
        addProperty("Keywords", sInfo.getKeywords());
        addProperty("Comments", sInfo.getComments());
        addProperty("Author", sInfo.getAuthor());
        addProperty("Last-Author", sInfo.getLastAuthor());

        /*
         * already provided by nutch
         */
        // addProperty("Saved-Date", si.getLastSaveDateTime());
        /*
         * following properties are not required for indexing/searching
         */
        // addProperty("Word-Count", si.getWordCount());
        // addProperty("Page-Count", si.getPageCount());
        // addProperty("Character Count", si.getCharCount());
        // addProperty("Revision-Number", si.getRevNumber());
        // addProperty("Creation-Date", si.getEditTime());
        // addProperty("Edit-Time", si.getEditTime());
        // addProperty("Last-Printed", si.getLastPrinted());
        // addProperty("Template", si.getTemplate());
        // addProperty("Security", si.getSecurity());
        // addProperty("Application-Name", si.getApplicationName());
      } catch (Exception ex) {
        LOG.throwing(this.getClass().getName(), "processPOIFSReaderEvent", ex);
      }
      
    } else {
      LOG.warning("Wrong stream not processed: " + event.getName());
    }

    this.propertiesBroker.setProperties(this.properties);
  }

  protected void addProperty(final String name, final long value) {
    if (value != 0) {
      this.properties.setProperty(name, String.valueOf(value));
    }
  }

  protected void addProperty(final String name, final String value) {
    if (value != null) {
      this.properties.setProperty(name, value);
    }
  }

  protected void addProperty(final String name, final Date value) {
    if (value != null) {
      this.properties.setProperty(name, this.dateFormatter.format(value));
    }
  }
}
