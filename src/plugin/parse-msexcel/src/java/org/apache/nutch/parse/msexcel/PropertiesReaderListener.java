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
package org.apache.nutch.parse.msexcel;

// JDK imports
import java.util.Date;
import java.util.Properties;

// Jakarta POI imports
import org.apache.poi.hpsf.PropertySetFactory;
import org.apache.poi.hpsf.SummaryInformation;
import org.apache.poi.poifs.eventfilesystem.POIFSReaderEvent;
import org.apache.poi.poifs.eventfilesystem.POIFSReaderListener;

// Nutch imports
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.parse.msexcel.ExcelExtractor.PropertiesBroker;


/**
 * @author Rohit Kulkarni & Ashish Vaidya
 * @author J&eacute;r&ocirc;me Charron
 */
public class PropertiesReaderListener implements POIFSReaderListener {
    
    private PropertiesBroker propertiesBroker;
    private Properties metaData = new Properties();

    public PropertiesReaderListener(PropertiesBroker propertiesBroker) {
      this.propertiesBroker = propertiesBroker;
    }

    public void processPOIFSReaderEvent(POIFSReaderEvent event) {

      SummaryInformation si = null;
      Properties properties = new Properties();

      try {
        si = (SummaryInformation)PropertySetFactory.create(event.getStream());
      } catch (Exception ex) {
        properties = null;
      }

      Date tmp = null;

      String title = si.getTitle();
      String applicationName = si.getApplicationName();
      String author = si.getAuthor();
      int charCount = si.getCharCount();
      String comments = si.getComments();
      Date createDateTime = si.getCreateDateTime();
      long editTime = si.getEditTime();
      String keywords = si.getKeywords();
      String lastAuthor = si.getLastAuthor();
      Date lastPrinted = si.getLastPrinted();
      Date lastSaveDateTime = si.getLastSaveDateTime();
      int pageCount = si.getPageCount();
      String revNumber = si.getRevNumber();
      int security = si.getSecurity();
      String subject = si.getSubject();
      String template = si.getTemplate();
      int wordCount = si.getWordCount();

      /*Dates are being stored in millis since the epoch to aid
      localization*/
      if(title != null)
        properties.setProperty(Metadata.TITLE, title);
      if(applicationName != null)
        properties.setProperty(Metadata.APPLICATION_NAME, applicationName);
      if(author != null)
        properties.setProperty(Metadata.AUTHOR, author);
      if(charCount != 0)
        properties.setProperty(Metadata.CHARACTER_COUNT, charCount + "");
      if(comments != null)
        properties.setProperty(Metadata.COMMENTS, comments);
      if(createDateTime != null)
        properties.setProperty(Metadata.DATE,
                               Metadata.DATE_FORMAT.format(createDateTime));
      if(editTime != 0)
        properties.setProperty(Metadata.LAST_MODIFIED, editTime + "");
      if(keywords != null)
        properties.setProperty(Metadata.KEYWORDS, keywords);
      if(lastAuthor != null)
        properties.setProperty(Metadata.LAST_AUTHOR, lastAuthor);
      if(lastPrinted != null)
        properties.setProperty(Metadata.LAST_PRINTED, lastPrinted.getTime() + "");
      if(lastSaveDateTime != null)
        properties.setProperty(Metadata.LAST_SAVED, lastSaveDateTime.getTime() + "");
      if(pageCount != 0)
        properties.setProperty(Metadata.PAGE_COUNT, pageCount + "");
      if(revNumber != null)
        properties.setProperty(Metadata.REVISION_NUMBER, revNumber);
      if(security != 0)
        properties.setProperty(Metadata.RIGHTS, security + "");
      if(subject != null)
        properties.setProperty(Metadata.SUBJECT, subject);
      if(template != null)
        properties.setProperty(Metadata.TEMPLATE, template);
      if(wordCount != 0)
        properties.setProperty(Metadata.WORD_COUNT, wordCount + "");
      propertiesBroker.setProperties(properties);
    }
    
}
