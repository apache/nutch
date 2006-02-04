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

import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.Properties;
import java.util.logging.Logger;

import org.apache.hadoop.util.LogFormatter;
import org.apache.poi.hpsf.SummaryInformation;
import org.apache.poi.poifs.eventfilesystem.POIFSReader;

/**
 * Converts the Powerpoint document content to plain text.
 * 
 * @author Stephan Strittmatter - http://www.sybit.de
 * 
 * @version 1.0
 */

public class PPTExtractor {

  private static final Logger LOG = LogFormatter.getLogger(PPTExtractor.class
      .getName());

  /** Parsed plain Powerpoint Text */
  private final transient StringBuffer contentBuf;

  private final PropertiesBroker propertiesBroker;

  private final POIFSReader poireader;

  /**
   * Constructor that takes a PowerPoint file as <code>InputStream</code> to
   * parse it.
   * 
   * @param in
   *          <code>InputStream</code> containing the PowerPoint file
   * @throws PowerPointDocumentException
   *           thrown if parsing failed
   */
  public PPTExtractor(final InputStream in) throws PowerPointDocumentException {
    this.poireader = new POIFSReader();
    this.propertiesBroker = new PropertiesBroker();
    this.contentBuf = new StringBuffer();

    this.init(in);
  }

  /**
   * Get the PowerPoint content text as plain text
   * 
   * @return String the content text
   */
  public String getText() {
    return this.contentBuf.toString();
  }

  /**
   * Get the <code>Properties</code> of the PowerPoint document.
   * 
   * @return the properties of the document
   */
  public Properties getProperties() {
    return this.propertiesBroker.getProperties();
  }

  /**
   * @param input
   * @throws PowerPointDocumentException
   */
  private void init(final InputStream input) throws PowerPointDocumentException {
    // register listener for SummaryInformation
    this.poireader.registerListener(new PropertiesReaderListener(
        this.propertiesBroker), SummaryInformation.DEFAULT_STREAM_NAME);

    // register listener for PPT-document content
    this.poireader.registerListener(new ContentReaderListener(this.contentBuf),
        PPTConstants.POWERPOINT_DOCUMENT);

    try {
      input.reset();
      if (input.available() > 0) {
        this.poireader.read(input);
      } else {
        LOG.warning("Input <=0 :" + input.available());
      }
    } catch (IOException e) {
      throw new PowerPointDocumentException(e);
    }
  }

  /**
   * The PropertiesBroker
   * 
   * @author Stephan Strittmatter
   * @version 1.0
   */
  static class PropertiesBroker {

    private final static int TIMEOUT = 2 * 1000;

    private Properties properties = null;

    /**
     * Get the collected properties.
     * 
     * @return properties of the PowerPoint file
     */
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

    /**
     * 
     * @param properties
     */
    public synchronized void setProperties(Properties properties) {
      this.properties = properties;
      notifyAll();
    }
  }
}
