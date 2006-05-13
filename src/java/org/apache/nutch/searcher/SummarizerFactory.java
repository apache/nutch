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
package org.apache.nutch.searcher;

// JDK imports
import java.util.logging.Logger;

// Hadoop imports
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.LogFormatter;

// Nutch imports
import org.apache.nutch.plugin.Extension;
import org.apache.nutch.plugin.PluginRepository;


/**
 * A factory for retrieving {@link Summarizer} extensions.
 * 
 * @author J&eacute;r&ocirc;me Charron
 */
public class SummarizerFactory {

  /** My logger */
  public final static Logger LOG =
    LogFormatter.getLogger(SummarizerFactory.class.getName());

  /** The first available {@link Summarizer} */
  private Summarizer summarizer = null;
  
  
  public SummarizerFactory(Configuration conf) {
    try {
      Extension[] extensions = PluginRepository
                                    .get(conf)
                                    .getExtensionPoint(Summarizer.X_POINT_ID)
                                    .getExtensions();
      summarizer = (Summarizer) extensions[0].getExtensionInstance();
      LOG.info("Using the first summarizer extension found: " +
               extensions[0].getId());
    } catch (Exception e) {
      LOG.warning(e.toString());
    }
  }

  /**
   * Get the first available {@link Summarizer} extension.
   * @return the first available {@link Summarizer} extension, or
   *         <code>null</code> if none available.
   */
  public Summarizer getSummarizer() {
    return summarizer;
  }

} 
