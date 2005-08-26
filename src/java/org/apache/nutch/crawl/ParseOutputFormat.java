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

package org.apache.nutch.crawl;

import org.apache.nutch.io.*;
import org.apache.nutch.fs.*;
import org.apache.nutch.mapred.*;
import org.apache.nutch.parse.*;
import org.apache.nutch.net.*;

import java.io.*;
import java.util.*;

/* Parse content in a segment. */
public class ParseOutputFormat implements OutputFormat {

  private UrlNormalizer urlNormalizer = UrlNormalizerFactory.getNormalizer();

  public RecordWriter getRecordWriter(NutchFileSystem fs, JobConf job,
                                      String name) throws IOException {

    final float interval = job.getFloat("db.default.fetch.interval", 30f);

    File text =
      new File(new File(job.getOutputDir(), ParseText.DIR_NAME), name);
    File data =
      new File(new File(job.getOutputDir(), ParseData.DIR_NAME), name);
    File crawl =
      new File(new File(job.getOutputDir(), CrawlDatum.PARSE_DIR_NAME), name);
    
    final MapFile.Writer textOut =
      new MapFile.Writer(fs, text.toString(), UTF8.class, ParseText.class);
    
    final MapFile.Writer dataOut =
      new MapFile.Writer(fs, data.toString(), UTF8.class,ParseData.class,true);
    
    final SequenceFile.Writer crawlOut =
      new SequenceFile.Writer(fs, crawl.toString(),
                              UTF8.class, CrawlDatum.class);
    
    return new RecordWriter() {

        public void write(WritableComparable key, Writable value)
          throws IOException {
          
          Parse parse = (Parse)value;
          
          textOut.append(key, new ParseText(parse.getText()));
          dataOut.append(key, parse.getData());

          // collect outlinks for subsequent db update
          Outlink[] links = parse.getData().getOutlinks();
          for (int i = 0; i < links.length; i++) {
            String toUrl = links[i].getToUrl();
            try {
              toUrl = urlNormalizer.normalize(toUrl); // normalize the url
              toUrl = URLFilters.filter(toUrl);   // filter the url
            } catch (Exception e) {
              toUrl = null;
            }
            if (toUrl != null)
              crawlOut.append(new UTF8(toUrl),
                              new CrawlDatum(CrawlDatum.STATUS_LINKED,
                                             interval));
          }
        }
        
        public void close(Reporter reporter) throws IOException {
          textOut.close();
          dataOut.close();
          crawlOut.close();
        }
        
      };
    
  }

}
