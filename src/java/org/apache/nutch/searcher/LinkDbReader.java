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

import java.io.IOException;
import java.io.File;

import org.apache.nutch.io.*;
import org.apache.nutch.fs.*;
import org.apache.nutch.mapred.*;
import org.apache.nutch.mapred.lib.HashPartitioner;
import org.apache.nutch.crawl.*;

import java.io.IOException;

/** . */
public class LinkDbReader implements HitInlinks {
  private static final Partitioner PARTITIONER = new HashPartitioner();

  private NutchFileSystem fs;
  private File directory;
  private MapFile.Reader[] readers;

  public LinkDbReader(NutchFileSystem fs, File directory) {
    this.fs = fs;
    this.directory = directory;
  }

  public String[] getAnchors(HitDetails details) throws IOException {
    Inlinks inlinks = getInlinks(details);
    if (inlinks == null)
      return null;
    return inlinks.getAnchors();
  }

  public Inlinks getInlinks(HitDetails details) throws IOException {

    synchronized (this) {
      if (readers == null) {
        readers = MapFileOutputFormat.getReaders
          (fs, new File(directory, LinkDb.CURRENT_NAME));
      }
    }
    
    return (Inlinks)MapFileOutputFormat.getEntry
      (readers, PARTITIONER,
       new UTF8(details.getValue("url")),
       new Inlinks());
  }
}
