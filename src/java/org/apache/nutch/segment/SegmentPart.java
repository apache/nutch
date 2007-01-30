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

package org.apache.nutch.segment;

import java.io.IOException;

import org.apache.hadoop.mapred.FileSplit;

/**
 * Utility class for handling information about segment parts.
 * 
 * @author Andrzej Bialecki
 */
public class SegmentPart {
  /** Name of the segment (just the last path component). */
  public String segmentName;
  /** Name of the segment part (ie. one of subdirectories inside a segment). */
  public String partName;
  
  public SegmentPart() {
    
  }
  
  public SegmentPart(String segmentName, String partName) {
    this.segmentName = segmentName;
    this.partName = partName;
  }
  
  /**
   * Return a String representation of this class, in the form
   * "segmentName/partName".
   */
  public String toString() {
    return segmentName + "/" + partName;
  }
  
  /**
   * Create SegmentPart from a FileSplit.
   * @param split
   * @return A {@link SegmentPart} resultant from a 
   * {@link FileSplit}.
   * @throws Exception
   */
  public static SegmentPart get(FileSplit split) throws IOException {
    return get(split.getPath().toString());
  }
  
  /**
   * Create SegmentPart from a full path of a location inside any segment part.
   * @param path full path into a segment part (may include "part-xxxxx" components)
   * @return SegmentPart instance describing this part.
   * @throws IOException if any required path components are missing.
   */
  public static SegmentPart get(String path) throws IOException {
    // find part name
    String dir = path.replace('\\', '/');
    int idx = dir.lastIndexOf("/part-");
    if (idx == -1) {
      throw new IOException("Cannot determine segment part: " + dir);
    }
    dir = dir.substring(0, idx);
    idx = dir.lastIndexOf('/');
    if (idx == -1) {
      throw new IOException("Cannot determine segment part: " + dir);
    }
    String part = dir.substring(idx + 1);
    // find segment name
    dir = dir.substring(0, idx);
    idx = dir.lastIndexOf('/');
    if (idx == -1) {
      throw new IOException("Cannot determine segment name: " + dir);
    }
    String segment = dir.substring(idx + 1);
    return new SegmentPart(segment, part);
  }
  
  /**
   * Create SegmentPart from a String in format "segmentName/partName".
   * @param string input String
   * @return parsed instance of SegmentPart
   * @throws IOException if "/" is missing.
   */
  public static SegmentPart parse(String string) throws IOException {
    int idx = string.indexOf('/');
    if (idx == -1) {
      throw new IOException("Invalid SegmentPart: '" + string + "'");
    }
    String segment = string.substring(0, idx);
    String part = string.substring(idx + 1);
    return new SegmentPart(segment, part);
  }
}