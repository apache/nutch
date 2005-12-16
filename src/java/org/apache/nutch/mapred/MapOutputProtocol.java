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

package org.apache.nutch.mapred;

import java.io.IOException;

import org.apache.nutch.io.*;

/** Protocol that a reduce task uses to retrieve output data from a map task's
 * tracker. */ 
public interface MapOutputProtocol {

  /** Returns the output from the named map task destined for this partition.*/
  MapOutputFile getFile(String mapTaskId, String reduceTaskId,
                        IntWritable partition) throws IOException;

}
