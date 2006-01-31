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

import org.apache.nutch.io.*;
import org.apache.nutch.ipc.*;
import org.apache.nutch.util.*;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.logging.*;

/** Runs a reduce task. */
class ReduceTaskRunner extends TaskRunner {
  private static final Logger LOG =
    LogFormatter.getLogger("org.apache.nutch.mapred.ReduceTaskRunner");
  private MapOutputFile mapOutputFile;

  public ReduceTaskRunner(Task task, TaskTracker tracker, NutchConf nutchConf) {
    super(task, tracker, nutchConf);
    this.mapOutputFile = new MapOutputFile();
    this.mapOutputFile.setConf(nutchConf);
  }

  /** Assemble all of the map output files. */
  public void prepare() throws IOException {
    ReduceTask task = ((ReduceTask)getTask());
    this.mapOutputFile.removeAll(task.getTaskId());    // cleanup from failures
    String[] mapTaskIds = task.getMapTaskIds();
    final Progress copyPhase = getTask().getProgress().phase();

    // we need input from every map task
    HashSet needed = new HashSet();
    for (int i = 0; i < mapTaskIds.length; i++) {
      needed.add(mapTaskIds[i]);
      copyPhase.addPhase();                       // add sub-phase per file
    }

    InterTrackerProtocol jobClient = getTracker().getJobClient();
    while (needed.size() > 0) {

      getTask().reportProgress(getTracker());

      // get list of available map output locations from job tracker
      String[] neededStrings =
        (String[])needed.toArray(new String[needed.size()]);
      MapOutputLocation[] locs =
        jobClient.locateMapOutputs(task.getTaskId(), neededStrings);

      if (locs.length == 0) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
        }
        continue;
      }

      LOG.info(task.getTaskId()+" Got "+locs.length+" map output locations.");

      // try each of these locations
      for (int i = 0; i < locs.length; i++) {
        MapOutputLocation loc = locs[i];
        InetSocketAddress addr =
          new InetSocketAddress(loc.getHost(), loc.getPort());
        MapOutputProtocol client =
          (MapOutputProtocol)RPC.getProxy(MapOutputProtocol.class, addr, this.nutchConf);

        this.mapOutputFile.setProgressReporter(new MapOutputFile.ProgressReporter() {
            public void progress(float progress) {
              copyPhase.phase().set(progress);
              try {
                getTask().reportProgress(getTracker());
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            }
          });

        getTask().reportProgress(getTracker());
        try {
          copyPhase.phase().setStatus(loc.toString());
          
          client.getFile(loc.getMapTaskId(), task.getTaskId(),
                         new IntWritable(task.getPartition()));
          
          needed.remove(loc.getMapTaskId());     // success: remove from needed
          
          copyPhase.startNextPhase();
          
        } catch (IOException e) {                 // failed: try again later
          LOG.log(Level.WARNING,
                  task.getTaskId()+" copy failed: "
                  +loc.getMapTaskId()+" from "+addr,
                  e);
        } finally {
          this.mapOutputFile.setProgressReporter(null);
        }
        
      }
      
    }
    getTask().reportProgress(getTracker());
  }

  /** Delete all of the temporary map output files. */
  public void close() throws IOException {
    getTask().getProgress().setStatus("closed");
    this.mapOutputFile.removeAll(getTask().getTaskId());
  }

}
