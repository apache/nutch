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

package org.apache.nutch.mapReduce;

import java.io.*;
import org.apache.nutch.io.*;

/** 
 * Protocol that a JobClient and the central JobTracker use to communicate.  The
 * JobClient can use these methods to submit a Job for execution, and learn about
 * the current system status.
 */ 
public interface JobSubmissionProtocol {
    /**
     * Submit a Job for execution.  Returns the latest profile for
     * that job.
     */
    public JobStatus submitJob(String jobFile) throws IOException;

    /**
     * Kill the indicated job
     */
    public void killJob(String jobid);

    /**
     * Grab a handle to a job that is already known to the JobTracker
     */
    public JobProfile getJobProfile(String jobid);

    /**
     * Grab a handle to a job that is already known to the JobTracker
     */
    public JobStatus getJobStatus(String jobid);

    /**
     * A MapReduce system always operates on a single filesystem.  This 
     * function returns the fs name.  ('local' if the localfs; 'addr:port' 
     * if ndfs).  The client can then copy files into the right locations 
     * prior to submitting the job.
     */
    public String getFilesystemName() throws IOException;
}
