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

import org.apache.nutch.io.*;

import java.io.*;
import java.net.*;
import java.util.*;

/**************************************************
 * A JobProfile is a MapReduce primitive.  Tracks a job,
 * whether living or dead.
 *
 * @author Mike Cafarella
 **************************************************/
public class JobProfile implements Writable {
    String jobid;
    String jobFile;
    String url;

    /**
     */
    public JobProfile() {
    }

    /**
     */
    public JobProfile(String jobid, String jobFile, String url) {
        this.jobid = jobid;
        this.jobFile = jobFile;
        this.url = url;
    }

    /**
     */
    public String getJobId() {
        return jobid;
    }

    /**
     */
    public String getJobFile() {
        return jobFile;
    }


    /**
     */
    public URL getURL() {
        try {
            return new URL(url.toString());
        } catch (IOException ie) {
            return null;
        }
    }

    ///////////////////////////////////////
    // Writable
    ///////////////////////////////////////
    public void write(DataOutput out) throws IOException {
        UTF8.writeString(out, jobid);
        UTF8.writeString(out, jobFile);
        UTF8.writeString(out, url);
    }
    public void readFields(DataInput in) throws IOException {
        this.jobid = UTF8.readString(in);
        this.jobFile = UTF8.readString(in);
        this.url = UTF8.readString(in);
    }
}


