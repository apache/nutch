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

import java.io.*;
import java.net.*;
import java.util.*;

/**************************************************
 * Describes the current status of a task.  This is
 * not intended to be a comprehensive piece of data.
 *
 * @author Mike Cafarella
 **************************************************/
public class TaskStatus implements Writable {
    public static final int RUNNING = 0;
    public static final int SUCCEEDED = 1;
    public static final int FAILED = 2;
    public static final int UNASSIGNED = 3;
    
    private String taskid;
    private float progress;
    private int runState;

    public TaskStatus() {}

    public TaskStatus(String taskid, float progress, int runState) {
        this.taskid = taskid;
        this.progress = progress;
        this.runState = runState;
    }
    
    public String getTaskId() { return taskid; }
    public float getProgress() { return progress; }
    public void setProgress(float progress) { this.progress = progress; } 
    public int getRunState() { return runState; }
    public void setRunState(int runState) { this.runState = runState; }

    //////////////////////////////////////////////
    // Writable
    //////////////////////////////////////////////
    public void write(DataOutput out) throws IOException {
        UTF8.writeString(out, taskid);
        out.writeFloat(progress);
        out.writeInt(runState);
    }

    public void readFields(DataInput in) throws IOException {
        this.taskid = UTF8.readString(in);
        this.progress = in.readFloat();
        this.runState = in.readInt();
    }
}
