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

package org.apache.nutch.util;

import java.util.*;

// Commons Logging imports
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/************************************************
 * ThreadPool.java                                                 
 *
 * ThreadPool maintains a large set of threads, which
 * can be dedicated to a certain task, and then recycled.
 ***********************************************/
public class ThreadPool {
    /**
     * A TaskThread sits in a loop, asking the pool
     * for a job, and servicing it.  That's all it does.
     */
    class TaskThread extends Thread {
        /**
         * Get a job from the pool, run it, repeat.
         * If the job is null, we exit the loop.
         */
        public void run() {
            while (true) {
                Runnable r = obtainJob();
                if (r == null) {
                    break;
                }
                try {
                    r.run();
                } catch (Exception e) {
                    System.err.println("E: " + e);
                    e.printStackTrace();
                }
            }
        }
    }

    int numThreads;
    boolean running = false;
    Vector<Runnable> jobs;

    /**
     * Creates a pool of numThreads size.
     * These threads sit around waiting for jobs to
     * be posted to the list.
     */
    public ThreadPool(int numThreads) {
        this.numThreads = numThreads;
        jobs = new Vector<Runnable>(37);
        running = true;

        for (int i = 0; i < numThreads; i++) {
            TaskThread t = new TaskThread();
            t.start();
        }
        Log l = LogFactory.getLog("org.apache.nutch.util");
        if (l.isDebugEnabled()) {
          l.debug("ThreadPool created with " + numThreads + " threads.");
        }
    }

    /**
     * Gets a job from the queue, returns to worker thread.
     * When the pool is closed down, return null for all
     * obtainJob() requests.  That tells the thread to
     * shut down.
     */
    Runnable obtainJob() {
        Runnable job = null;

        synchronized (jobs) {
            while (job == null && running) {
                try {
                    if (jobs.size() == 0) {
                        jobs.wait();
                    }
                } catch (InterruptedException ie) {
                }

                if (jobs.size() > 0) {
                    job = jobs.firstElement();
                    jobs.removeElementAt(0);
                }
            }
        }

        if (running) {
            // Got a job from the queue
            return job;
        } else {
            // Shut down the pool
            return null;
        }
    }

    /**
     * Post a Runnable to the queue.  This will be
     * picked up by an active thread.
     */
    public void addJob(Runnable runnable) {
        synchronized (jobs) {
            jobs.add(runnable);
            jobs.notifyAll();
        }
    }

    /**
     * Turn off the pool.  Every thread, when finished with
     * its current work, will realize that the pool is no
     * longer running, and will exit.
     */
    public void shutdown() {
        running = false;
        Log l = LogFactory.getLog("org.apache.nutch.util");
        if (l.isDebugEnabled()) {
          l.debug("ThreadPool shutting down.");
        }
    }
}
