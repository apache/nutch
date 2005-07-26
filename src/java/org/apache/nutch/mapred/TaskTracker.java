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

import org.apache.nutch.fs.*;
import org.apache.nutch.io.*;
import org.apache.nutch.ipc.*;
import org.apache.nutch.util.*;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.logging.*;

/*******************************************************
 * TaskTracker is a process that starts and tracks MR Tasks
 * in a networked environment.  It contacts the JobTracker
 * for Task assignments and reporting results.
 *
 * @author Mike Cafarella
 *******************************************************/
public class TaskTracker implements MRConstants, TaskUmbilicalProtocol, MapOutputProtocol, Runnable {
    private static final int MAX_CURRENT_TASKS = 
    NutchConf.get().getInt("mapred.tasktracker.tasks.maximum", 2);
    static final long WAIT_FOR_DONE = 3 * 1000;
    static final int STALE_STATE = 1;

    public static final Logger LOG =
    LogFormatter.getLogger("org.apache.nutch.mapred.TaskTracker");

    String taskTrackerName;
    String localHostname;
    InetSocketAddress jobTrackAddr;

    int taskReportPort;
    int mapOutputPort;

    Server taskReportServer = null;
    Server mapOutputServer = null;
    InterTrackerProtocol jobClient;

    TreeMap tasks = null;
    TreeMap runningTasks = null;
    boolean justStarted = true;

    static Random r = new Random();
    NutchFileSystem fs = null;
    File localDir = null;

    /**
     * Start with the local machine name, and the default JobTracker
     */
    public TaskTracker() throws IOException {
      this(JobTracker.getAddress(NutchConf.get()));
    }

    /**
     * Start with the local machine name, and the addr of the target JobTracker
     */
    public TaskTracker(InetSocketAddress jobTrackAddr) throws IOException {
        this.jobTrackAddr = jobTrackAddr;
        this.localDir = new File(JobConf.getLocalDir(), "tracker");
        initialize();
    }

    /**
     * Do the real constructor work here.  It's in a separate method
     * so we can call it again and "recycle" the object after calling
     * close().
     */
    void initialize() throws IOException {
        this.taskTrackerName = "tracker_" + (Math.abs(r.nextInt()) % 100000);
        this.localHostname = InetAddress.getLocalHost().getHostName();

        if (localDir.exists()) {
            FileUtil.fullyDelete(localDir);
        }
        localDir.mkdirs();

        // Clear out state tables
        this.tasks = new TreeMap();
        this.runningTasks = new TreeMap();

        // generate port numbers
        this.taskReportPort = 32768+r.nextInt(32768);
        this.mapOutputPort = 32768+r.nextInt(32768);

        // RPC initialization
        this.taskReportServer =
          RPC.getServer(this, taskReportPort, MAX_CURRENT_TASKS, false);
        this.taskReportServer.start();
        this.mapOutputServer =
          RPC.getServer(this, mapOutputPort, MAX_CURRENT_TASKS, false);
        this.mapOutputServer.start();

        // Clear out temporary files that might be lying around
        MapOutputFile.cleanupStorage();
        this.justStarted = true;

        this.jobClient = (InterTrackerProtocol) RPC.getProxy(InterTrackerProtocol.class, jobTrackAddr);
    }

    /**
     * Close down the TaskTracker and all its components.  We must also shutdown
     * any running tasks or threads, and cleanup disk space.  A new TaskTracker
     * within the same process space might be restarted, so everything must be
     * clean.
     */
    public void close() throws IOException {
        // Kill running tasks
        Vector v = new Vector();
        for (Iterator it = tasks.values().iterator(); it.hasNext(); ) {
            TaskInProgress tip = (TaskInProgress) it.next();
            v.add(tip);
        }
        for (Iterator it = v.iterator(); it.hasNext(); ) { 
            TaskInProgress tip = (TaskInProgress) it.next();           
            tip.cleanup();
        }

        // Wait for them to die and report in
        try {
            Thread.sleep(5000);
        } catch (InterruptedException ie) {
        }

        // Shutdown local RPC servers
        if (taskReportServer != null) {
            taskReportServer.stop();
            taskReportServer = null;
        }
        if (mapOutputServer != null) {
            mapOutputServer.stop();
            mapOutputServer = null;
        }

        // Clear local storage
        MapOutputFile.cleanupStorage();
    }

    /**
     * The connection to the JobTracker, used by the TaskRunner 
     * for locating remote files.
     */
    public InterTrackerProtocol getJobClient() {
      return jobClient;
    }

    /**
     * Main service loop.  Will stay in this loop forever.
     */
    int offerService() throws Exception {
        long lastHeartbeat = 0;

        while (true) {
            long now = System.currentTimeMillis();

            long waitTime = HEARTBEAT_INTERVAL - (now - lastHeartbeat);
            if (waitTime > 0) {
                try {
                    Thread.sleep(waitTime);
                } catch (InterruptedException ie) {
                }
                continue;
            }

            //
            // Emit standard hearbeat message to check in with JobTracker
            //
            Vector taskReports = new Vector();
            synchronized (runningTasks) {
                for (Iterator it = runningTasks.keySet().iterator(); it.hasNext(); ) {
                    String taskid = (String) it.next();
                    TaskInProgress tip = (TaskInProgress) runningTasks.get(taskid);
                    TaskStatus status = tip.createStatus();
                    taskReports.add(status);
                    if (status.getRunState() != TaskStatus.RUNNING) {
                        it.remove();
                    }
                }
            }

            //
            // Xmit the heartbeat
            //
            if (justStarted) {
                this.fs = NutchFileSystem.getNamed(jobClient.getFilesystemName());
            }

            IntWritable resultCode = jobClient.emitHeartbeat(new TaskTrackerStatus(taskTrackerName, localHostname, mapOutputPort, taskReports), new BooleanWritable(justStarted));
            justStarted = false;

            if (resultCode.get() == InterTrackerProtocol.UNKNOWN_TASKTRACKER) {
                return STALE_STATE;
            }

            //
            // Check if we should create a new Task
            //
            if (runningTasks.size() < MAX_CURRENT_TASKS) {
                Task t = jobClient.pollForNewTask(taskTrackerName);
                if (t != null) {
                    TaskInProgress tip = new TaskInProgress(t);
                    tasks.put(t.getTaskId(), tip);
                    runningTasks.put(t.getTaskId(), tip);
                    tip.launchTask();
                }
            }

            //
            // Check for any Tasks whose job may have ended
            //
            String toCloseId = jobClient.pollForClosedTask(taskTrackerName);
            if (toCloseId != null) {
                TaskInProgress tip = (TaskInProgress) tasks.get(toCloseId);
                tip.cleanup();
            }
            lastHeartbeat = now;
        }
    }

    /**
     * The server retry loop.  
     * This while-loop attempts to connect to the JobTracker.  It only 
     * loops when the old TaskTracker has gone bad (its state is
     * stale somehow) and we need to reinitialize everything.
     */
    public void run() {
        try {
            while (true) {
                boolean staleState = false;
                try {
                    // This while-loop attempts reconnects if we get network errors
                    while (! staleState) {
                        try {
                            if (offerService() == STALE_STATE) {
                                staleState = true;
                            }
                        } catch (Exception ex) {
                            ex.printStackTrace();
                            LOG.info("Lost connection to JobTracker [" + jobTrackAddr + "].  Retrying...");
                            try {
                                Thread.sleep(5000);
                            } catch (InterruptedException ie) {
                            }
                        }
                    }
                } finally {
                    close();
                }
                LOG.info("Reinitializing local state");
                initialize();
            }
        } catch (IOException iex) {
            LOG.info("Got fatal exception while reinitializing TaskTracker: " + iex.toString());
            return;
        }
    }

    ///////////////////////////////////////////////////////
    // TaskInProgress maintains all the info for a Task that
    // lives at this TaskTracker.  It maintains the Task object,
    // its TaskStatus, and the TaskRunner.
    ///////////////////////////////////////////////////////
    class TaskInProgress {
        Task task;
        File localTaskDir;
        float progress;
        int runstate;
        StringBuffer diagnosticInfo = new StringBuffer();
        TaskRunner runner;
        boolean done = false;
        boolean closeRunnerUponEnd = false;

        /**
         */
        public TaskInProgress(Task task) throws IOException {
            this.task = task;
            this.localTaskDir = new File(localDir, task.getTaskId());
            if (localTaskDir.exists()) {
                FileUtil.fullyDelete(localTaskDir);
            }
            this.localTaskDir.mkdirs();
            localizeTask(task);
        }

        /**
         * Some fields in the Task object need to be made machine-specific.
         * So here, edit the Task's fields appropriately.
         */
        void localizeTask(Task t) throws IOException {
            File localJobFile = new File(localTaskDir, "job.xml");
            File localJarFile = new File(localTaskDir, "job.jar");

            String jobFile = t.getJobFile();
            fs.copyToLocalFile(new File(jobFile), localJobFile);
            t.setJobFile(localJobFile.getCanonicalPath());

            JobConf jc = new JobConf(localJobFile);
            String jarFile = jc.getJar();
            if (jarFile != null) {
              fs.copyToLocalFile(new File(jarFile), localJarFile);
              jc.setJar(localJarFile.getCanonicalPath());

              BufferedOutputStream out =
                new BufferedOutputStream(new FileOutputStream(localJobFile));
              try {
                jc.write(out);
              } finally {
                out.close();
              }
            }
        }

        /**
         */
        public Task getTask() {
            return task;
        }

        /**
         */
        public TaskStatus createStatus() {
            TaskStatus status = new TaskStatus(task.getTaskId(), progress, runstate, diagnosticInfo.toString());
            if (diagnosticInfo.length() > 0) {
                diagnosticInfo = new StringBuffer();
            }
            return status;
        }

        /**
         * Kick off the task execution
         */
        public synchronized void launchTask() throws IOException {
            this.progress = 0.0f;
            this.runstate = TaskStatus.RUNNING;
            this.diagnosticInfo = new StringBuffer();
            this.runner = task.createRunner(TaskTracker.this);
            this.runner.start();
        }

        /**
         * The task is reporting its progress
         */
        public synchronized void reportProgress(float p) {
            LOG.info("Progress for task " + task.getTaskId() + " is " + p);
            this.progress = p;
            this.runstate = TaskStatus.RUNNING;
        }

        /**
         * The task has reported some diagnostic info about its status
         */
        public synchronized void reportDiagnosticInfo(String info) {
            this.diagnosticInfo.append(info);
        }

        /**
         * The task is reporting that it's done running
         */
        public synchronized void reportDone() {
            LOG.info("Task " + task.getTaskId() + " is done.");
            this.done = true;
        }

        /**
         * The task has actually finished running.
         */
        public synchronized void taskFinished() {
            long start = System.currentTimeMillis();

            //
            // Wait until task reports as done.  If it hasn't reported in,
            // wait for a second and try again.
            //
            while (! done && (System.currentTimeMillis() - start < WAIT_FOR_DONE)) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                }
            }

            //
            // Change state to success or failure, depending on whether
            // task was 'done' before terminating
            //
            if (done) {
                runstate = TaskStatus.SUCCEEDED;
            } else {
                runstate = TaskStatus.FAILED;
            }

            //
            // We've already tried to 'cleanup' this task.  So once
            // the process actually finishes, finish the cleanup work.
            //
            if (closeRunnerUponEnd) {
                runningTasks.remove(task.getTaskId());
                tasks.remove(task.getTaskId());
                try {
                    runner.close();
                } catch (IOException ie) {
                }
                try {
                    FileUtil.fullyDelete(localTaskDir);
                } catch (IOException ie) {
                }                
            }
        }

        /**
         * The owning job is done, and this task is no longer needed.  
         * This method cleans up the task, first killing it if necessary.
         */
        public synchronized void cleanup() throws IOException {
            if (runstate == TaskStatus.RUNNING) {
                closeRunnerUponEnd = true;
                runner.kill();
            } else {
                runningTasks.remove(task.getTaskId());
                tasks.remove(task.getTaskId());
                try {
                    runner.close();
                } catch (IOException ie) {
                }
                FileUtil.fullyDelete(localTaskDir);
            }
        }
    }

    /////////////////////////////////////////////////////////////////
    // MapOutputProtocol
    /////////////////////////////////////////////////////////////////
    public MapOutputFile getFile(String mapTaskId, String reduceTaskId,
                                 IntWritable partition) {
        return new MapOutputFile(mapTaskId, reduceTaskId, partition.get());
    }

    /////////////////////////////////////////////////////////////////
    // TaskUmbilicalProtocol
    /////////////////////////////////////////////////////////////////
    /**
     * Called upon startup by the child process, to fetch Task data.
     */
    public Task getTask(String taskid) throws IOException {
        TaskInProgress tip = (TaskInProgress) tasks.get(taskid);
        if (tip != null) {
            return (Task) tip.getTask();
        } else {
            return null;
        }
    }

    /**
     * Called periodically to report Task progress, from 0.0 to 1.0.
     */
    public void progress(String taskid, FloatWritable progress) throws IOException {
        TaskInProgress tip = (TaskInProgress) tasks.get(taskid);
        tip.reportProgress(progress.get());
    }

    /**
     * Called when the task dies before completion, and we want to report back
     * diagnostic info
     */
    public void reportDiagnosticInfo(String taskid, String info) throws IOException {
        TaskInProgress tip = (TaskInProgress) tasks.get(taskid);
        tip.reportDiagnosticInfo(info);
    }

    /**
     * The task is done.
     */
    public void done(String taskid) throws IOException {
        TaskInProgress tip = (TaskInProgress) tasks.get(taskid);
        tip.reportDone();
    }

    /** Child checking to see if we're alive.  Normally does nothing.*/
    public void ping(String taskid) throws IOException {
      if (tasks.get(taskid) == null) {
        throw new IOException("No such task id."); // force child exit
      }
    }

    /////////////////////////////////////////////////////
    //  Called by TaskTracker thread after task process ends
    /////////////////////////////////////////////////////
    /**
     * The task is no longer running.  It may not have completed successfully
     */
    void reportTaskFinished(String taskid) {
        TaskInProgress tip = (TaskInProgress) tasks.get(taskid);
        tip.taskFinished();
    }

    /** 
     * The main() for child processes. 
     */
    public static class Child {
        public static void main(String[] args) throws Throwable {
          LogFormatter.showTime(false);
          LOG.info("Child starting");

          int port = Integer.parseInt(args[0]);
          String taskid = args[1];
          TaskUmbilicalProtocol umbilical =
            (TaskUmbilicalProtocol)RPC.getProxy(TaskUmbilicalProtocol.class,
                                                new InetSocketAddress(port));
            
          Task task = umbilical.getTask(taskid);
          JobConf job = new JobConf(task.getJobFile());

          startPinging(umbilical, taskid);        // start pinging parent

          try {
              task.run(job, umbilical);           // run the task
              umbilical.done(taskid);
          } catch (Throwable throwable) {
              LOG.log(Level.WARNING, "Failed to spawn child", throwable);
              // Report back any failures, for diagnostic purposes
              ByteArrayOutputStream baos = new ByteArrayOutputStream();
              throwable.printStackTrace(new PrintStream(baos));
              umbilical.reportDiagnosticInfo(taskid, baos.toString());
          }
        }

        /** Periodically ping parent and exit when this fails.*/
        private static void startPinging(final TaskUmbilicalProtocol umbilical,
                                         final String taskid) {
          Thread thread = new Thread(new Runnable() {
              public void run() {
                while (true) {
                  try {
                    umbilical.ping(taskid);
                  } catch (Throwable t) {
                    LOG.warning("Parent died.  Exiting "+taskid);
                    System.exit(1);
                  }
                  try {
                    Thread.sleep(1000);
                  } catch (InterruptedException e) {
                  }
                }
              }
            }, "Pinger for "+taskid);
          thread.setDaemon(true);
          thread.start();
        }
    }

    /**
     * Start the TaskTracker, point toward the indicated JobTracker
     */
    public static void main(String argv[]) throws IOException {
        if (argv.length != 0) {
            System.out.println("usage: TaskTracker");
            System.exit(-1);
        }

        TaskTracker tt = new TaskTracker();
        tt.run();
    }
}
