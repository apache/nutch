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
import org.apache.nutch.fs.*;
import org.apache.nutch.ipc.*;
import org.apache.nutch.util.*;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.logging.*;

/*******************************************************
 * JobTracker is the central location for submitting and 
 * tracking MR jobs in a network environment.
 *
 * @author Mike Cafarella
 *******************************************************/
public class JobTracker implements MRConstants, InterTrackerProtocol, JobSubmissionProtocol {
    static long RETIRE_JOB_INTERVAL;
    static long RETIRE_JOB_CHECK_INTERVAL;
    static float TASK_ALLOC_EPSILON;
    static float PAD_FRACTION;
    static float MIN_SLOTS_FOR_PADDING;

    public static final Logger LOG = LogFormatter.getLogger("org.apache.nutch.mapred.JobTracker");

    private static JobTracker tracker = null;
    public static void startTracker(NutchConf conf) throws IOException {
      if (tracker != null)
        throw new IOException("JobTracker already running.");
      while (true) {
        try {
          tracker = new JobTracker(conf);
          break;
        } catch (IOException e) {
          LOG.log(Level.WARNING, "Starting tracker", e);
        }
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
        }
      }
      tracker.offerService();
    }

    public static JobTracker getTracker() {
        return tracker;
    }

    ///////////////////////////////////////////////////////
    // Used to expire TaskTrackers that have gone down
    ///////////////////////////////////////////////////////
    class ExpireTrackers implements Runnable {
        boolean shouldRun = true;
        public ExpireTrackers() {
        }
        /**
         * The run method lives for the life of the JobTracker, and removes TaskTrackers
         * that have not checked in for some time.
         */
        public void run() {
            while (shouldRun) {
                //
                // Thread runs periodically to check whether trackers should be expired.
                // The sleep interval must be no more than half the maximum expiry time
                // for a task tracker.
                //
                try {
                    Thread.sleep(TASKTRACKER_EXPIRY_INTERVAL / 3);
                } catch (InterruptedException ie) {
                }

                //
                // Loop through all expired items in the queue
                //
                synchronized (taskTrackers) {
                    synchronized (trackerExpiryQueue) {
                        long now = System.currentTimeMillis();
                        TaskTrackerStatus leastRecent = null;
                        while ((trackerExpiryQueue.size() > 0) &&
                               ((leastRecent = (TaskTrackerStatus) trackerExpiryQueue.first()) != null) &&
                               (now - leastRecent.getLastSeen() > TASKTRACKER_EXPIRY_INTERVAL)) {

                            // Remove profile from head of queue
                            trackerExpiryQueue.remove(leastRecent);

                            // Figure out if last-seen time should be updated, or if tracker is dead
                            TaskTrackerStatus newProfile = (TaskTrackerStatus) taskTrackers.get(leastRecent.getTrackerName());
                            // Items might leave the taskTracker set through other means; the
                            // status stored in 'taskTrackers' might be null, which means the
                            // tracker has already been destroyed.
                            if (newProfile != null) {
                                if (now - newProfile.getLastSeen() > TASKTRACKER_EXPIRY_INTERVAL) {
                                    // Remove completely

                                    TaskTrackerStatus oldStatus = (TaskTrackerStatus) taskTrackers.remove(leastRecent.getTrackerName());
                                    if (oldStatus != null) {
                                        totalMaps -= oldStatus.countMapTasks();
                                        totalReduces -= oldStatus.countReduceTasks();
                                    }
                                    lostTaskTracker(leastRecent.getTrackerName());
                                } else {
                                    // Update time by inserting latest profile
                                    trackerExpiryQueue.add(newProfile);
                                }
                            }
                        }
                    }
                }
            }
        }
        
        /**
         * Stop the tracker on next iteration
         */
        public void stopTracker() {
            shouldRun = false;
        }
    }

    ///////////////////////////////////////////////////////
    // Used to remove old finished Jobs that have been around for too long
    ///////////////////////////////////////////////////////
    class RetireJobs implements Runnable {
        boolean shouldRun = true;
        public RetireJobs() {
        }

        /**
         * The run method lives for the life of the JobTracker,
         * and removes Jobs that are not still running, but which
         * finished a long time ago.
         */
        public void run() {
            while (shouldRun) {
                try {
                    Thread.sleep(RETIRE_JOB_CHECK_INTERVAL);
                } catch (InterruptedException ie) {
                }
                
                synchronized (jobs) {
                    for (Iterator it = jobs.keySet().iterator(); it.hasNext(); ) {
                        String jobid = (String) it.next();
                        JobInProgress job = (JobInProgress) jobs.get(jobid);

                        if (job.getStatus().getRunState() != JobStatus.RUNNING &&
                            (job.getFinishTime() + RETIRE_JOB_INTERVAL < System.currentTimeMillis())) {
                            it.remove();
                            jobsByArrival.remove(job);
                        }
                    }
                }
            }
        }
        public void stopRetirer() {
            shouldRun = false;
        }
    }

    /////////////////////////////////////////////////////////////////
    // The real JobTracker
    ////////////////////////////////////////////////////////////////
    int port;
    String localMachine;
    long startTime;
    int totalSubmissions = 0;
    Random r = new Random();

    private int maxCurrentTasks;

    //
    // Properties to maintain while running Jobs and Tasks:
    //
    // 1.  Each Task is always contained in a single Job.  A Job succeeds when all its 
    //     Tasks are complete.
    //
    // 2.  Every running or successful Task is assigned to a Tracker.  Idle Tasks are not.
    //
    // 3.  When a Tracker fails, all of its assigned Tasks are marked as failures.
    //
    // 4.  A Task might need to be reexecuted if it (or the machine it's hosted on) fails
    //     before the Job is 100% complete.  Sometimes an upstream Task can fail without
    //     reexecution if all downstream Tasks that require its output have already obtained
    //     the necessary files.
    //

    // All the known jobs.  (jobid->JobInProgress)
    TreeMap jobs = new TreeMap();
    Vector jobsByArrival = new Vector();

    // All the known TaskInProgress items, mapped to by taskids (taskid->TIP)
    TreeMap taskidToTIPMap = new TreeMap();

    // (taskid --> trackerID) 
    TreeMap taskidToTrackerMap = new TreeMap();

    // (trackerID->TreeSet of taskids running at that tracker)
    TreeMap trackerToTaskMap = new TreeMap();

    //
    // Watch and expire TaskTracker objects using these structures.
    // We can map from Name->TaskTrackerStatus, or we can expire by time.
    //
    int totalMaps = 0;
    int totalReduces = 0;
    TreeMap taskTrackers = new TreeMap();
    ExpireTrackers expireTrackers = new ExpireTrackers();
    RetireJobs retireJobs = new RetireJobs();

    /**
     * It might seem like a bug to maintain a TreeSet of status objects,
     * which can be updated at any time.  But that's not what happens!  We
     * only update status objects in the taskTrackers table.  Status objects
     * are never updated once they enter the expiry queue.  Instead, we wait
     * for them to expire and remove them from the expiry queue.  If a status
     * object has been updated in the taskTracker table, the latest status is 
     * reinserted.  Otherwise, we assume the tracker has expired.
     */
    TreeSet trackerExpiryQueue = new TreeSet(new Comparator() {
        public int compare(Object o1, Object o2) {
            TaskTrackerStatus p1 = (TaskTrackerStatus) o1;
            TaskTrackerStatus p2 = (TaskTrackerStatus) o2;
            if (p1.getLastSeen() < p2.getLastSeen()) {
                return -1;
            } else if (p1.getLastSeen() > p2.getLastSeen()) {
                return 1;
            } else {
                return (p1.getTrackerName().compareTo(p2.getTrackerName()));
            }
        }
    });

    // Used to provide an HTML view on Job, Task, and TaskTracker structures
    JobTrackerInfoServer infoServer;
    int infoPort;

    Server interTrackerServer;

    // Some jobs are stored in a local system directory.  We can delete
    // the files when we're done with the job.
    static final String SUBDIR = "jobTracker";
    NutchFileSystem fs;
    File systemDir;
    private NutchConf nutchConf;

    /**
     * Start the JobTracker process, listen on the indicated port
     */
    JobTracker(NutchConf conf) throws IOException {
        //
        // Grab some static constants
        //
        maxCurrentTasks = conf.getInt("mapred.tasktracker.tasks.maximum", 2);
        RETIRE_JOB_INTERVAL = conf.getLong("mapred.jobtracker.retirejob.interval", 24 * 60 * 60 * 1000);
        RETIRE_JOB_CHECK_INTERVAL = conf.getLong("mapred.jobtracker.retirejob.check", 60 * 1000);
        TASK_ALLOC_EPSILON = conf.getFloat("mapred.jobtracker.taskalloc.loadbalance.epsilon", 0.2f);
        PAD_FRACTION = conf.getFloat("mapred.jobtracker.taskalloc.capacitypad", 0.1f);
        MIN_SLOTS_FOR_PADDING = 3 * maxCurrentTasks;

        // This is a directory of temporary submission files.  We delete it
        // on startup, and can delete any files that we're done with
        this.nutchConf = conf;
        JobConf jobConf = new JobConf(conf);
        this.systemDir = jobConf.getSystemDir();
        this.fs = NutchFileSystem.get(conf);
        FileUtil.fullyDelete(fs, systemDir);
        fs.mkdirs(systemDir);

        // Same with 'localDir' except it's always on the local disk.
        jobConf.deleteLocalFiles(SUBDIR);

        // Set ports, start RPC servers, etc.
        InetSocketAddress addr = getAddress(conf);
        this.localMachine = addr.getHostName();
        this.port = addr.getPort();
        this.interTrackerServer = RPC.getServer(this, addr.getPort(), 10, false, conf);
        this.interTrackerServer.start();
	Properties p = System.getProperties();
	for (Iterator it = p.keySet().iterator(); it.hasNext(); ) {
	    String key = (String) it.next();
	    String val = (String) p.getProperty(key);
	    LOG.info("Property '" + key + "' is " + val);
	}

        this.infoPort = conf.getInt("mapred.job.tracker.info.port", 50030);
        this.infoServer = new JobTrackerInfoServer(this, infoPort);
        this.infoServer.start();

        this.startTime = System.currentTimeMillis();

        new Thread(this.expireTrackers).start();
        new Thread(this.retireJobs).start();
    }

    public static InetSocketAddress getAddress(NutchConf conf) {
      String jobTrackerStr =
        conf.get("mapred.job.tracker", "localhost:8012");
      int colon = jobTrackerStr.indexOf(":");
      if (colon < 0) {
        throw new RuntimeException("Bad mapred.job.tracker: "+jobTrackerStr);
      }
      String jobTrackerName = jobTrackerStr.substring(0, colon);
      int jobTrackerPort = Integer.parseInt(jobTrackerStr.substring(colon+1));
      return new InetSocketAddress(jobTrackerName, jobTrackerPort);
    }


    /**
     * Run forever
     */
    public void offerService() {
        try {
            this.interTrackerServer.join();
        } catch (InterruptedException ie) {
        }
    }

    ///////////////////////////////////////////////////////
    // Maintain lookup tables; called by JobInProgress
    // and TaskInProgress
    ///////////////////////////////////////////////////////
    void createTaskEntry(String taskid, String taskTracker, TaskInProgress tip) {
        LOG.info("Adding task '" + taskid + "' to tip " + tip.getTIPId() + ", for tracker '" + taskTracker + "'");

        // taskid --> tracker
        taskidToTrackerMap.put(taskid, taskTracker);

        // tracker --> taskid
        TreeSet taskset = (TreeSet) trackerToTaskMap.get(taskTracker);
        if (taskset == null) {
            taskset = new TreeSet();
            trackerToTaskMap.put(taskTracker, taskset);
        }
        taskset.add(taskid);

        // taskid --> TIP
        taskidToTIPMap.put(taskid, tip);
    }
    void removeTaskEntry(String taskid) {
        // taskid --> tracker
        String tracker = (String) taskidToTrackerMap.remove(taskid);

        // tracker --> taskid
        TreeSet trackerSet = (TreeSet) trackerToTaskMap.get(tracker);
        if (trackerSet != null) {
            trackerSet.remove(taskid);
        }

        // taskid --> TIP
        taskidToTIPMap.remove(taskid);
    }

    ///////////////////////////////////////////////////////
    // Accessors for objects that want info on jobs, tasks,
    // trackers, etc.
    ///////////////////////////////////////////////////////
    public int getTotalSubmissions() {
        return totalSubmissions;
    }
    public String getJobTrackerMachine() {
        return localMachine;
    }
    public int getTrackerPort() {
        return port;
    }
    public int getInfoPort() {
        return infoPort;
    }
    public long getStartTime() {
        return startTime;
    }
    public Vector runningJobs() {
        Vector v = new Vector();
        for (Iterator it = jobs.values().iterator(); it.hasNext(); ) {
            JobInProgress jip = (JobInProgress) it.next();
            JobStatus status = jip.getStatus();
            if (status.getRunState() == JobStatus.RUNNING) {
                v.add(jip);
            }
        }
        return v;
    }
    public Vector failedJobs() {
        Vector v = new Vector();
        for (Iterator it = jobs.values().iterator(); it.hasNext(); ) {
            JobInProgress jip = (JobInProgress) it.next();
            JobStatus status = jip.getStatus();
            if (status.getRunState() == JobStatus.FAILED) {
                v.add(jip);
            }
        }
        return v;
    }
    public Vector completedJobs() {
        Vector v = new Vector();
        for (Iterator it = jobs.values().iterator(); it.hasNext(); ) {
            JobInProgress jip = (JobInProgress) it.next();
            JobStatus status = jip.getStatus();
            if (status.getRunState() == JobStatus.SUCCEEDED) {
                v.add(jip);
            }
        }
        return v;
    }
    public Collection taskTrackers() {
        return taskTrackers.values();
    }
    public TaskTrackerStatus getTaskTracker(String trackerID) {
        return (TaskTrackerStatus) taskTrackers.get(trackerID);
    }

    ////////////////////////////////////////////////////
    // InterTrackerProtocol
    ////////////////////////////////////////////////////
    public void initialize(String taskTrackerName) {
        if (taskTrackers.get(taskTrackerName) != null) {
            TaskTrackerStatus oldStatus = (TaskTrackerStatus) taskTrackers.remove(taskTrackerName);
            totalMaps -= oldStatus.countMapTasks();
            totalReduces -= oldStatus.countReduceTasks();

            lostTaskTracker(taskTrackerName);
        }
    }

    /**
     * Process incoming heartbeat messages from the task trackers.
     */
    public synchronized int emitHeartbeat(TaskTrackerStatus trackerStatus, boolean initialContact) {
        String trackerName = trackerStatus.getTrackerName();
        trackerStatus.setLastSeen(System.currentTimeMillis());

        synchronized (taskTrackers) {
            synchronized (trackerExpiryQueue) {
                if (initialContact) {
                    // If it's first contact, then clear out any state hanging around
                    if (taskTrackers.get(trackerName) != null) {
                        TaskTrackerStatus oldStatus = (TaskTrackerStatus) taskTrackers.remove(trackerName);
                        totalMaps -= oldStatus.countMapTasks();
                        totalReduces -= oldStatus.countReduceTasks();
                        lostTaskTracker(trackerName);
                    }
                } else {
                    // If not first contact, there should be some record of the tracker
                    if (taskTrackers.get(trackerName) == null) {
                        return InterTrackerProtocol.UNKNOWN_TASKTRACKER;
                    }
                }

                // Store latest state.  If first contact, then save current
                // state in expiry queue
                totalMaps += trackerStatus.countMapTasks();
                totalReduces += trackerStatus.countReduceTasks();
                taskTrackers.put(trackerName, trackerStatus);
                if (initialContact) {
                    trackerExpiryQueue.add(trackerStatus);
                }
            }
        }

        updateTaskStatuses(trackerStatus);
        //LOG.info("Got heartbeat from "+trackerName);
        return InterTrackerProtocol.TRACKERS_OK;
    }

    /**
     * A tracker wants to know if there's a Task to run.  Returns
     * a task we'd like the TaskTracker to execute right now.
     *
     * Eventually this function should compute load on the various TaskTrackers,
     * and incorporate knowledge of NDFS file placement.  But for right now, it
     * just grabs a single item out of the pending task list and hands it back.
     */
    public synchronized Task pollForNewTask(String taskTracker) {
        //
        // Compute average map and reduce task numbers across pool
        //
        int avgMaps = 0;
        int avgReduces = 0;
        if (taskTrackers.size() > 0) {
            avgMaps = totalMaps / taskTrackers.size();
            avgReduces = totalReduces / taskTrackers.size();
        }
        int totalCapacity = taskTrackers.size() * maxCurrentTasks;

        //
        // Get map + reduce counts for the current tracker.
        //
        TaskTrackerStatus tts = (TaskTrackerStatus) taskTrackers.get(taskTracker);
        int numMaps = tts.countMapTasks();
        int numReduces = tts.countReduceTasks();

        //
        // In the below steps, we allocate first a map task (if appropriate),
        // and then a reduce task if appropriate.  We go through all jobs
        // in order of job arrival; jobs only get serviced if their 
        // predecessors are serviced, too.
        //

        //
        // We hand a task to the current taskTracker if the given machine 
        // has a workload that's equal to or less than the averageMaps 
        // +/- TASK_ALLOC_EPSILON.  (That epsilon is in place in case
        // there is an odd machine that is failing for some reason but 
        // has not yet been removed from the pool, making capacity seem
        // larger than it really is.)
        //
        if ((numMaps < maxCurrentTasks) &&
            (numMaps <= (avgMaps + TASK_ALLOC_EPSILON))) {

            int totalNeededMaps = 0;
            for (Iterator it = jobsByArrival.iterator(); it.hasNext(); ) {
                JobInProgress job = (JobInProgress) it.next();
                if (job.getStatus().getRunState() != JobStatus.RUNNING) {
                    continue;
                }

                Task t = job.obtainNewMapTask(taskTracker, tts);
                if (t != null) {
                    return t;
                }

                //
                // Beyond the highest-priority task, reserve a little 
                // room for failures and speculative executions; don't 
                // schedule tasks to the hilt.
                //
                totalNeededMaps += job.desiredMaps();
                double padding = 0;
                if (totalCapacity > MIN_SLOTS_FOR_PADDING) {
                    padding = Math.min(maxCurrentTasks, totalNeededMaps * PAD_FRACTION);
                }
                if (totalNeededMaps + padding >= totalCapacity) {
                    break;
                }
            }
        }

        //
        // Same thing, but for reduce tasks
        //
        if ((numReduces < maxCurrentTasks) &&
            (numReduces <= (avgReduces + TASK_ALLOC_EPSILON))) {

            int totalNeededReduces = 0;
            for (Iterator it = jobsByArrival.iterator(); it.hasNext(); ) {
                JobInProgress job = (JobInProgress) it.next();
                if (job.getStatus().getRunState() != JobStatus.RUNNING) {
                    continue;
                }

                Task t = job.obtainNewReduceTask(taskTracker, tts);
                if (t != null) {
                    return t;
                }

                //
                // Beyond the highest-priority task, reserve a little 
                // room for failures and speculative executions; don't 
                // schedule tasks to the hilt.
                //
                totalNeededReduces += job.desiredReduces();
                double padding = 0;
                if (totalCapacity > MIN_SLOTS_FOR_PADDING) {
                    padding = Math.min(maxCurrentTasks, totalNeededReduces * PAD_FRACTION);
                }
                if (totalNeededReduces + padding >= totalCapacity) {
                    break;
                }
            }
        }
        return null;
    }

    /**
     * A tracker wants to know if any of its Tasks have been
     * closed (because the job completed, whether successfully or not)
     */
    public String pollForTaskWithClosedJob(String taskTracker) {
        TreeSet taskIds = (TreeSet) trackerToTaskMap.get(taskTracker);
        if (taskIds != null) {
            for (Iterator it = taskIds.iterator(); it.hasNext(); ) {
                String taskId = (String) it.next();
                TaskInProgress tip = (TaskInProgress) taskidToTIPMap.get(taskId);
                if (tip.shouldCloseForClosedJob(taskId)) {
                    // 
                    // This is how the JobTracker ends a task at the TaskTracker.
                    // It may be successfully completed, or may be killed in
                    // mid-execution.
                    //
                    return taskId;
                }
            }
        }
        return null;
    }

    /**
     * A TaskTracker wants to know the physical locations of completed, but not
     * yet closed, tasks.  This exists so the reduce task thread can locate
     * map task outputs.
     */
    public synchronized MapOutputLocation[] locateMapOutputs(String taskId, String[][] mapTasksNeeded) {
        ArrayList v = new ArrayList();
        for (int i = 0; i < mapTasksNeeded.length; i++) {
            for (int j = 0; j < mapTasksNeeded[i].length; j++) {
                TaskInProgress tip = (TaskInProgress) taskidToTIPMap.get(mapTasksNeeded[i][j]);
                if (tip != null && tip.isComplete(mapTasksNeeded[i][j])) {
                    String trackerId = (String) taskidToTrackerMap.get(mapTasksNeeded[i][j]);
                    TaskTrackerStatus tracker = (TaskTrackerStatus) taskTrackers.get(trackerId);
                    v.add(new MapOutputLocation(mapTasksNeeded[i][j], tracker.getHost(), tracker.getPort()));
                    break;
                }
            }
        }
        // randomly shuffle results to load-balance map output requests
        Collections.shuffle(v);

        return (MapOutputLocation[]) v.toArray(new MapOutputLocation[v.size()]);
    }

    /**
     * Grab the local fs name
     */
    public synchronized String getFilesystemName() throws IOException {
        return fs.getName();
    }

    ////////////////////////////////////////////////////
    // JobSubmissionProtocol
    ////////////////////////////////////////////////////
    public synchronized JobStatus submitJob(String jobFile) throws IOException {
        totalSubmissions++;
        JobInProgress job = createJob(jobFile);
        return job.getStatus();
    }

    public synchronized void killJob(String jobid) {
        JobInProgress job = (JobInProgress) jobs.get(jobid);
        job.kill();
    }

    public synchronized JobProfile getJobProfile(String jobid) {
        JobInProgress job = (JobInProgress) jobs.get(jobid);
        if (job != null) {
            return job.getProfile();
        } else {
            return null;
        }
    }
    public synchronized JobStatus getJobStatus(String jobid) {
        JobInProgress job = (JobInProgress) jobs.get(jobid);
        if (job != null) {
            return job.getStatus();
        } else {
            return null;
        }
    }
    public synchronized Vector[] getMapTaskReport(String jobid) {
        JobInProgress job = (JobInProgress) jobs.get(jobid);
        if (job == null) {
            return new Vector[0];
        } else {
            Vector reports = new Vector();
            Vector completeMapTasks = job.reportTasksInProgress(true, true);
            for (Iterator it = completeMapTasks.iterator(); it.hasNext(); ) {
                TaskInProgress tip = (TaskInProgress) it.next();
                reports.add(tip.generateSingleReport());
            }
            Vector incompleteMapTasks = job.reportTasksInProgress(true, false);
            for (Iterator it = incompleteMapTasks.iterator(); it.hasNext(); ) {
                TaskInProgress tip = (TaskInProgress) it.next();
                reports.add(tip.generateSingleReport());
            }
            return (Vector[]) reports.toArray(new Vector[reports.size()]);
        }
    }

    public synchronized Vector[] getReduceTaskReport(String jobid) {
        JobInProgress job = (JobInProgress) jobs.get(jobid);
        if (job == null) {
            return new Vector[0];
        } else {
            Vector reports = new Vector();
            Vector completeReduceTasks = job.reportTasksInProgress(false, true);
            for (Iterator it = completeReduceTasks.iterator(); it.hasNext(); ) {
                TaskInProgress tip = (TaskInProgress) it.next();
                reports.add(tip.generateSingleReport());
            }
            Vector incompleteReduceTasks = job.reportTasksInProgress(false, false);
            for (Iterator it = incompleteReduceTasks.iterator(); it.hasNext(); ) {
                TaskInProgress tip = (TaskInProgress) it.next();
                reports.add(tip.generateSingleReport());
            }
            return (Vector[]) reports.toArray(new Vector[reports.size()]);
        }
    }

    ///////////////////////////////////////////////////////////////
    // JobTracker methods
    ///////////////////////////////////////////////////////////////
    public JobInProgress getJob(String jobid) {
        return (JobInProgress) jobs.get(jobid);
    }
    /**
     * Grab random num for task id
     */
    String createUniqueId() {
        return "" + Integer.toString(Math.abs(r.nextInt()),36);
    }

    /**
     * JobProfile createJob() kicks off a new job.  
     * This function creates a job profile and also decomposes it into
     * tasks.  The tasks are added to the unassignedTasks structure.  
     * (The precise structure will change as we get more sophisticated about 
     * task allocation.)
     *
     * Create a 'JobInProgress' object, which contains both JobProfile
     * and JobStatus.  Those two sub-objects are sometimes shipped outside
     * of the JobTracker.  But JobInProgress adds info that's useful for
     * the JobTracker alone.
     */
    JobInProgress createJob(String jobFile) throws IOException {
        JobInProgress job = new JobInProgress(jobFile, this, this.nutchConf);
        jobs.put(job.getProfile().getJobId(), job);
        jobsByArrival.add(job);
        return job;
    }

    ////////////////////////////////////////////////////
    // Methods to track all the TaskTrackers
    ////////////////////////////////////////////////////
    /**
     * Accept and process a new TaskTracker profile.  We might
     * have known about the TaskTracker previously, or it might
     * be brand-new.  All task-tracker structures have already
     * been updated.  Just process the contained tasks and any
     * jobs that might be affected.
     */
    void updateTaskStatuses(TaskTrackerStatus status) {
        for (Iterator it = status.taskReports(); it.hasNext(); ) {
            TaskStatus report = (TaskStatus) it.next();
            TaskInProgress tip = (TaskInProgress) taskidToTIPMap.get(report.getTaskId());
            if (tip == null) {
                LOG.info("Serious problem.  While updating status, cannot find taskid " + report.getTaskId());
            } else {
                JobInProgress job = tip.getJob();
                job.updateTaskStatus(tip, report);

                if (report.getRunState() == TaskStatus.SUCCEEDED) {
                    job.completedTask(tip, report.getTaskId());
                } else if (report.getRunState() == TaskStatus.FAILED) {
                    // Tell the job to fail the relevant task
                    job.failedTask(tip, report.getTaskId(), status.getTrackerName());
                }
            }
        }
    }

    /**
     * We lost the task tracker!  All task-tracker structures have 
     * already been updated.  Just process the contained tasks and any
     * jobs that might be affected.
     */
    void lostTaskTracker(String trackerName) {
        LOG.info("Lost tracker '" + trackerName + "'");
        TreeSet lostTasks = (TreeSet) trackerToTaskMap.get(trackerName);
        trackerToTaskMap.remove(trackerName);

        if (lostTasks != null) {
            for (Iterator it = lostTasks.iterator(); it.hasNext(); ) {
                String taskId = (String) it.next();
                TaskInProgress tip = (TaskInProgress) taskidToTIPMap.get(taskId);

                // Tell the job to fail the relevant task
                JobInProgress job = tip.getJob();
                job.failedTask(tip, taskId, trackerName);
            }
        }
    }

    ////////////////////////////////////////////////////////////
    // main()
    ////////////////////////////////////////////////////////////

    /**
     * Start the JobTracker process.  This is used only for debugging.  As a rule,
     * JobTracker should be run as part of the NDFS Namenode process.
     */
    public static void main(String argv[]) throws IOException, InterruptedException {
        if (argv.length != 0) {
          System.out.println("usage: JobTracker");
          System.exit(-1);
        }

        startTracker(new NutchConf());
    }
}
