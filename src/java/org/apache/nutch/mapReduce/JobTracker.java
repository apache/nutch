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
    static final int TRACKERINFO_PORT = 7845;

    public static final Logger LOG = LogFormatter.getLogger("org.apache.nutch.mapReduce.JobTracker");
    public static JobTracker tracker = null;
    public static void createTracker() throws IOException {
      createTracker(getDefaultAddress());
    }
    public static void createTracker(InetSocketAddress addr) throws IOException {
      tracker = new JobTracker(addr);
    }
    public static JobTracker getTracker() {
        return tracker;
    }

    public static InetSocketAddress getDefaultAddress() {
      String jobTrackerStr =
        NutchConf.get().get("mapred.job.tracker", "localhost:8012");
      int colon = jobTrackerStr.indexOf(":");
      if (colon < 0) {
        throw new RuntimeException("Bad mapred.job.tracker: "+jobTrackerStr);
      }
      String jobTrackerName = jobTrackerStr.substring(0, colon);
      int jobTrackerPort = Integer.parseInt(jobTrackerStr.substring(colon+1));
      return new InetSocketAddress(jobTrackerName, jobTrackerPort);
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
                                    taskTrackers.remove(leastRecent.getTrackerName());
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

    /////////////////////////////////////////////////////////////////
    // The real JobTracker
    ////////////////////////////////////////////////////////////////
    int port;
    String localMachine;
    long startTime;
    int totalSubmissions = 0;
    Random r = new Random();

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

    // (taskId->jobid that contains the task)
    TreeMap taskToJobMap = new TreeMap();

    // (taskId->trackerID) locates a given task
    TreeMap taskToTrackerMap = new TreeMap();

    // (trackerID->TreeSet of taskids running at that tracker)
    TreeMap trackerToTaskMap = new TreeMap();

    // a list of taskIds waiting for a task tracker to run on
    Vector unassignedTasks = new Vector();

    // Tasks that we've closed
    TreeSet tasksReportedClosed = new TreeSet();

    //
    // Watch and expire TaskTracker objects using these structures.
    // We can map from Name->TaskTrackerStatus, or we can expire by time.
    //
    TreeMap taskTrackers = new TreeMap();
    ExpireTrackers expireTrackers = new ExpireTrackers();

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
    Server interTrackerServer;

    // Some jobs are stored in a local system directory.  We can delete
    // the files when we're done with the job.
    File localDir;
    NutchFileSystem fs;
    File systemDir;

    /**
     * Start the JobTracker process, listen on the indicated port
     */
    JobTracker(InetSocketAddress addr) throws IOException {
        // This is a directory of temporary submission files.  We delete it
        // on startup, and can delete any files that we're done with
        this.systemDir = JobConf.getSystemDir();
        this.fs = NutchFileSystem.get();
        FileUtil.fullyDelete(fs, systemDir);
        fs.mkdirs(systemDir);

        // Same with 'localDir' except it's always on the local disk.
        this.localDir = JobConf.getLocalDir();
        FileUtil.fullyDelete(localDir);
        this.localDir.mkdirs();

        // Set ports, start RPC servers, etc.
        this.localMachine = addr.getHostName();
        this.port = addr.getPort();
        this.interTrackerServer = RPC.getServer(this, addr.getPort());
        this.interTrackerServer.start();
        this.infoServer = new JobTrackerInfoServer(this, TRACKERINFO_PORT);
        this.infoServer.start();

        this.startTime = System.currentTimeMillis();

        new Thread(this.expireTrackers).start();
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
            taskTrackers.remove(taskTrackerName);
            lostTaskTracker(taskTrackerName);
        }
    }

    /**
     * Process incoming heartbeat messages from the task trackers.
     */
    public IntWritable emitHeartbeat(TaskTrackerStatus trackerStatus, BooleanWritable initialContact) {
        String trackerName = trackerStatus.getTrackerName();
        trackerStatus.setLastSeen(System.currentTimeMillis());

        synchronized (taskTrackers) {
            synchronized (trackerExpiryQueue) {
                if (initialContact.get()) {
                    // If it's first contact, then clear out any state hanging around
                    if (taskTrackers.get(trackerName) != null) {
                        taskTrackers.remove(trackerName);
                        lostTaskTracker(trackerName);
                    }
                } else {
                    // If not first contact, there should be some record of the tracker
                    if (taskTrackers.get(trackerName) == null) {
                        return new IntWritable(InterTrackerProtocol.UNKNOWN_TASKTRACKER);
                    }
                }

                // Store latest state.  If first contact, then save current
                // state in expiry queue
                taskTrackers.put(trackerName, trackerStatus);
                if (initialContact.get()) {
                    trackerExpiryQueue.add(trackerStatus);
                }
            }
        }

        updateTaskStatuses(trackerStatus);
        //LOG.info("Got heartbeat from "+trackerName);
        return new IntWritable(InterTrackerProtocol.TRACKERS_OK);
    }

    /**
     * A tracker wants to know if there's a Task to run
     */
    public Task pollForNewTask(String trackerName) {
        //LOG.info("Unassigned tasks: " + unassignedTasks.size());

        // Allocate a pending task to this TaskTracker
        return getTaskAssignment(trackerName);
    }

    /**
     * A tracker wants to know if any of its Tasks have been
     * closed (because the job completed, whether successfully or not)
     */
    public String pollForClosedTask(String trackerName) {
        TreeSet taskIds = (TreeSet) trackerToTaskMap.get(trackerName);
        if (taskIds != null) {
            for (Iterator it = taskIds.iterator(); it.hasNext(); ) {
                String taskId = (String) it.next();
                String jobId = (String) taskToJobMap.get(taskId);
                JobInProgress job = (JobInProgress) jobs.get(jobId);
                int runState = job.getStatus().getRunState();

                if (! tasksReportedClosed.contains(taskId) &&
                    (runState == JobStatus.SUCCEEDED ||
                     runState == JobStatus.FAILED)) {
                    tasksReportedClosed.add(taskId);

                    //
                    // REMIND - mjc - what happens when a job dies, but no one
                    // arrives to claim the tasks?  I suppose when the job is
                    // eventually GC'ed, we make sure all its tasks are flushed
                    // from lookup tables.
                    //
                    return taskId;
                }
            }
        }
        return null;
    }

    /**
     * A tracker wants to know the physical locations of completed, but not
     * yet closed, tasks.  This exists so the reduce task thread can locate
     * map task outputs.
     * 
     */
    public MapOutputLocation[] locateMapOutputs(String taskId, String[] mapTasksNeeded) {
        JobInProgress job = (JobInProgress) jobs.get((String) taskToJobMap.get(taskId));
        MapOutputLocation outputs[] = job.locateTasks(mapTasksNeeded);
        return outputs;
    }

    public JobTracker.JobInProgress getJob(String jobid) {
        return (JobInProgress) jobs.get(jobid);
    }
    ////////////////////////////////////////////////////
    // JobSubmissionProtocol
    ////////////////////////////////////////////////////
    public JobStatus submitJob(String jobFile) throws IOException {
        totalSubmissions++;
        JobInProgress job = createJob(jobFile);
        return job.getStatus();
    }

    public void killJob(String jobid) {
        JobInProgress job = (JobInProgress) jobs.get(jobid);
        job.kill();
    }

    public JobProfile getJobProfile(String jobid) {
        JobInProgress job = (JobInProgress) jobs.get(jobid);
        if (job != null) {
            return job.getProfile();
        } else {
            return null;
        }
    }
    public JobStatus getJobStatus(String jobid) {
        JobInProgress job = (JobInProgress) jobs.get(jobid);
        if (job != null) {
            return job.getStatus();
        } else {
            return null;
        }
    }

    public String getFilesystemName() throws IOException {
        return fs.getName();
    }

    ///////////////////////////////////////////////////////////////
    // JobTracker methods
    ///////////////////////////////////////////////////////////////
    /**
     * JobProfile createJob() kicks off a new job.  
     * This function creates a job profile and also decomposes it into
     * tasks.  The tasks are added to the unassignedTasks structure.  
     * (The precise structure will change as we get more sophisticated about 
     * task allocation.)
     */
    JobInProgress createJob(String jobFile) throws IOException {
        JobInProgress job = new JobInProgress(jobFile);
        jobs.put(job.getProfile().getJobId(), job);
        job.launch();
        return job;
    }

    ///////////////////////////////////////////////////////
    // JobInProgress maintains all the info for keeping
    // a Job on the straight and narrow.  It keeps its JobProfile
    // and its latest JobStatus, plus a set of tables for 
    // doing bookkeeping of its Tasks.
    ///////////////////////////////////////////////////////
    public class JobInProgress {
        JobProfile profile;
        JobStatus status;
        Vector reducesToLaunch = new Vector();
        File localJobFile = null;

        TreeMap taskStatus = new TreeMap();
        TreeMap incompleteMapTasks = new TreeMap();
        TreeMap completeMapTasks = new TreeMap();
        TreeMap incompleteReduceTasks = new TreeMap();
        TreeMap completeReduceTasks = new TreeMap();

        // Info for user; useless for JobTracker
        int numMapTasks = 0;
        int numReduceTasks = 0;
        float totalReportedMapProgress = 0.0f;
        float totalReportedReduceProgress = 0.0f;
        int attemptedMapExecutions = 0;
        int attemptedReduceExecutions = 0;
        long startTime;
        long finishTime;
        String deleteUponCompletion = null;

        /**
         * Create a 'JobInProgress' object, which contains both JobProfile
         * and JobStatus.  Those two sub-objects are sometimes shipped outside
         * of the JobTracker.  But JobInProgress adds info that's useful for
         * the JobTracker alone.
         */
        public JobInProgress(String jobFile) throws IOException {
            String jobid = createJobId();
            String url = "http://" + localMachine + ":" + TRACKERINFO_PORT + "/jobdetails.jsp?jobid=" + jobid;
            this.profile = new JobProfile(jobid, jobFile, url);
            this.status = new JobStatus(jobid, 0.0f, 0.0f, JobStatus.RUNNING);

            this.localJobFile = new File(localDir, jobid+".xml");
            fs.copyToLocalFile(new File(jobFile), localJobFile);

            JobConf jd = new JobConf(localJobFile);
            this.numMapTasks = jd.getNumMapTasks();
            this.numReduceTasks = jd.getNumReduceTasks();
            this.startTime = System.currentTimeMillis();

            // If a jobFile is in the systemDir, we can delete it (and
            // its JAR) upon completion
            if (jobFile.startsWith(systemDir.getPath())) {
                this.deleteUponCompletion = jobFile;
            }
        }

        /**
         * Start up the tasks
         */
        public void launch() throws IOException {
            String jobid = profile.getJobId();
            String jobFile = profile.getJobFile();

            // construct input splits
            JobConf jd = new JobConf(localJobFile);
            NutchFileSystem fs = NutchFileSystem.get();
            FileSplit[] splits =
              jd.getInputFormat().getSplits(fs, jd, numMapTasks);

            // adjust number of map tasks to actual number of splits
            numMapTasks = splits.length;

            // create a map task for each split
            String mapIds[] = new String[numMapTasks];
            for (int i = 0; i < numMapTasks; i++) {
                mapIds[i] = createMapTaskId();
                Task t = new MapTask(jobFile, mapIds[i], splits[i]);

                incompleteMapTasks.put(mapIds[i], t);
                taskToJobMap.put(mapIds[i], jobid);
            }

            // Create reduce tasks
            for (int i = 0; i < numReduceTasks; i++) {
                String taskid = createReduceTaskId();
                Task t = new ReduceTask(jobFile, taskid, mapIds, i);
                reducesToLaunch.add(t);
                taskToJobMap.put(taskid, jobid);
            }

            // Launch the map tasks
            for (int i = 0; i < mapIds.length; i++) {
                executeTask(mapIds[i]);
            }
        }

        /**
         * Kill the job and all its component tasks.
         */
        public synchronized void kill() {
            this.status = new JobStatus(status.getJobId(), 1.0f, 1.0f, JobStatus.FAILED);
            this.finishTime = System.currentTimeMillis();
        }

        /**
         * The job is dead.  We're now GC'ing it, getting rid of the job
         * from all tables.  Be sure to remove all of this job's tasks
         * from the various tables.
         */
        public synchronized void garbageCollect() throws IOException {
            //
            // Remove this job from all tables
            //
            

            // Definitely remove the local-disk copy of the job file
            if (localJobFile != null) {
                localJobFile.delete();
                localJobFile = null;
            }

            //
            // If the job file was in the temporary system directory,
            // we should delete it upon garbage collect.
            //
            if (deleteUponCompletion != null) {
                JobConf jd = new JobConf(deleteUponCompletion);
                fs.delete(new File(jd.getJar()));
                fs.delete(new File(deleteUponCompletion));
                deleteUponCompletion = null;
            }
        }

        /**
         * A task assigned to this JobInProgress has reported in successfully.
         * 
         * This might prompt us to launch more tasks, or it might even indicate
         * the job is now complete.
         */
        public synchronized void completedTask(String taskid) {
            LOG.info("Task '" + taskid + "' has finished successfully.");

            Task t = null;
            if ((t = (Task) incompleteMapTasks.get(taskid)) != null) {
                incompleteMapTasks.remove(taskid);
                completeMapTasks.put(taskid, t);
            } else if ((t = (Task) incompleteReduceTasks.get(taskid)) != null) {
                incompleteReduceTasks.remove(taskid);
                completeReduceTasks.put(taskid, t);
            } else {
                // Impossible situation; taskid is not 'incomplete' yet is reported
                // as just finishing
                throw new IllegalArgumentException("Impossible state: task " + taskid + " reported as complete, but was not known as incomplete");
            }

            //
            // We may have Reduce tasks that are still waiting to be scheduled for execution;
            // we don't want to launch them all while still waiting for most Map tasks to
            // finish.  Nor do we want to wait for all the maps to finish, as the reduce task
            // could be downloading files while other map tasks complete.
            //
            // WELL, maybe we do want to wait.  It's not obvious what the right launch rate
            // would be.  For the moment, we wait until ALL map tasks have finished
            // before launching even a single reduce task.
            //
            while (status.getRunState() == JobStatus.RUNNING &&
                   reducesToLaunch.size() > 0 && 
                   completeMapTasks.size() == numMapTasks) {
                t = (Task) reducesToLaunch.elementAt(0);
                reducesToLaunch.removeElement(t);
                incompleteReduceTasks.put(t.getTaskId(), t);
                executeTask(t.getTaskId());
            }

            //
            // If all tasks are complete, then the job is done!
            //
            if (status.getRunState() == JobStatus.RUNNING &&
                completeReduceTasks.size() == numReduceTasks) {
                this.status = new JobStatus(status.getJobId(), 1.0f, 1.0f, JobStatus.SUCCEEDED);
                this.finishTime = System.currentTimeMillis();
            }
        }

        /**
         * A task assigned to this JobInProgress has reported in as failed.
         * Most of the time, we'll just reschedule execution.  However, after
         * many repeated failures we may instead decide to allow the entire 
         * job to fail.
         *
         * Even if a task has reported as completed in the past, it might later
         * be reported as failed.  That's because the TaskTracker that hosts a map
         * task might die before the entire job can complete.  If that happens,
         * we need to schedule reexecution so that downstream reduce tasks can 
         * obtain the map task's output.
         */
        public void failedTask(String taskid) {
            LOG.info("Task '" + taskid + "' has been lost.");

            Task t = null;
            if ((t = (Task) completeMapTasks.get(taskid)) != null) {
                completeMapTasks.remove(taskid);
                incompleteMapTasks.put(taskid, t);
            } else if ((t = (Task) completeReduceTasks.get(taskid)) != null) {
                completeReduceTasks.remove(taskid);
                incompleteReduceTasks.put(taskid, t);
            }
            if (status.getRunState() == JobStatus.RUNNING) {
                executeTask(taskid);
            }
        }

        /**
         * Return locations for all the indicated taskIds.  If the task
         * is not complete, don't return anything.
         */
        public MapOutputLocation[] locateTasks(String taskIds[]) {
            ArrayList v = new ArrayList();
            for (int i = 0; i < taskIds.length; i++) {
                if (completeMapTasks.get(taskIds[i]) != null) {
                    String trackerId = (String) taskToTrackerMap.get(taskIds[i]);
                    TaskTrackerStatus tracker = (TaskTrackerStatus) taskTrackers.get(trackerId);
                    v.add(new MapOutputLocation(taskIds[i], tracker.getHost(), tracker.getPort()));
                }
            }
            // randomly shuffle results to load-balance map output requests
            Collections.shuffle(v);

            return (MapOutputLocation[]) v.toArray(new MapOutputLocation[v.size()]);
        }

        //////////////////////////
        // A number of accessors
        //////////////////////////
        public JobProfile getProfile() {
            return profile;
        }
        public JobStatus getStatus() {
            return status;
        }
        public Task getTask(String taskid) {
            Task t = null;
            synchronized (incompleteMapTasks) {
                if ((t = (Task) incompleteMapTasks.get(taskid)) != null) {
                    return t;
                }
                synchronized (completeMapTasks) {
                    if ((t = (Task) completeMapTasks.get(taskid)) != null) {
                        return t;
                    }
                    synchronized (incompleteReduceTasks) {
                        if ((t = (Task) incompleteReduceTasks.get(taskid)) != null) {
                            return t;
                        }
                        synchronized (completeReduceTasks) {
                            t = (Task) completeReduceTasks.get(taskid);
                        }
                    }
                }
            }
            return t;
        }
        public float completedRatio() {
            return (0.5f * status.mapProgress()) + (0.5f * status.reduceProgress());
        }
        public long getStartTime() {
            return startTime;
        }
        public long getFinishTime() {
            return finishTime;
        }
        public int desiredMaps() {
            return numMapTasks;
        }
        public int attemptedMaps() {
            return attemptedMapExecutions;
        }
        public int completedMaps() {
            return completeMapTasks.size();
        }
        public int desiredReduces() {
            return numReduceTasks;
        }
        public int attemptedReduces() {
            return attemptedReduceExecutions;
        }
        public int completedReduces() {
            return completeReduceTasks.size();
        }
        public void updateTaskStatus(String taskid, TaskStatus status) {
            TaskStatus oldStatus = (TaskStatus) taskStatus.put(taskid, status);
            
            float delta = 0.0f;
            if (oldStatus != null) {
                delta -= oldStatus.getProgress();
            }
            delta += status.getProgress();

            if (incompleteMapTasks.get(taskid) != null || 
                completeMapTasks.get(taskid) != null) {
                totalReportedMapProgress += delta;
                if (numMapTasks == 0) {
                    this.status.setMapProgress(1.0f);
                } else {
                    this.status.setMapProgress((float) (totalReportedMapProgress / numMapTasks));
                }
            } else if (incompleteReduceTasks.get(taskid) != null ||
                       completeReduceTasks.get(taskid) != null) {
                totalReportedReduceProgress += delta;
                if (numReduceTasks == 0) {
                    this.status.setReduceProgress(1.0f);
                } else {
                    this.status.setReduceProgress((float) (totalReportedReduceProgress / numReduceTasks));
                }
            } else {
                LOG.info("Serious problem.  While updating status, cannot find taskid " + taskid);
            }
        }
        public TaskStatus getTaskStatus(String taskid) {
            return (TaskStatus) taskStatus.get(taskid);
        }

        /////////////////////////////
        // Some private util methods
        /////////////////////////////
        void executeTask(String taskid) {
            if (incompleteMapTasks.get(taskid) != null) {
                attemptedMapExecutions++;
            } else if (incompleteReduceTasks.get(taskid) != null) {
                attemptedReduceExecutions++;
            }
            updateTaskStatus(taskid, new TaskStatus(taskid, 0.0f, TaskStatus.UNASSIGNED));
            unassignedTasks.add(taskid);
        }
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
            JobInProgress job = (JobInProgress) jobs.get((String) taskToJobMap.get(report.getTaskId()));
            job.updateTaskStatus(report.getTaskId(), report);

            if (report.getRunState() == TaskStatus.SUCCEEDED) {
                job.completedTask(report.getTaskId());
            } else if (report.getRunState() == TaskStatus.FAILED) {
                TreeSet taskset = (TreeSet) trackerToTaskMap.get(status.getTrackerName());
                taskset.remove(report.getTaskId());
                taskToTrackerMap.remove(report.getTaskId());
                job.failedTask(report.getTaskId());
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
                taskToTrackerMap.remove(taskId);
                JobInProgress job = (JobInProgress) jobs.get((String) taskToJobMap.get(taskId));
                job.failedTask(taskId);
            }
        }
    }

    /**
     * Task getTaskAssignment() returns
     * a task we'd like the taskTracker to execute right now.
     *
     * Eventually this function should compute load on the various TaskTrackers,
     * and incorporate knowledge of NDFS file placement.  But for right now, it
     * just grabs a single item out of the pending task list and hands it back.
     */
    Task getTaskAssignment(String taskTracker) {
        if (unassignedTasks.size() > 0) {
            String taskid = (String) unassignedTasks.elementAt(0);
            unassignedTasks.remove(taskid);

            // Move task status to RUNNING
            JobInProgress job = (JobInProgress) jobs.get((String) taskToJobMap.get(taskid));
            job.updateTaskStatus(taskid, new TaskStatus(taskid, 0.0f, TaskStatus.RUNNING));

            // Remember where we are running it
            TreeSet taskset = (TreeSet) trackerToTaskMap.get(taskTracker);
            if (taskset == null) {
                taskset = new TreeSet();
                trackerToTaskMap.put(taskTracker, taskset);
            }
            LOG.info("Adding task '" + taskid + "' to set for tracker '" + taskTracker + "'");
            taskset.add(taskid);

            taskToTrackerMap.put(taskid, taskTracker);

            return job.getTask(taskid);
        } else {
            return null;
        }
    }

    /**
     * Grab random num for task id
     */
    String createMapTaskId() {
        return "task_m_" + Integer.toString(Math.abs(r.nextInt()),36);
    }
    String createReduceTaskId() {
        return "task_r_" + Integer.toString(Math.abs(r.nextInt()),36);
    }

    String createJobId() {
        return "job_" + Integer.toString(Math.abs(r.nextInt()),36);
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

        JobTracker.createTracker();
        JobTracker jt = JobTracker.getTracker();
        jt.offerService();
    }
}
