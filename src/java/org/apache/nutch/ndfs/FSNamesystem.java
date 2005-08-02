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
package org.apache.nutch.ndfs;

import org.apache.nutch.io.*;
import org.apache.nutch.util.*;

import java.io.*;
import java.util.*;
import java.util.logging.*;

/***************************************************
 * The FSNamesystem tracks several important tables.
 *
 * 1)  valid fsname --> blocklist  (kept on disk, logged)
 * 2)  Set of all valid blocks (inverted #1)
 * 3)  block --> machinelist (kept in memory, rebuilt dynamically from reports)
 * 4)  machine --> blocklist (inverted #2)
 * 5)  LRU cache of updated-heartbeat machines
 ***************************************************/
public class FSNamesystem implements FSConstants {
    public static final Logger LOG = LogFormatter.getLogger("org.apache.nutch.fs.FSNamesystem");

    // DESIRED_REPLICATION is how many copies we try to have at all times
    final static int DESIRED_REPLICATION =
      NutchConf.get().getInt("ndfs.replication", 3);

    // How many outgoing replication streams a given node should have at one time
    final static int MAX_REPLICATION_STREAMS = NutchConf.get().getInt("ndfs.max-repl-streams", 2);

    // MIN_REPLICATION is how many copies we need in place or else we disallow the write
    final static int MIN_REPLICATION = 1;

    // HEARTBEAT_RECHECK is how often a datanode sends its hearbeat
    final static long HEARTBEAT_RECHECK = 1000;

    //
    // Stores the correct file name hierarchy
    //
    FSDirectory dir;

    //
    // Stores the block-->datanode(s) map.  Updated only in response
    // to client-sent information.
    //
    TreeMap blocksMap = new TreeMap();

    //
    // Stores the datanode-->block map.  Done by storing a 
    // set of datanode info objects, sorted by name.  Updated only in
    // response to client-sent information.
    //
    TreeMap datanodeMap = new TreeMap();

    //
    // Keeps a Vector for every named machine.  The Vector contains
    // blocks that have recently been invalidated and are thought to live
    // on the machine in question.
    //
    TreeMap recentInvalidateSets = new TreeMap();

    //
    // Keeps track of files that are being created, plus the
    // blocks that make them up.
    //
    TreeMap pendingCreates = new TreeMap();

    //
    // Keeps track of the blocks that are part of those pending creates
    //
    TreeSet pendingCreateBlocks = new TreeSet();

    //
    // Stats on overall usage
    //
    long totalCapacity = 0, totalRemaining = 0;

    //
    Random r = new Random();

    //
    // Stores a set of datanode info objects, sorted by heartbeat
    //
    TreeSet heartbeats = new TreeSet(new Comparator() {
        public int compare(Object o1, Object o2) {
            DatanodeInfo d1 = (DatanodeInfo) o1;
            DatanodeInfo d2 = (DatanodeInfo) o2;            
            long lu1 = d1.lastUpdate();
            long lu2 = d2.lastUpdate();
            if (lu1 < lu2) {
                return -1;
            } else if (lu1 > lu2) {
                return 1;
            } else {
                return d1.getName().compareTo(d2.getName());
            }
        }
    });

    //
    // Store set of Blocks that need to be replicated 1 or more times.
    // We also store pending replication-orders.
    //
    TreeSet neededReplications = new TreeSet();
    TreeSet pendingReplications = new TreeSet();

    //
    // Used for handling lock-leases
    //
    TreeMap leases = new TreeMap();
    TreeSet sortedLeases = new TreeSet();

    //
    // Threaded object that checks to see if we have been
    // getting heartbeats from all clients. 
    //
    HeartbeatMonitor hbmon = null;
    LeaseMonitor lmon = null;
    Daemon hbthread = null, lmthread = null;
    boolean fsRunning = true;
    long systemStart = 0;
    
    /**
     * dir is where the filesystem directory state 
     * is stored
     */
    public FSNamesystem(File dir) throws IOException {
        this.dir = new FSDirectory(dir);
        this.hbthread = new Daemon(new HeartbeatMonitor());
        this.lmthread = new Daemon(new LeaseMonitor());
        hbthread.start();
        lmthread.start();
        this.systemStart = System.currentTimeMillis();
    }

    /**
     */
    public void close() {
        fsRunning = false;
        try {
            hbthread.join();
        } catch (InterruptedException ie) {
        }
        try {
            lmthread.join();
        } catch (InterruptedException ie) {
        }
    }

    /////////////////////////////////////////////////////////
    //
    // These methods are called by NutchFS clients
    //
    /////////////////////////////////////////////////////////
    /**
     * The client wants to open the given filename.  Return a
     * list of (block,machineArray) pairs.  The sequence of unique blocks
     * in the list indicates all the blocks that make up the filename.
     *
     * The client should choose one of the machines from the machineArray
     * at random.
     */
    public Object[] open(UTF8 src) {
        Object results[] = null;
        Block blocks[] = dir.getFile(src);
        if (blocks != null) {
            results = new Object[2];
            DatanodeInfo machineSets[][] = new DatanodeInfo[blocks.length][];

            for (int i = 0; i < blocks.length; i++) {
                TreeSet containingNodes = (TreeSet) blocksMap.get(blocks[i]);
                if (containingNodes == null) {
                    machineSets[i] = new DatanodeInfo[0];
                } else {
                    machineSets[i] = new DatanodeInfo[containingNodes.size()];
                    int j = 0;
                    for (Iterator it = containingNodes.iterator(); it.hasNext(); j++) {
                        machineSets[i][j] = (DatanodeInfo) it.next();
                    }
                }
            }

            results[0] = blocks;
            results[1] = machineSets;
        }
        return results;
    }

    /**
     * The client would like to create a new block for the indicated
     * filename.  Return an array that consists of the block, plus a set 
     * of machines.  The first on this list should be where the client 
     * writes data.  Subsequent items in the list must be provided in
     * the connection to the first datanode.
     */
    public synchronized Object[] startFile(UTF8 src, UTF8 holder, boolean overwrite) {
        Object results[] = null;
        if (pendingCreates.get(src) == null) {
            boolean fileValid = dir.isValidToCreate(src);
            if (overwrite && ! fileValid) {
                delete(src);
                fileValid = true;
            }

            if (fileValid) {
                results = new Object[2];

                // Get the array of replication targets 
                DatanodeInfo targets[] = chooseTargets(DESIRED_REPLICATION, null);
                if (targets.length < MIN_REPLICATION) {
                    LOG.info("Target-length is " + targets.length + ", below MIN_REPLICATION (" + MIN_REPLICATION + ")");
                    return null;
                }

                // Reserve space for this pending file
                pendingCreates.put(src, new Vector());
                synchronized (leases) {
                    Lease lease = (Lease) leases.get(holder);
                    if (lease == null) {
                        lease = new Lease(holder);
                        leases.put(holder, lease);
                        sortedLeases.add(lease);
                    } else {
                        sortedLeases.remove(lease);
                        lease.renew();
                        sortedLeases.add(lease);
                    }
                    lease.startedCreate(src);
                }

                // Create next block
                results[0] = allocateBlock(src);
                results[1] = targets;
            }
        } else {
            LOG.info("Cannot start file because pendingCreates is non-null");
        }
        return results;
    }

    /**
     * The client would like to obtain an additional block for the indicated
     * filename (which is being written-to).  Return an array that consists
     * of the block, plus a set of machines.  The first on this list should
     * be where the client writes data.  Subsequent items in the list must
     * be provided in the connection to the first datanode.
     *
     * Make sure the previous blocks have been reported by datanodes and
     * are replicated.  Will return an empty 2-elt array if we want the
     * client to "try again later".
     */
    public synchronized Object[] getAdditionalBlock(UTF8 src) {
        Object results[] = null;
        if (dir.getFile(src) == null && pendingCreates.get(src) != null) {
            results = new Object[2];

            //
            // If we fail this, bad things happen!
            //
            if (checkFileProgress(src)) {
                // Get the array of replication targets 
                DatanodeInfo targets[] = chooseTargets(DESIRED_REPLICATION, null);
                if (targets.length < MIN_REPLICATION) {
                    return null;
                }

                // Create next block
                results[0] = allocateBlock(src);
                results[1] = targets;
            } else {
                LOG.info("File progress failure for " + src);
                Vector v = (Vector) pendingCreates.get(src);
                for (Iterator it = v.iterator(); it.hasNext(); ) {
                    Block b = (Block) it.next();
                    TreeSet containingNodes = (TreeSet) blocksMap.get(b);
                    if (containingNodes == null || containingNodes.size() < MIN_REPLICATION) {
                        LOG.info("Problem with block " + b + ", with " + (containingNodes == null ? "0" : "" + containingNodes.size()) + " nodes reporting in.");
                    }
                }
            }
        }
        return results;
    }

    /**
     * The client would like to let go of the given block
     */
    public synchronized boolean abandonBlock(Block b, UTF8 src) {
        //
        // Remove the block from the pending creates list
        //
        Vector pendingVector = (Vector) pendingCreates.get(src);
        if (pendingVector != null) {
            for (Iterator it = pendingVector.iterator(); it.hasNext(); ) {
                Block cur = (Block) it.next();
                if (cur.compareTo(b) == 0) {
                    pendingCreateBlocks.remove(cur);
                    it.remove();
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Abandon the entire file in progress
     */
    public synchronized void abandonFileInProgress(UTF8 src) throws IOException {
        internalReleaseCreate(src);
    }

    /**
     * Finalize the created file and make it world-accessible.  The
     * FSNamesystem will already know the blocks that make up the file.
     * Before we return, we make sure that all the file's blocks have 
     * been reported by datanodes and are replicated correctly.
     */
    public synchronized int completeFile(UTF8 src, UTF8 holder) {
        if (dir.getFile(src) != null || pendingCreates.get(src) == null) {
	    LOG.info("Failed to complete " + src + "  because dir.getFile()==" + dir.getFile(src) + " and " + pendingCreates.get(src));
            return OPERATION_FAILED;
        } else if (! checkFileProgress(src)) {
            return STILL_WAITING;
        } else {
            Vector pendingVector = (Vector) pendingCreates.get(src);
            Block pendingBlocks[] = (Block[]) pendingVector.toArray(new Block[pendingVector.size()]);

            //
            // We have the pending blocks, but they won't have
            // length info in them (as they were allocated before
            // data-write took place).  So we need to add the correct
            // length info to each
            //
            // REMIND - mjc - this is very inefficient!  We should
            // improve this!
            //
            for (int i = 0; i < pendingBlocks.length; i++) {
                Block b = pendingBlocks[i];
                TreeSet containingNodes = (TreeSet) blocksMap.get(b);
                DatanodeInfo node = (DatanodeInfo) containingNodes.first();
                for (Iterator it = node.getBlockIterator(); it.hasNext(); ) {
                    Block cur = (Block) it.next();
                    if (b.getBlockId() == cur.getBlockId()) {
                        b.setNumBytes(cur.getNumBytes());
                        break;
                    }
                }
            }
            
            //
            // Now we can add the (name,blocks) tuple to the filesystem
            //
            if (dir.addFile(src, pendingBlocks)) {
                // The file is no longer pending
                pendingCreates.remove(src);
                for (int i = 0; i < pendingBlocks.length; i++) {
                    pendingCreateBlocks.remove(pendingBlocks[i]);
                }

                synchronized (leases) {
                    Lease lease = (Lease) leases.get(holder);
                    if (lease != null) {
                        lease.completedCreate(src);
                        if (! lease.hasLocks()) {
                            leases.remove(holder);
                            sortedLeases.remove(lease);
                        }
                    }
                }

                //
                // REMIND - mjc - this should be done only after we wait a few secs.
                // The namenode isn't giving datanodes enough time to report the
                // replicated blocks that are automatically done as part of a client
                // write.
                //

                // Now that the file is real, we need to be sure to replicate
                // the blocks.
                for (int i = 0; i < pendingBlocks.length; i++) {
                    TreeSet containingNodes = (TreeSet) blocksMap.get(pendingBlocks[i]);
                    if (containingNodes.size() < DESIRED_REPLICATION) {
                        synchronized (neededReplications) {
                            LOG.info("Completed file " + src + ", at holder " + holder + ".  There is/are only " + containingNodes.size() + " copies of block " + pendingBlocks[i] + ", so replicating up to " + DESIRED_REPLICATION);
                            neededReplications.add(pendingBlocks[i]);
                        }
                    }
                }
                return COMPLETE_SUCCESS;
            } else {
                System.out.println("AddFile() for " + src + " failed");
            }
	    LOG.info("Dropped through on file add....");
        }

        return OPERATION_FAILED;
    }

    /**
     * Allocate a block at the given pending filename
     */
    synchronized Block allocateBlock(UTF8 src) {
        Block b = new Block();
        Vector v = (Vector) pendingCreates.get(src);
        v.add(b);
        pendingCreateBlocks.add(b);
        return b;
    }

    /**
     * Check that the indicated file's blocks are present and
     * replicated.  If not, return false.
     */
    synchronized boolean checkFileProgress(UTF8 src) {
        Vector v = (Vector) pendingCreates.get(src);

        for (Iterator it = v.iterator(); it.hasNext(); ) {
            Block b = (Block) it.next();
            TreeSet containingNodes = (TreeSet) blocksMap.get(b);
            if (containingNodes == null || containingNodes.size() < MIN_REPLICATION) {
                return false;
            }
        }
        return true;
    }

    ////////////////////////////////////////////////////////////////
    // Here's how to handle block-copy failure during client write:
    // -- As usual, the client's write should result in a streaming
    // backup write to a k-machine sequence.
    // -- If one of the backup machines fails, no worries.  Fail silently.
    // -- Before client is allowed to close and finalize file, make sure
    // that the blocks are backed up.  Namenode may have to issue specific backup
    // commands to make up for earlier datanode failures.  Once all copies
    // are made, edit namespace and return to client.
    ////////////////////////////////////////////////////////////////

    /**
     * Change the indicated filename.
     */
    public boolean renameTo(UTF8 src, UTF8 dst) {
        return dir.renameTo(src, dst);
    }

    /**
     * Remove the indicated filename from the namespace.  This may
     * invalidate some blocks that make up the file.
     */
    public synchronized boolean delete(UTF8 src) {
        Block deletedBlocks[] = (Block[]) dir.delete(src);
        if (deletedBlocks != null) {
            for (int i = 0; i < deletedBlocks.length; i++) {
                Block b = deletedBlocks[i];

                TreeSet containingNodes = (TreeSet) blocksMap.get(b);
                if (containingNodes != null) {
                    for (Iterator it = containingNodes.iterator(); it.hasNext(); ) {
                        DatanodeInfo node = (DatanodeInfo) it.next();
                        Vector invalidateSet = (Vector) recentInvalidateSets.get(node.getName());
                        if (invalidateSet == null) {
                            invalidateSet = new Vector();
                            recentInvalidateSets.put(node.getName(), invalidateSet);
                        }
                        invalidateSet.add(b);
                    }
                }
            }
        }

        return (deletedBlocks != null);
    }

    /**
     * Return whether the given filename exists
     */
    public boolean exists(UTF8 src) {
        if (dir.getFile(src) != null || dir.isDir(src)) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Whether the given name is a directory
     */
    public boolean isDir(UTF8 src) {
        return dir.isDir(src);
    }

    /**
     * Create all the necessary directories
     */
    public boolean mkdirs(UTF8 src) {
        return dir.mkdirs(src);
    }

    /**
     * Figure out a few hosts that are likely to contain the
     * block referred to by the given filename, offset pair.
     */
    public UTF8[] getDatanodeHints(UTF8 src, long offset) {
        Block targetBlock = null;
        Block blocks[] = dir.getFile(src);

        //
        // First, figure out where the offset would fall in
        // the blocklist.
        //
        for (int i = 0; i < blocks.length; i++) {
            offset -= blocks[i].getNumBytes();
            if (offset <= 0) {
                targetBlock = blocks[i];
                break;
            }
        }

        //
        // Next, create an array of hosts where that block can
        // be found
        //
        UTF8 hosts[] = null;
        if (targetBlock != null) {
            TreeSet containingNodes = (TreeSet) blocksMap.get(targetBlock);
            Vector v = new Vector();
            for (Iterator it = containingNodes.iterator(); it.hasNext(); ) {
                DatanodeInfo cur = (DatanodeInfo) it.next();
                v.add(cur.getHost());
            }
            hosts = (UTF8[]) v.toArray(new UTF8[v.size()]);
        }
        return hosts;
    }

    /************************************************************
     * A Lease governs all the locks held by a single client.
     * For each client there's a corresponding lease, whose
     * timestamp is updated when the client periodically
     * checks in.  If the client dies and allows its lease to
     * expire, all the corresponding locks can be released.
     *************************************************************/
    class Lease implements Comparable {
        public UTF8 holder;
        public long lastUpdate;
        TreeSet locks = new TreeSet();
        TreeSet creates = new TreeSet();

        public Lease(UTF8 holder) {
            this.holder = holder;
            renew();
        }
        public void renew() {
            this.lastUpdate = System.currentTimeMillis();
        }
        public boolean expired() {
            if (System.currentTimeMillis() - lastUpdate > LEASE_PERIOD) {
                return true;
            } else {
                return false;
            }
        }
        public void obtained(UTF8 src) {
            locks.add(src);
        }
        public void released(UTF8 src) {
            locks.remove(src);
        }
        public void startedCreate(UTF8 src) {
            creates.add(src);
        }
        public void completedCreate(UTF8 src) {
            creates.remove(src);
        }
        public boolean hasLocks() {
            return (locks.size() + creates.size()) > 0;
        }
        public void releaseLocks() {
            for (Iterator it = locks.iterator(); it.hasNext(); ) {
                UTF8 src = (UTF8) it.next();
                internalReleaseLock(src, holder);
            }
            locks.clear();
            for (Iterator it = creates.iterator(); it.hasNext(); ) {
                UTF8 src = (UTF8) it.next();
                internalReleaseCreate(src);
            }
            creates.clear();
        }

        /**
         */
        public String toString() {
            return "[Lease.  Holder: " + holder.toString() + ", heldlocks: " + locks.size() + ", pendingcreates: " + creates.size() + "]";
        }

        /**
         */
        public int compareTo(Object o) {
            Lease l1 = (Lease) this;
            Lease l2 = (Lease) o;
            long lu1 = l1.lastUpdate;
            long lu2 = l2.lastUpdate;
            if (lu1 < lu2) {
                return -1;
            } else if (lu1 > lu2) {
                return 1;
            } else {
                return l1.holder.compareTo(l2.holder);
            }
        }
    }
    /******************************************************
     * LeaseMonitor checks for leases that have expired,
     * and disposes of them.
     ******************************************************/
    class LeaseMonitor implements Runnable {
        public void run() {
            while (fsRunning) {
                synchronized (FSNamesystem.this) {
                    synchronized (leases) {
                        Lease top;
                        while ((sortedLeases.size() > 0) &&
                               ((top = (Lease) sortedLeases.first()) != null)) {
                            if (top.expired()) {
                                top.releaseLocks();
                                leases.remove(top.holder);
                                LOG.info("Removing lease " + top + ", leases remaining: " + sortedLeases.size());
                                if (!sortedLeases.remove(top)) {
                                    LOG.info("Unknown failure trying to remove " + top + " from lease set.");
                                }
                            } else {
                                break;
                            }
                        }
                    }
                }
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ie) {
                }
            }
        }
    }

    /**
     * Get a lock (perhaps exclusive) on the given file
     */
    public synchronized int obtainLock(UTF8 src, UTF8 holder, boolean exclusive) {
        int result = dir.obtainLock(src, holder, exclusive);
        if (result == COMPLETE_SUCCESS) {
            synchronized (leases) {
                Lease lease = (Lease) leases.get(holder);
                if (lease == null) {
                    lease = new Lease(holder);
                    leases.put(holder, lease);
                    sortedLeases.add(lease);
                } else {
                    sortedLeases.remove(lease);
                    lease.renew();
                    sortedLeases.add(lease);
                }
                lease.obtained(src);
            }
        }
        return result;
    }

    /**
     * Release the lock on the given file
     */
    public synchronized int releaseLock(UTF8 src, UTF8 holder) {
        int result = internalReleaseLock(src, holder);
        if (result == COMPLETE_SUCCESS) {
            synchronized (leases) {
                Lease lease = (Lease) leases.get(holder);
                if (lease != null) {
                    lease.released(src);
                    if (! lease.hasLocks()) {
                        leases.remove(holder);
                        sortedLeases.remove(lease);
                    }
                }
            }
        }
        return result;
    }
    private int internalReleaseLock(UTF8 src, UTF8 holder) {
        return dir.releaseLock(src, holder);
    }
    private void internalReleaseCreate(UTF8 src) {
        Vector v = (Vector) pendingCreates.remove(src);
        for (Iterator it2 = v.iterator(); it2.hasNext(); ) {
            Block b = (Block) it2.next();
            pendingCreateBlocks.remove(b);
        }
    }

    /**
     * Renew the lease(s) held by the given client
     */
    public void renewLease(UTF8 holder) {
        synchronized (leases) {
            Lease lease = (Lease) leases.get(holder);
            if (lease != null) {
                sortedLeases.remove(lease);
                lease.renew();
                sortedLeases.add(lease);
            }
        }
    }

    /**
     * Get a listing of all files at 'src'.  The Object[] array
     * exists so we can return file attributes (soon to be implemented)
     */
    public NDFSFileInfo[] getListing(UTF8 src) {
        return dir.getListing(src);
    }

    /////////////////////////////////////////////////////////
    //
    // These methods are called by datanodes
    //
    /////////////////////////////////////////////////////////
    /**
     * The given node has reported in.  This method should:
     * 1) Record the heartbeat, so the datanode isn't timed out
     * 2) Adjust usage stats for future block allocation
     */
    public void gotHeartbeat(UTF8 name, long capacity, long remaining) {
        synchronized (heartbeats) {
            synchronized (datanodeMap) {
                long capacityDiff = 0;
                long remainingDiff = 0;
                DatanodeInfo nodeinfo = (DatanodeInfo) datanodeMap.get(name);

                if (nodeinfo == null) {
                    LOG.info("Got brand-new heartbeat from " + name);
                    nodeinfo = new DatanodeInfo(name, capacity, remaining);
                    datanodeMap.put(name, nodeinfo);
                    capacityDiff = capacity;
                    remainingDiff = remaining;
                } else {
                    capacityDiff = capacity - nodeinfo.getCapacity();
                    remainingDiff = remaining - nodeinfo.getRemaining();
                    heartbeats.remove(nodeinfo);
                    nodeinfo.updateHeartbeat(capacity, remaining);
                }
                heartbeats.add(nodeinfo);
                totalCapacity += capacityDiff;
                totalRemaining += remainingDiff;
            }
        }
    }

    /**
     * Periodically calls heartbeatCheck().
     */
    class HeartbeatMonitor implements Runnable {
        /**
         */
        public void run() {
            while (fsRunning) {
                heartbeatCheck();
                try {
                    Thread.sleep(HEARTBEAT_RECHECK);
                } catch (InterruptedException ie) {
                }
            }
        }
    }

    /**
     * Check if there are any expired heartbeats, and if so,
     * whether any blocks have to be re-replicated.
     */
    synchronized void heartbeatCheck() {
        synchronized (heartbeats) {
            DatanodeInfo nodeInfo = null;

            while ((heartbeats.size() > 0) &&
                   ((nodeInfo = (DatanodeInfo) heartbeats.first()) != null) &&
                   (nodeInfo.lastUpdate() < System.currentTimeMillis() - EXPIRE_INTERVAL)) {
                LOG.info("Lost heartbeat for " + nodeInfo.getName());

                heartbeats.remove(nodeInfo);
                synchronized (datanodeMap) {
                    datanodeMap.remove(nodeInfo.getName());
                }
                totalCapacity -= nodeInfo.getCapacity();
                totalRemaining -= nodeInfo.getRemaining();

                Block deadblocks[] = nodeInfo.getBlocks();
                if (deadblocks != null) {
                    for (int i = 0; i < deadblocks.length; i++) {
                        removeStoredBlock(deadblocks[i], nodeInfo);
                    }
                }

                if (heartbeats.size() > 0) {
                    nodeInfo = (DatanodeInfo) heartbeats.first();
                }
            }
        }
    }
    
    /**
     * The given node is reporting all its blocks.  Use this info to 
     * update the (machine-->blocklist) and (block-->machinelist) tables.
     */
    public synchronized void processReport(Block newReport[], UTF8 name) {
        DatanodeInfo node = (DatanodeInfo) datanodeMap.get(name);
        if (node == null) {
            throw new IllegalArgumentException("Unexpected exception.  Received block report from node " + name + ", but there is no info for " + name);
        }

        //
        // Modify the (block-->datanode) map, according to the difference
        // between the old and new block report.
        //
        int oldPos = 0, newPos = 0;
        Block oldReport[] = node.getBlocks();
        while (oldReport != null && newReport != null && oldPos < oldReport.length && newPos < newReport.length) {
            int cmp = oldReport[oldPos].compareTo(newReport[newPos]);
            
            if (cmp == 0) {
                // Do nothing, blocks are the same
                oldPos++;
                newPos++;
            } else if (cmp < 0) {
                // The old report has a block the new one does not
                removeStoredBlock(oldReport[oldPos], node);
                oldPos++;
            } else {
                // The new report has a block the old one does not
                addStoredBlock(newReport[newPos], node);
                newPos++;
            }
        }
        while (oldReport != null && oldPos < oldReport.length) {
            // The old report has a block the new one does not
            removeStoredBlock(oldReport[oldPos], node);
            oldPos++;
        }
        while (newReport != null && newPos < newReport.length) {
            // The new report has a block the old one does not
            addStoredBlock(newReport[newPos], node);
            newPos++;
        }

        //
        // Modify node so it has the new blockreport
        //
        node.updateBlocks(newReport);
    }

    /**
     * Modify (block-->datanode) map.  Remove block from set of 
     * needed replications if this takes care of the problem.
     */
    synchronized void addStoredBlock(Block block, DatanodeInfo node) {
        TreeSet containingNodes = (TreeSet) blocksMap.get(block);
        if (containingNodes == null) {
            containingNodes = new TreeSet();
            blocksMap.put(block, containingNodes);
        }
        if (! containingNodes.contains(node)) {
            containingNodes.add(node);
        } else {
            LOG.info("Redundant addStoredBlock request received for block " + block + " on node " + node);
        }

        synchronized (neededReplications) {
            if (dir.isValidBlock(block)) {
              //LOG.info("Node " + node + " is reporting stored block " + block);
                if (containingNodes.size() >= DESIRED_REPLICATION) {
                    neededReplications.remove(block);
                    pendingReplications.remove(block);
                } else if (containingNodes.size() < DESIRED_REPLICATION) {
                    if (! neededReplications.contains(block)) {
                        neededReplications.add(block);
                    }
                }
            }
        }
    }

    /**
     * Modify (block-->datanode) map.  Possibly generate 
     * replication tasks, if the removed block is still valid.
     */
    synchronized void removeStoredBlock(Block block, DatanodeInfo node) {
        TreeSet containingNodes = (TreeSet) blocksMap.get(block);
        if (containingNodes == null || ! containingNodes.contains(node)) {
            throw new IllegalArgumentException("No machine mapping found for block " + block + ", which should be at node " + node);
        }
        containingNodes.remove(node);

        //
        // It's possible that the block was removed because of a datanode
        // failure.  If the block is still valid, check if replication is
        // necessary.  In that case, put block on a possibly-will-
        // be-replicated list.
        //
        if (dir.isValidBlock(block) && (containingNodes.size() < DESIRED_REPLICATION)) {
            synchronized (neededReplications) {
                neededReplications.add(block);
            }
        }
    }

    /**
     * The given node is reporting that it received a certain block.
     */
    public synchronized void blockReceived(Block block, UTF8 name) {
        DatanodeInfo node = (DatanodeInfo) datanodeMap.get(name);
        if (node == null) {
            throw new IllegalArgumentException("Unexpected exception.  Got blockReceived message from node " + name + ", but there is no info for " + name);
        }
        //
        // Modify the blocks->datanode map
        // 
        addStoredBlock(block, node);

        //
        // Supplement node's blockreport
        //
        node.addBlock(block);
    }

    /**
     * Total raw bytes
     */
    public long totalCapacity() {
        return totalCapacity;
    }

    /**
     * Total non-used raw bytes
     */
    public long totalRemaining() {
        return totalRemaining;
    }

    /**
     */
    public DatanodeInfo[] datanodeReport() {
        DatanodeInfo results[] = null;
        synchronized (heartbeats) {
            synchronized (datanodeMap) {
                results = new DatanodeInfo[datanodeMap.size()];
                int i = 0;
                for (Iterator it = datanodeMap.values().iterator(); it.hasNext(); ) {
                    DatanodeInfo cur = (DatanodeInfo) it.next();
                    results[i++] = cur;
                }
            }
        }
        return results;
    }

    /////////////////////////////////////////////////////////
    //
    // These methods are called by the Namenode system, to see
    // if there is any work for a given datanode.
    //
    /////////////////////////////////////////////////////////

    /**
     * Return with a list of Blocks that should be invalidated
     * at the given node.  Done in response to a file delete, which 
     * eliminates a number of blocks from the universe.
     */
    public synchronized Block[] recentlyInvalidBlocks(UTF8 name) {
        Vector invalidateSet = (Vector) recentInvalidateSets.remove(name);
        if (invalidateSet == null) {
            return null;
        } else {
            return (Block[]) invalidateSet.toArray(new Block[invalidateSet.size()]);
        }
    }

    /**
     * If the node has not been checked in some time, go through
     * its blocks and find which ones are neither valid nor pending.
     * It often happens that a client will start writing blocks and
     * then exit.  The blocks are on-disk, but the file will be 
     * abandoned.
     *
     * It's not enough to invalidate blocks at lease expiry time;
     * datanodes can go down before the client's lease on 
     * the failed file expires and miss the "expire" event.
     *
     * This function considers every block on a datanode, and thus
     * should only be invoked infrequently.
     */
    public synchronized Block[] checkObsoleteBlocks(UTF8 name) {
        DatanodeInfo nodeInfo = (DatanodeInfo) datanodeMap.get(name);
        if (System.currentTimeMillis() - nodeInfo.lastObsoleteCheck() <= OBSOLETE_INTERVAL) {
            return null;
        } else {
            nodeInfo.updateObsoleteCheck();
            Vector obsolete = new Vector();
            for (Iterator it = nodeInfo.getBlockIterator(); it.hasNext(); ) {
                Block b = (Block) it.next();

                if (! dir.isValidBlock(b) && ! pendingCreateBlocks.contains(b)) {
                    LOG.info("Obsoleting block " + b);
                    obsolete.add(b);
                }
            }
            return (Block[]) obsolete.toArray(new Block[obsolete.size()]);
        }
    }

    /**
     * Return with a list of Block/DataNodeInfo sets, indicating
     * where various Blocks should be copied, ASAP.
     *
     * The Array that we return consists of two objects:
     * The 1st elt is an array of Blocks.
     * The 2nd elt is a 2D array of DatanodeInfo objs, identifying the
     *     target sequence for the Block at the appropriate index.
     *
     */
    public synchronized Object[] pendingTransfers(DatanodeInfo srcNode, int xmitsInProgress) {
        synchronized (neededReplications) {
            Object results[] = null;
	    int scheduledXfers = 0;

            if (neededReplications.size() > 0) {
                //
                // Go through all blocks that need replications.  See if any
                // are present at the current node.  If so, ask the node to
                // replicate them.
                //
                Vector replicateBlocks = new Vector();
                Vector replicateTargetSets = new Vector();
                for (Iterator it = neededReplications.iterator(); it.hasNext(); ) {
                    //
                    // We can only reply with 'maxXfers' or fewer blocks
                    //
                    if (scheduledXfers >= MAX_REPLICATION_STREAMS - xmitsInProgress) {
                        break;
                    }

                    Block block = (Block) it.next();
                    if (! dir.isValidBlock(block)) {
                        it.remove();
                    } else {
                        TreeSet containingNodes = (TreeSet) blocksMap.get(block);

                        if (containingNodes.contains(srcNode)) {
                            DatanodeInfo targets[] = chooseTargets(Math.min(DESIRED_REPLICATION - containingNodes.size(), MAX_REPLICATION_STREAMS - xmitsInProgress), containingNodes);
                            if (targets.length > 0) {
                                // Build items to return
                                replicateBlocks.add(block);
                                replicateTargetSets.add(targets);
				scheduledXfers += targets.length;
                            }
                        }
                    }
                }

                //
                // Move the block-replication into a "pending" state.
                // The reason we use 'pending' is so we can retry
                // replications that fail after an appropriate amount of time.  
                // (REMIND - mjc - this timer is not yet implemented.)
                //
                if (replicateBlocks.size() > 0) {
                    int i = 0;
                    for (Iterator it = replicateBlocks.iterator(); it.hasNext(); i++) {
                        Block block = (Block) it.next();
                        DatanodeInfo targets[] = (DatanodeInfo[]) replicateTargetSets.elementAt(i);
                        TreeSet containingNodes = (TreeSet) blocksMap.get(block);

                        if (containingNodes.size() + targets.length >= DESIRED_REPLICATION) {
                            neededReplications.remove(block);
                            pendingReplications.add(block);
                        }

			LOG.info("Pending transfer (block " + block.getBlockName() + ") from " + srcNode.getName() + " to " + targets.length + " destinations");
                    }

                    //
                    // Build returned objects from above lists
                    //
                    DatanodeInfo targetMatrix[][] = new DatanodeInfo[replicateTargetSets.size()][];
                    for (i = 0; i < targetMatrix.length; i++) {
                        targetMatrix[i] = (DatanodeInfo[]) replicateTargetSets.elementAt(i);
                    }

                    results = new Object[2];
                    results[0] = replicateBlocks.toArray(new Block[replicateBlocks.size()]);
                    results[1]  = targetMatrix;
                }
            }
            return results;
        }
    }


    /**
     * Get a certain number of targets, if possible.  If not,
     * return as many as we can.
     */
    DatanodeInfo[] chooseTargets(int desiredReplicates, TreeSet forbiddenNodes) {
        TreeSet alreadyChosen = new TreeSet();
        Vector targets = new Vector();

        for (int i = 0; i < desiredReplicates; i++) {
            DatanodeInfo target = chooseTarget(forbiddenNodes, alreadyChosen);
            if (target != null) {
                targets.add(target);
                alreadyChosen.add(target);
            } else {
                break;
            }
        }
        return (DatanodeInfo[]) targets.toArray(new DatanodeInfo[targets.size()]);
    }

    /**
     * Choose a target from available machines, excepting the
     * given ones.
     *
     * Right now it chooses randomly from available boxes.  In future could 
     * choose according to capacity and load-balancing needs (or even 
     * network-topology, to avoid inter-switch traffic).
     */
    DatanodeInfo chooseTarget(TreeSet alreadyHasNode, TreeSet alreadyChosen) {
        int totalMachines = datanodeMap.size();
        if (totalMachines == 0) {
            LOG.info("While choosing target, totalMachines is " + totalMachines);
            return null;
        }
        int freeMachines = totalMachines;
        for (Iterator it = datanodeMap.values().iterator(); it.hasNext(); ) {
            DatanodeInfo node = (DatanodeInfo) it.next();
            if ((alreadyHasNode != null && alreadyHasNode.contains(node)) ||
                (alreadyChosen != null && alreadyChosen.contains(node))) {
                freeMachines--;
            }
        }

        //
        // Now pick one
        //
        DatanodeInfo target = null;
        if (freeMachines > 0) {
            //
            // Get all possible targets
            //
            int i = 0;
            DatanodeInfo targetlist[] = new DatanodeInfo[totalMachines];
            for (Iterator it = datanodeMap.values().iterator(); it.hasNext(); i++) {
                targetlist[i] = (DatanodeInfo) it.next();
            }
        
            do {
                int index = r.nextInt(totalMachines);
                target = targetlist[index];

                if ((alreadyHasNode != null && alreadyHasNode.contains(target)) ||
                    (alreadyChosen != null && alreadyChosen.contains(target))) {
                    target = null;
                }
            } while (target == null);
        }
        return target;

        /**
         * Choose target weighted by available storage
         */
        /**
        synchronized (datanodeMap) {
            if (datanodeMap.size() == 0) {
                return;
            }

            long totalRemaining = 0;
            Vector okTargets = new Vector();
            for (Iterator it = datanodeMap.values().iterator(); it.hasNext(); ) {
                DatanodeInfo node = (DatanodeInfo) it.next();
                if ((alreadyHasNode == null || ! alreadyHasNode.contains(node)) &&
                    (alreadyChosen == null || ! alreadyChosen.contains(node))) {
                    okTargets.add(node);
                    totalRemaining += node.getRemaining();
                }
            }

            //
            // Now pick one
            //
            DatanodeInfo target = null;
            if (okTargets.size() > 0) {
                //
                // Repeatedly choose random byte of the total bytes free.
                // The machine that has that byte will be our target.  Thus,
                // we select at random with bias toward machines with greater
                // available storage.
                //
                long targetByte = r.nextLong(totalRemaining);
                for (Iterator it = okTargets.iterator(); it.hasNext(); ) {
                    DatanodeInfo node = (DatanodeInfO) it.next();
                    targetByte -= node.getRemaining();
                    if (targetByte <= 0) {
                        target = node;
                        break;
                    }
                }
            }
            return target;
        }
        **/
    }
}
