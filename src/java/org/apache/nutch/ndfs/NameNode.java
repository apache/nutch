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
import org.apache.nutch.ipc.*;
import org.apache.nutch.util.*;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.logging.*;

/**********************************************************
 * NameNode controls two critical tables:
 *   1)  filename->blocksequence,version
 *   2)  block->machinelist
 *
 * The first table is stored on disk and is very precious.
 * The second table is rebuilt every time the NameNode comes
 * up.
 *
 * @author Mike Cafarella
 **********************************************************/
public class NameNode implements ClientProtocol, DatanodeProtocol, FSConstants {
    public static final Logger LOG = LogFormatter.getLogger("org.apache.nutch.ndfs.NameNode");

    FSNamesystem namesystem;
    Server server;

    /**
     * Create a NameNode at the default location
     */
    public NameNode() throws IOException {
        this(new File(NutchConf.get().get("ndfs.name.dir",
                                          "/tmp/nutch/ndfs/name")),
             DataNode.createSocketAddr
             (NutchConf.get().get("fs.default.name", "local")).getPort());
    }

    /**
     * Create a NameNode at the specified location
     */
    public NameNode(File dir, int port) throws IOException {
        this.namesystem = new FSNamesystem(dir);
        this.server = RPC.getServer(this, port, 10, false);
        this.server.start();
    }

    /**
     * Run forever
     */
    public void offerService() {
        try {
            this.server.join();
        } catch (InterruptedException ie) {
        }
    }

    /////////////////////////////////////////////////////
    // ClientProtocol
    /////////////////////////////////////////////////////
    /**
     */
    public LocatedBlock[] open(String src) throws IOException {
        Object openResults[] = namesystem.open(new UTF8(src));
        if (openResults == null) {
            throw new IOException("Cannot find filename " + src);
        } else {
            Block blocks[] = (Block[]) openResults[0];
            DatanodeInfo sets[][] = (DatanodeInfo[][]) openResults[1];
            LocatedBlock results[] = new LocatedBlock[blocks.length];
            for (int i = 0; i < blocks.length; i++) {
                results[i] = new LocatedBlock(blocks[i], sets[i]);
            }
            return results;
        }
    }

    /**
     */
    public LocatedBlock create(String src, String clientName, boolean overwrite) throws IOException {
        Object results[] = namesystem.startFile(new UTF8(src), new UTF8(clientName), overwrite);
        if (results == null) {
            throw new IOException("Cannot create file " + src);
        } else {
            Block b = (Block) results[0];
            DatanodeInfo targets[] = (DatanodeInfo[]) results[1];
            return new LocatedBlock(b, targets);
        }
    }

    /**
     */
    public LocatedBlock addBlock(String src) throws IOException {
        int retries = 5;
        Object results[] = namesystem.getAdditionalBlock(new UTF8(src));
        while (results != null && results[0] == null && retries > 0) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ie) {
            }
            results = namesystem.getAdditionalBlock(new UTF8(src));
            retries--;
        }

        if (results == null) {
            throw new IOException("Cannot obtain additional block for file " + src);
        } else if (results[0] == null) {
            return null;
        } else {
            Block b = (Block) results[0];
            DatanodeInfo targets[] = (DatanodeInfo[]) results[1];
            return new LocatedBlock(b, targets);
        }
    }

    /**
     */
    public void abandonBlock(Block b, String src) throws IOException {
        if (! namesystem.abandonBlock(b, new UTF8(src))) {
            throw new IOException("Cannot abandon block during write to " + src);
        }
    }
    /**
     */
    public void abandonFileInProgress(String src) throws IOException {
        namesystem.abandonFileInProgress(new UTF8(src));
    }
    /**
     */
    public boolean complete(String src, String clientName) throws IOException {
        int returnCode = namesystem.completeFile(new UTF8(src), new UTF8(clientName));
        if (returnCode == STILL_WAITING) {
            return false;
        } else if (returnCode == COMPLETE_SUCCESS) {
            return true;
        } else {
            throw new IOException("Could not complete write to file " + src + " by " + clientName);
        }
    }
    /**
     */
    public String[] getHints(String src, long offset) throws IOException {
        UTF8 hosts[] = namesystem.getDatanodeHints(new UTF8(src), offset);
        if (hosts == null) {
            return new String[0];
        } else {
            String results[] = new String[hosts.length];
            for (int i = 0; i < hosts.length; i++) {
                results[i] = hosts[i].toString();
            }
            return results;
        }
    }
    /**
     */
    public boolean rename(String src, String dst) throws IOException {
        return namesystem.renameTo(new UTF8(src), new UTF8(dst));
    }

    /**
     */
    public boolean delete(String src) throws IOException {
        return namesystem.delete(new UTF8(src));
    }

    /**
     */
    public boolean exists(String src) throws IOException {
        return namesystem.exists(new UTF8(src));
    }

    /**
     */
    public boolean isDir(String src) throws IOException {
        return namesystem.isDir(new UTF8(src));
    }

    /**
     */
    public boolean mkdirs(String src) throws IOException {
        return namesystem.mkdirs(new UTF8(src));
    }

    /**
     */
    public boolean obtainLock(String src, String clientName, boolean exclusive) throws IOException {
        int returnCode = namesystem.obtainLock(new UTF8(src), new UTF8(clientName), exclusive);
        if (returnCode == COMPLETE_SUCCESS) {
            return true;
        } else if (returnCode == STILL_WAITING) {
            return false;
        } else {
            throw new IOException("Failure when trying to obtain lock on " + src);
        }
    }

    /**
     */
    public boolean releaseLock(String src, String clientName) throws IOException {
        int returnCode = namesystem.releaseLock(new UTF8(src), new UTF8(clientName));
        if (returnCode == COMPLETE_SUCCESS) {
            return true;
        } else if (returnCode == STILL_WAITING) {
            return false;
        } else {
            throw new IOException("Failure when trying to release lock on " + src);
        }
    }

    /**
     */
    public void renewLease(String clientName) throws IOException {
        namesystem.renewLease(new UTF8(clientName));        
    }

    /**
     */
    public NDFSFileInfo[] getListing(String src) throws IOException {
        return namesystem.getListing(new UTF8(src));
    }

    /**
     */
    public long[] getStats() throws IOException {
        long results[] = new long[2];
        results[0] = namesystem.totalCapacity();
        results[1] = namesystem.totalCapacity() - namesystem.totalRemaining();
        return results;
    }

    /**
     */
    public DatanodeInfo[] getDatanodeReport() throws IOException {
        DatanodeInfo results[] = namesystem.datanodeReport();
        if (results == null || results.length == 0) {
            throw new IOException("Cannot find datanode report");
        }
        return results;
    }

    ////////////////////////////////////////////////////////////////
    // DatanodeProtocol
    ////////////////////////////////////////////////////////////////
    /**
     */
    public void sendHeartbeat(String sender, long capacity, long remaining) {
        namesystem.gotHeartbeat(new UTF8(sender), capacity, remaining);
    }

    public Block[] blockReport(String sender, Block blocks[]) {
        LOG.info("Block report from "+sender+": "+blocks.length+" blocks.");
        return namesystem.processReport(blocks, new UTF8(sender));
    }

    public void blockReceived(String sender, Block blocks[]) {
        for (int i = 0; i < blocks.length; i++) {
            namesystem.blockReceived(blocks[i], new UTF8(sender));
        }
    }

    /**
     */
    public void errorReport(String sender, String msg) {
        // Log error message from datanode
        //LOG.info("Report from " + sender + ": " + msg);
    }

    /**
     * Return a block-oriented command for the datanode to execute.
     * This will be either a transfer or a delete operation.
     */
    public BlockCommand getBlockwork(String sender, int xmitsInProgress) {
        //
        // Ask to perform pending transfers, if any
        //
        Object xferResults[] = namesystem.pendingTransfers(new DatanodeInfo(new UTF8(sender)), xmitsInProgress);
        if (xferResults != null) {
            return new BlockCommand((Block[]) xferResults[0], (DatanodeInfo[][]) xferResults[1]);
        }

        //
        // If there are no transfers, check for recently-deleted blocks that
        // should be removed.  This is not a full-datanode sweep, as is done during
        // a block report.  This is just a small fast removal of blocks that have
        // just been removed.
        //
        Block blocks[] = namesystem.blocksToInvalidate(new UTF8(sender));
        if (blocks != null) {
            return new BlockCommand(blocks);
        }
        return null;
    }

    /**
     */
    public static void main(String argv[]) throws IOException, InterruptedException {
        NameNode namenode = new NameNode();
        namenode.offerService();
    }
}
