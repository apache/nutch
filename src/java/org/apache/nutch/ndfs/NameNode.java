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
        long start = System.currentTimeMillis();
        try {
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
        } finally {
            long end = System.currentTimeMillis();
            opCounts++;
            opTime += (end - start);
        }
    }

    /**
     */
    public LocatedBlock create(String src, String clientName, boolean overwrite) throws IOException {
        long start = System.currentTimeMillis();
        try {
        Object results[] = namesystem.startFile(new UTF8(src), new UTF8(clientName), overwrite);
        if (results == null) {
            throw new IOException("Cannot create file " + src);
        } else {
            Block b = (Block) results[0];
            DatanodeInfo targets[] = (DatanodeInfo[]) results[1];
            return new LocatedBlock(b, targets);
        }
        } finally {
            long end = System.currentTimeMillis();
            crCounts++;
            crTime += (end - start);
        }
    }

    /**
     */
    public LocatedBlock addBlock(String src) throws IOException {
        long start = System.currentTimeMillis();
        try {
        Object results[] = namesystem.getAdditionalBlock(new UTF8(src));
        if (results != null && results[0] == null) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException ie) {
            }
            results = namesystem.getAdditionalBlock(new UTF8(src));
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
        } finally {
            long end = System.currentTimeMillis();
            addbCounts++;
            addbTime += (end - start);
        }
    }

    /**
     */
    public void abandonBlock(Block b, String src) throws IOException {
        long start = System.currentTimeMillis();
        try {
        if (! namesystem.abandonBlock(b, new UTF8(src))) {
            throw new IOException("Cannot abandon block during write to " + src);
        }
        } finally {
            long end = System.currentTimeMillis();
            abCounts++;
            abTime += (end - start);
        }
    }
    /**
     */
    public void abandonFileInProgress(String src) throws IOException {
        long start = System.currentTimeMillis();
        try {
        namesystem.abandonFileInProgress(new UTF8(src));
        } finally {
            long end = System.currentTimeMillis();
            afCounts++;
            afTime += (end - start);
        }
    }
    /**
     */
    public boolean complete(String src, String clientName) throws IOException {
        long start = System.currentTimeMillis();
        try {
        int returnCode = namesystem.completeFile(new UTF8(src), new UTF8(clientName));
        if (returnCode == STILL_WAITING) {
            return false;
        } else if (returnCode == COMPLETE_SUCCESS) {
            return true;
        } else {
            throw new IOException("Could not complete write to file " + src + " by " + clientName);
        }
        } finally {
            long end = System.currentTimeMillis();
            coCounts++;
            coTime += (end - start);
        }
    }
    /**
     */
    public String[] getHints(String src, long offset) throws IOException {
        long start = System.currentTimeMillis();
        try {
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
        } finally {
            long end = System.currentTimeMillis();
            ghCounts++;
            ghTime += (end - start);
        }
    }
    /**
     */
    public boolean rename(String src, String dst) throws IOException {
        long start = System.currentTimeMillis();
        try {
        return namesystem.renameTo(new UTF8(src), new UTF8(dst));
        } finally {
            long end = System.currentTimeMillis();
            rnCounts++;
            rnTime += (end - start);
        }
    }

    /**
     */
    public boolean delete(String src) throws IOException {
        long start = System.currentTimeMillis();
        try {
        return namesystem.delete(new UTF8(src));
        } finally {
            long end = System.currentTimeMillis();
            deCounts++;
            deTime += (end - start);
        }
    }

    /**
     */
    public boolean exists(String src) throws IOException {
        long start = System.currentTimeMillis();
        try {
        return namesystem.exists(new UTF8(src));
        } finally {
            long end = System.currentTimeMillis();
            exCounts++;
            exTime += (end - start);
        }
    }

    /**
     */
    public boolean isDir(String src) throws IOException {
        long start = System.currentTimeMillis();
        try {
        return namesystem.isDir(new UTF8(src));
        } finally {
            long end = System.currentTimeMillis();
            idCounts++;
            idTime += (end - start);
        }
    }

    /**
     */
    public boolean mkdirs(String src) throws IOException {
        long start = System.currentTimeMillis();
        try {
        return namesystem.mkdirs(new UTF8(src));
        } finally {
            long end = System.currentTimeMillis();
            mdCounts++;
            mdTime += (end - start);
        }
    }

    /**
     */
    public boolean obtainLock(String src, String clientName, boolean exclusive) throws IOException {
        long start = System.currentTimeMillis();
        try {
        int returnCode = namesystem.obtainLock(new UTF8(src), new UTF8(clientName), exclusive);
        if (returnCode == COMPLETE_SUCCESS) {
            return true;
        } else if (returnCode == STILL_WAITING) {
            return false;
        } else {
            throw new IOException("Failure when trying to obtain lock on " + src);
        }
        } finally {
            long end = System.currentTimeMillis();
            olCounts++;
            olTime += (end - start);
        }
    }

    /**
     */
    public boolean releaseLock(String src, String clientName) throws IOException {
        long start = System.currentTimeMillis();
        try {
            int returnCode = namesystem.releaseLock(new UTF8(src), new UTF8(clientName));
            if (returnCode == COMPLETE_SUCCESS) {
                return true;
            } else if (returnCode == STILL_WAITING) {
                return false;
            } else {
                throw new IOException("Failure when trying to release lock on " + src);
            }
        } finally {
            long end = System.currentTimeMillis();
            rlCounts++;
            rlTime += (end - start);
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
        /**
        System.out.println("opCounts: " + opCounts + ", avgTime: " + (opTime / (1.0 * opCounts)));
        System.out.println("crCounts: " + crCounts + ", avgTime: " + (crTime / (1.0 * crCounts)));
        System.out.println("addbCounts: " + addbCounts + ", avgTime: " + (addbTime / (1.0 * addbCounts)));
        System.out.println("abCounts: " + abCounts + ", avgTime: " + (abTime / (1.0 * abCounts)));
        System.out.println("afCounts: " + afCounts + ", avgTime: " + (afTime / (1.0 * afCounts)));
        System.out.println("coCounts: " + coCounts + ", avgTime: " + (coTime / (1.0 * coCounts)));
        System.out.println("ghCounts: " + ghCounts + ", avgTime: " + (ghTime / (1.0 * ghCounts)));
        System.out.println("rnCounts: " + rnCounts + ", avgTime: " + (rnTime / (1.0 * rnCounts)));
        System.out.println("deCounts: " + deCounts + ", avgTime: " + (deTime / (1.0 * deCounts)));
        System.out.println("exCounts: " + exCounts + ", avgTime: " + (exTime / (1.0 * exCounts)));


        System.out.println("hbCounts: " + hbCounts + ", avgTime: " + (hbTime / (1.0 * hbCounts)));
        System.out.println("brCounts: " + brCounts + ", avgTime: " + (brTime / (1.0 * brCounts)));
        System.out.println("brvCounts: " + brvCounts + ", avgTime: " + (brvTime / (1.0 * brvCounts)));
        System.out.println("bwCounts: " + bwCounts + ", avgTime: " + (bwTime / (1.0 * bwCounts)));
        **/
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
    long opTime = 0, crTime = 0, addbTime = 0, abTime = 0, afTime = 0, coTime = 0, ghTime = 0, rnTime = 0, deTime = 0, exTime = 0, hbTime = 0, brTime = 0, brvTime = 0, bwTime = 0, rlTime = 0, olTime = 0, mdTime = 0, idTime = 0;
    int opCounts = 0, crCounts = 0, addbCounts = 0, abCounts = 0, afCounts = 0, coCounts = 0, ghCounts = 0, rnCounts = 0, deCounts = 0, exCounts = 0, hbCounts = 0, brCounts = 0, brvCounts = 0, bwCounts = 0, rlCounts = 0, olCounts = 0, mdCounts = 0, idCounts = 0;
    /**
     */
    public void sendHeartbeat(String sender, long capacity, long remaining) {
        long start = System.currentTimeMillis();
        namesystem.gotHeartbeat(new UTF8(sender), capacity, remaining);
        long end = System.currentTimeMillis();
        hbCounts++;
        hbTime += (end-start);
    }

    public void blockReport(String sender, Block blocks[]) {
        long start = System.currentTimeMillis();
        namesystem.processReport(blocks, new UTF8(sender));
        long end = System.currentTimeMillis();
        brCounts++;
        brTime += (end-start);
    }

    public void blockReceived(String sender, Block blocks[]) {
        long start = System.currentTimeMillis();
        for (int i = 0; i < blocks.length; i++) {
            namesystem.blockReceived(blocks[i], new UTF8(sender));
        }
        long end = System.currentTimeMillis();
        brvCounts++;
        brvTime += (end-start);
    }

    /**
     */
    public void errorReport(String sender, String msg) {
        // Log error message from datanode
        //LOG.info("Report from " + sender + ": " + msg);
    }

    /**
     * Return a block-oriented command for the datanode to execute
     */
    public BlockCommand getBlockwork(String sender, int xmitsInProgress) {
        //
        // Ask to perform pending transfers, if any
        //
        long start = System.currentTimeMillis();
        try {
            Object xferResults[] = namesystem.pendingTransfers(new DatanodeInfo(new UTF8(sender)), xmitsInProgress);
            if (xferResults != null) {
                return new BlockCommand((Block[]) xferResults[0], (DatanodeInfo[][]) xferResults[1]);
            }

            //
            // If none, check to see if there are blocks to invalidate
            //
            Block blocks[] = namesystem.recentlyInvalidBlocks(new UTF8(sender));
            if (blocks == null) {
                blocks = namesystem.checkObsoleteBlocks(new UTF8(sender));
            }
            if (blocks != null) {
                return new BlockCommand(blocks);
            }

            return new BlockCommand();
        } finally {
            long end = System.currentTimeMillis();
            bwCounts++;
            bwTime += (end-start);
        }
    }

    /**
     */
    public static void main(String argv[]) throws IOException, InterruptedException {
        NameNode namenode = new NameNode();
        namenode.offerService();
    }
}
