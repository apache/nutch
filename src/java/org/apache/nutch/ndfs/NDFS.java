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

/********************************************************
 * The NDFS class holds the NDFS client and server.
 *
 * @author Mike Cafarella
 ********************************************************/
public class NDFS implements FSConstants {
    public static final Logger LOG = LogFormatter.getLogger("org.apache.nutch.fs.NDFS");
    //
    // REMIND - mjc - I might bring "maxgigs" back so user can place 
    // artificial  limit on space
    //private static final long GIGABYTE = 1024 * 1024 * 1024;
    //private static long numGigs = NutchConf.get().getLong("ndfs.datanode.maxgigs", 100);
    //

    //
    // Eventually, this constant should be computed dynamically using 
    // load information
    //
    private static final int MAX_BLOCKS_PER_ROUNDTRIP = 3;

    /**
     * Util method to build socket addr from string
     */
    public static InetSocketAddress createSocketAddr(String s) throws IOException {
        String target = s;
        int colonIndex = target.indexOf(':');
        if (colonIndex < 0) {
          throw new RuntimeException("Not a host:port pair: " + s);
        }
        String host = target.substring(0, colonIndex);
        int port = Integer.parseInt(target.substring(colonIndex + 1));

        return new InetSocketAddress(host, port);
    }

    /**
     * This class isn't for the outside world
     */
    private NDFS() {
    }

    ////////////////////////////////////////////////////////////////
    //
    // NameNode
    //
    ////////////////////////////////////////////////////////////////

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
    public static class NameNode extends org.apache.nutch.ipc.Server {
        FSNamesystem namesystem;

        /**
         * Create a NameNode at the default location
         */
        public NameNode() throws IOException {
          this(new File(NutchConf.get().get("ndfs.name.dir",
                                            "/tmp/nutch/ndfs/name")),
               createSocketAddr
               (NutchConf.get().get("fs.default.name", "local")).getPort());
        }

        /**
         * Create a NameNode at the specified location
         */
        public NameNode(File dir, int port) throws IOException {
            super(port, FSParam.class, 10);
            this.namesystem = new FSNamesystem(dir);
        }

        /**
         * This method implements the call invoked by client.
         * Return a Writable as the function return value.
         */
        public Writable call(Writable param) throws IOException {
            FSParam p = (FSParam) param;
            FSResults r = null;

            switch (p.op) {
            //
            // Client methods
            //
            case OP_CLIENT_OPEN: {
                UTF8 src = (UTF8) p.first;
                Object results[] = namesystem.open(src);
                if (results != null) {
                    Block blocks[] = (Block[]) results[0];
                    DatanodeInfo sets[][] = (DatanodeInfo[][]) results[1];
                    r = new FSResults(OP_CLIENT_OPEN_ACK, new ArrayWritable(Block.class, blocks), new TwoDArrayWritable(DatanodeInfo.class, sets));
                } else {
                    r = new FSResults(OP_FAILURE);
                }
                break;
            }
            case OP_CLIENT_STARTFILE: {
                UTF8 nameParams[] = (UTF8[]) ((ArrayWritable) p.first).toArray();
                UTF8 src = nameParams[0];
                UTF8 clientName = nameParams[1];
                boolean overwrite = ((BooleanWritable) p.second).get();
                Object results[] = namesystem.startFile(src, clientName, overwrite);
                if (results != null) {
                    Block b = (Block) results[0];
                    DatanodeInfo targets[] = (DatanodeInfo[]) results[1];
                    r = new FSResults(OP_CLIENT_STARTFILE_ACK, b, new ArrayWritable(DatanodeInfo.class, targets));
                } else {
                    r = new FSResults(OP_FAILURE);
                }
                break;
            }
            case OP_CLIENT_ADDBLOCK: {
                UTF8 src = (UTF8) p.first;
                Object results[] = namesystem.getAdditionalBlock(src);
                if (results != null && results[0] == null) {
                    try {
                        Thread.sleep(50);
                    } catch (InterruptedException ie) {
                    }
                    results = namesystem.getAdditionalBlock(src);
                }

                if (results != null) {
                    if (results[0] != null) {
                        Block b = (Block) results[0];
                        DatanodeInfo targets[] = (DatanodeInfo[]) results[1];
                        r = new FSResults(OP_CLIENT_ADDBLOCK_ACK, b, new ArrayWritable(DatanodeInfo.class, targets));
                    } else {
                        r = new FSResults(OP_CLIENT_TRYAGAIN);
                    }
                } else {
                    r = new FSResults(OP_FAILURE);
                }
                break;
            }
            case OP_CLIENT_ABANDONBLOCK: {
                Block b = (Block) p.first;
                UTF8 src = (UTF8) p.second;
                boolean success = namesystem.abandonBlock(b, src);
                if (success) {
                    r = new FSResults(OP_CLIENT_ABANDONBLOCK_ACK);
                } else {
                    r = new FSResults(OP_FAILURE);
                }
                break;
            }
            case OP_CLIENT_COMPLETEFILE: {
                UTF8 nameParams[] = (UTF8[]) ((ArrayWritable) p.first).toArray();
                UTF8 src = nameParams[0];
                UTF8 clientName = nameParams[1];
                int returnCode = namesystem.completeFile(src, clientName);
                if (returnCode == COMPLETE_SUCCESS) {
                    r = new FSResults(OP_CLIENT_COMPLETEFILE_ACK);
                } else if (returnCode == STILL_WAITING) {
                    r = new FSResults(OP_CLIENT_TRYAGAIN);
                } else {
                    r = new FSResults(OP_FAILURE);
                }
                break;
            }
            case OP_CLIENT_DATANODE_HINTS: {
                UTF8 src = (UTF8) p.first;
                long offset = ((LongWritable) p.second).get();
                UTF8 hosts[] = namesystem.getDatanodeHints(src, offset);
                if (hosts != null) {
                    r = new FSResults(OP_CLIENT_DATANODE_HINTS_ACK, new ArrayWritable(UTF8.class, hosts));
                } else {
                    r = new FSResults(OP_FAILURE);
                }
                break;
            }
            case OP_CLIENT_RENAMETO: {
                UTF8 src = (UTF8) p.first;
                UTF8 dst = (UTF8) p.second;
                boolean success = namesystem.renameTo(src, dst);
                if (success) {
                    r = new FSResults(OP_CLIENT_RENAMETO_ACK);
                } else {
                    r = new FSResults(OP_FAILURE);
                }
                break;
            }
            case OP_CLIENT_DELETE: {
                UTF8 src = (UTF8) p.first;
                boolean success = namesystem.delete(src);
                if (success) {
                    r = new FSResults(OP_CLIENT_DELETE_ACK);
                } else {
                    r = new FSResults(OP_FAILURE);
                }
                break;
            }
            case OP_CLIENT_EXISTS: {
                UTF8 src = (UTF8) p.first;
                boolean success = namesystem.exists(src);
                if (success) {
                    r = new FSResults(OP_CLIENT_EXISTS_ACK);
                } else {
                    r = new FSResults(OP_FAILURE);
                }
                break;
            }
            case OP_CLIENT_ISDIR: {
                UTF8 src = (UTF8) p.first;
                boolean success = namesystem.isDir(src);
                if (success) {
                    r = new FSResults(OP_CLIENT_ISDIR_ACK);
                } else {
                    r = new FSResults(OP_FAILURE);
                }
                break;
            }
            case OP_CLIENT_MKDIRS: {
                UTF8 src = (UTF8) p.first;
                boolean success = namesystem.mkdirs(src);
                if (success) {
                    r = new FSResults(OP_CLIENT_MKDIRS_ACK);
                } else {
                    r = new FSResults(OP_FAILURE);
                }
                break;
            }
            case OP_CLIENT_OBTAINLOCK: {
                UTF8 nameParams[] = (UTF8[]) ((ArrayWritable) p.first).toArray();
                UTF8 src = nameParams[0];
                UTF8 clientName = nameParams[1];
                boolean exclusive = ((BooleanWritable) p.second).get();
                int returnCode = namesystem.obtainLock(src, clientName, exclusive);
                if (returnCode == COMPLETE_SUCCESS) {
                    r = new FSResults(OP_CLIENT_OBTAINLOCK_ACK);
                } else if (returnCode == STILL_WAITING) {
                    r = new FSResults(OP_CLIENT_TRYAGAIN);
                } else {
                    r = new FSResults(OP_FAILURE);
                }
                break;
            }
            case OP_CLIENT_RELEASELOCK: {
                UTF8 nameParams[] = (UTF8[]) ((ArrayWritable) p.first).toArray();
                UTF8 src = nameParams[0];
                UTF8 clientName = nameParams[1];
                int returnCode = namesystem.releaseLock(src, clientName);
                if (returnCode == COMPLETE_SUCCESS) {
                    r = new FSResults(OP_CLIENT_COMPLETEFILE_ACK);
                } else if (returnCode == STILL_WAITING) {
                    r = new FSResults(OP_CLIENT_TRYAGAIN);
                } else {
                    r = new FSResults(OP_FAILURE);
                }
                break;
            }
            case OP_CLIENT_RENEW_LEASE: {
                UTF8 clientName = (UTF8) p.first;
                namesystem.renewLease(clientName);
                r = new FSResults(OP_CLIENT_RENEW_LEASE_ACK);
                break;
            }
            case OP_CLIENT_LISTING: {
                UTF8 src = (UTF8) p.first;
                NDFSFileInfo results[] = namesystem.getListing(src);
                if (results != null) {
                    r = new FSResults(OP_CLIENT_LISTING_ACK, new ArrayWritable(NDFSFileInfo.class, results));
                } else {
                    r = new FSResults(OP_FAILURE);
                }
                break;
            }
            case OP_CLIENT_RAWSTATS: {
                long totalRaw = namesystem.totalCapacity();
                long remainingRaw = namesystem.totalRemaining();
                LongWritable results[] = new LongWritable[2];
                results[0] = new LongWritable(totalRaw);
                results[1] = new LongWritable(totalRaw - remainingRaw);
                r = new FSResults(OP_CLIENT_RAWSTATS_ACK, new ArrayWritable(LongWritable.class, results));
                break;
            }
            case OP_CLIENT_DATANODEREPORT: {
                DatanodeInfo report[] = namesystem.datanodeReport();
                if (report != null) {
                    r = new FSResults(OP_CLIENT_DATANODEREPORT_ACK, new ArrayWritable(DatanodeInfo.class, report));
                } else {
                    r = new FSResults(OP_FAILURE);
                }
                break;
            }

            //
            // Datanode methods
            //
            case OP_HEARTBEAT: 
            case OP_BLOCKREPORT:
            case OP_BLOCKRECEIVED: 
            case OP_ERROR: {
                UTF8 sender = null;
                if (p.op == OP_HEARTBEAT) {
                    // Receive heartbeat, update last-seen info
                    HeartbeatData hd = (HeartbeatData) p.first;
                    sender = hd.getName();
                    namesystem.gotHeartbeat(sender, hd.getCapacity(), hd.getRemaining());

                } else if (p.op == OP_BLOCKREPORT) {
                    // Receive report on blocks stored at datanode
                    Block blocks[] = (Block[]) ((ArrayWritable) p.first).toArray();
                    sender = (UTF8) p.second;
                    namesystem.processReport(blocks, sender);

                } else if (p.op == OP_BLOCKRECEIVED) {
                    // Receive info on block that's just been received by datanode
                    Writable blocks[] = ((ArrayWritable) p.first).get();
                    sender = (UTF8) p.second;
                    for (int i = 0; i < blocks.length; i++) {
                        namesystem.blockReceived((Block) blocks[i], sender);
                    }
                } else {
                    // Got an error report from datanode
                    System.err.println("ERR from datanode!  Op = " + p.op);
                    sender = (UTF8) p.first;
                    System.err.println("Datanode: " + sender);
                    System.err.println("Message: " + ((UTF8) p.second));
                }

                //
                // Compute return message
                //
                Object xferResults[] = namesystem.pendingTransfers(new DatanodeInfo(sender), MAX_BLOCKS_PER_ROUNDTRIP);
                if (xferResults != null) {
                    r = new FSResults(OP_TRANSFERBLOCKS, new ArrayWritable(Block.class, (Block[]) xferResults[0]), new TwoDArrayWritable(DatanodeInfo.class, (DatanodeInfo[][]) xferResults[1]));
                } else {
                    Block blocks[] = namesystem.recentlyInvalidBlocks(sender);
                    if (blocks == null) {
                        blocks = namesystem.checkObsoleteBlocks(sender);
                    }
                    if (blocks != null) {
                        r = new FSResults(OP_INVALIDATE_BLOCKS, new ArrayWritable(Block.class, blocks));
                    } else {
                        r = new FSResults(OP_ACK);
                    }
                }
                break;
            }
            default:
                throw new RuntimeException("Unknown op code: " + p.op);
            }
            return r;
        }

        /**
         */
        public static void main(String argv[]) throws IOException, InterruptedException {
          NameNode namenode = new NameNode();
          namenode.start();
          namenode.join();
        }
    }

    ////////////////////////////////////////////////////////////////
    //
    // DataNode
    //
    ////////////////////////////////////////////////////////////////
    
    /**********************************************************
     * DataNode controls just one critical table:
     *   block-> BLOCK_SIZE stream of bytes
     *
     * This info is stored on disk (the NameNode is responsible for
     * asking other machines to replicate the data).  The DataNode
     * reports the table's contents to the NameNode upon startup
     * and every so often afterwards.
     *
     * @author Mike Cafarella
     **********************************************************/
    public static class DataNode extends org.apache.nutch.ipc.Client {
        FSDataset data;
        String localName;
        InetSocketAddress nameNodeAddr;

        Vector receivedBlockList = new Vector();

        /**
         * Create using configured defaults.
         */
        public DataNode() throws IOException {
          this(InetAddress.getLocalHost().getHostName(),
               new File(NutchConf.get().get("ndfs.data.dir",
                                            "/tmp/nutch/data/name")),
               createSocketAddr
               (NutchConf.get().get("fs.default.name", "local")));
        }

        /**
         * Needs a directory to find its data (and config info)
         */
        public DataNode(String machineName, File dir, InetSocketAddress nameNodeAddr) throws IOException {
            super(FSResults.class);
            this.data = new FSDataset(dir);
            this.nameNodeAddr = nameNodeAddr;

            ServerSocket ss = null;
            int tmpPort = 7000;
            while (ss == null) {
                try {
                    ss = new ServerSocket(tmpPort);
                    LOG.info("Opened server at " + tmpPort);
                } catch (IOException ie) {
                    LOG.info("Could not open server at " + tmpPort + ", trying new port");
                    tmpPort++;
                }
            }
            this.localName = machineName + ":" + tmpPort;
            new Daemon(new DataXceiveServer(ss)).start();
        }

        /**
         * Main loop for the DataNode.  Runs until shutdown.
         */
        public void offerService() throws Exception {
            long wakeups = 0;
            long lastHeartbeat = 0, lastBlockReport = 0;
            long sendStart = System.currentTimeMillis();
            int heartbeatsSent = 0;

            //
            // Now loop for a long time....
            //
            boolean shouldRun = true;
            while (shouldRun) {
                long now = System.currentTimeMillis();

                //
                // Every so often, send heartbeat or block-report
                //
                FSParam p = null;
                FSResults r = null;

                synchronized (receivedBlockList) {
                    if (now - lastHeartbeat > HEARTBEAT_INTERVAL) {
                        //
                        // All heartbeat messages include following info:
                        // -- Datanode name
                        // -- data transfer port
                        // -- Total capacity
                        // -- Bytes remaining
                        //
                        p = new FSParam(OP_HEARTBEAT, new HeartbeatData(localName, data.getCapacity(), data.getRemaining()));
                        lastHeartbeat = now;
                    } else if (now - lastBlockReport > BLOCKREPORT_INTERVAL) {
                        //
                        // Send latest blockinfo report if timer has expired
                        //
                        p = new FSParam(OP_BLOCKREPORT, new ArrayWritable(Block.class, data.getBlockReport()), new UTF8(localName));
                        lastBlockReport = now;
                    } else if (receivedBlockList.size() > 0) {
                        //
                        // Send newly-received blockids to namenode
                        //
                        Block blockArray[] = null;
                        blockArray = (Block[]) receivedBlockList.toArray(new Block[receivedBlockList.size()]);
                        receivedBlockList.removeAllElements();
                        p = new FSParam(OP_BLOCKRECEIVED, new ArrayWritable(Block.class, blockArray), new UTF8(localName));
                    } else {
                        //
                        // Nothing to do;  sleep (until time elapses, or work arrives)
                        // and continue work
                        //
                        long waitTime = HEARTBEAT_INTERVAL - (now - lastHeartbeat);
                        if (waitTime > 0) {
                            try {
                                receivedBlockList.wait(waitTime);
                            } catch (InterruptedException ie) {
                            }
                        }
                        continue;
                    }
                }

                //
                // Invoke remote call
                //
                r = (FSResults) call(p, nameNodeAddr);
                if (r == null) {
                    throw new IOException("No response to remote call to " + nameNodeAddr);
                }

                //
                // Respond to namenode's reply
                //
                // REMIND - mjc - Right now the Datanode can only get
                // requests from the Namenode via a response to HEARTBEAT
                // or BLOCKREPORT.  That's not so hot, as it means the
                // Namenode will have to wait for HEARTBEAT_INTERVAL before
                // it can make a request on the Datanode.  
                //
                // The advantage of this system is that it's very simple.
                // We'll fix it later on.
                //
                switch (r.op) {
                case OP_ACK: {
                    //
                    // The nameserver just acked our call, and didn't
                    // want anything else.  
                    //
                    break;
                }
                case OP_TRANSFERBLOCKS: {
                    //
                    // Send a copy of the indicated block to another
                    // datanode
                    //
                    Block blocks[] = (Block[]) ((ArrayWritable) r.first).toArray();
                    DatanodeInfo xferTargets[][] = (DatanodeInfo[][]) ((TwoDArrayWritable) r.second).toArray();
                    for (int i = 0; i < blocks.length; i++) {
                        if (!data.isValidBlock(blocks[i])) {
                            System.out.println("Invoking error!  " + localName);
                            call(new FSParam(OP_ERROR, new UTF8(localName), new UTF8("Can't send invalid block " + blocks[i])), nameNodeAddr);
                            break;
                        } else {
                            if (xferTargets[i].length > 0) {
                                LOG.info("Starting thread to transfer block " + blocks[i] + " to " + xferTargets[i]);
                                new Daemon(new DataTransfer(xferTargets[i], blocks[i])).start();
                            }
                        }
                    }
                    break;
                }
                case OP_INVALIDATE_BLOCKS: {
                    //
                    // Some local block(s) are obsolete and can be 
                    // safely garbage-collected.
                    //
                    ArrayWritable aw = (ArrayWritable) r.first;
                    Block b[] = (Block[]) aw.toArray();
                    data.invalidate(b);
                    break;
                }
                default:
                    throw new RuntimeException("Unknown op code: " + r.op);
                }
            }
        }

        /**
         * Server used for receiving/sending a block of data
         */
        class DataXceiveServer implements Runnable {
            ServerSocket ss;
            public DataXceiveServer(ServerSocket ss) {
                this.ss = ss;
            }

            /**
             */
            public void run() {
                try {
                    while (true) {
                        Socket s = ss.accept();
                        new Daemon(new DataXceiver(s)).start();
                    }
                } catch (IOException ie) {
                    LOG.info("Exiting DataXceiveServer due to " + ie.toString());
                }
            }
        }

        /**
         * Thread for processing incoming/outgoing data stream
         */
        class DataXceiver implements Runnable {
            Socket s;
            public DataXceiver(Socket s) {
                this.s = s;
            }

            /**
             */
            public void run() {
                try {
                    DataInputStream in = new DataInputStream(new BufferedInputStream(s.getInputStream()));
                    try {
                        byte op = (byte) in.read();
                        if (op == OP_WRITE_BLOCK) {
                            //
                            // Read in the header
                            //
                            DataOutputStream reply = new DataOutputStream(new BufferedOutputStream(s.getOutputStream()));
                            try {
                                Block b = new Block();
                                b.readFields(in);
                                int numTargets = in.readInt();
                                if (numTargets <= 0) {
                                    throw new IOException("Mislabelled incoming datastream.");
                                }
                                DatanodeInfo targets[] = new DatanodeInfo[numTargets];
                                for (int i = 0; i < targets.length; i++) {
                                    DatanodeInfo tmp = new DatanodeInfo();
                                    tmp.readFields(in);
                                    targets[i] = tmp;
                                }
                                byte encodingType = (byte) in.read();
                                long len = in.readLong();

                                //
                                // Make sure curTarget is equal to this machine
                                // REMIND - mjc
                                //
                                DatanodeInfo curTarget = targets[0];

                                //
                                // Open local disk out
                                //
                                DataOutputStream out = new DataOutputStream(new BufferedOutputStream(data.writeToBlock(b)));
                                InetSocketAddress mirrorTarget = null;
                                try {
                                    //
                                    // Open network conn to backup machine, if 
                                    // appropriate
                                    //
                                    DataInputStream in2 = null;
                                    DataOutputStream out2 = null;
                                    if (targets.length > 1) {
                                        // Connect to backup machine
                                        mirrorTarget = createSocketAddr(targets[1].getName().toString());
                                        try {
                                            Socket s = new Socket(mirrorTarget.getAddress(), mirrorTarget.getPort());
                                            out2 = new DataOutputStream(new BufferedOutputStream(s.getOutputStream()));
                                            in2 = new DataInputStream(new BufferedInputStream(s.getInputStream()));

                                            // Write connection header
                                            out2.write(OP_WRITE_BLOCK);
                                            b.write(out2);
                                            out2.writeInt(targets.length - 1);
                                            for (int i = 1; i < targets.length; i++) {
                                                targets[i].write(out2);
                                            }
                                            out2.write(encodingType);
                                            out2.writeLong(len);
                                        } catch (IOException ie) {
                                            if (out2 != null) {
                                                try {
                                                    out2.close();
                                                    in2.close();
                                                } catch (IOException out2close) {
                                                } finally {
                                                    out2 = null;
                                                    in2 = null;
                                                }
                                            }
                                        }
                                    }

                                    //
                                    // Process incoming data, copy to disk and
                                    // maybe to network.
                                    //
                                    try {
                                        boolean anotherChunk = true;
                                        byte buf[] = new byte[2048];

                                        while (anotherChunk) {
                                            while (len > 0) {
                                                int bytesRead = in.read(buf, 0, Math.min(buf.length, (int) len));
                                                if (bytesRead >= 0) {
                                                    out.write(buf, 0, bytesRead);
                                                    if (out2 != null) {
                                                        try {
                                                            out2.write(buf, 0, bytesRead);
                                                        } catch (IOException out2e) {
                                                            //
                                                            // If stream-copy fails, continue 
                                                            // writing to disk.  We shouldn't 
                                                            // interrupt client write.
                                                            //
                                                            try {
                                                                out2.close();
                                                                in2.close();
                                                            } catch (IOException out2close) {
                                                            } finally {
                                                                out2 = null;
                                                                in2 = null;
                                                            }
                                                        }
                                                    }
                                                }
                                                len -= bytesRead;
                                            }

                                            if (encodingType == RUNLENGTH_ENCODING) {
                                                anotherChunk = false;
                                            } else if (encodingType == CHUNKED_ENCODING) {
                                                len = in.readLong();
                                                if (out2 != null) {
                                                    out2.writeLong(len);
                                                }
                                                if (len == 0) {
                                                    anotherChunk = false;
                                                }
                                            }
                                        }

                                        if (out2 == null) {
                                            LOG.info("Received block " + b + " from " + s.getInetAddress());
                                        } else {
                                            out2.flush();
                                            long complete = in2.readLong();
                                            if (complete != WRITE_COMPLETE) {
                                                LOG.info("Conflicting value for WRITE_COMPLETE: " + complete);
                                            }
                                            LOG.info("Received block " + b + " from " + s.getInetAddress() + " and mirrored to " + mirrorTarget);
                                        }
                                    } finally {
                                        if (out2 != null) {
                                            out2.close();
                                            in2.close();
                                        }
                                    }
                                } finally {
                                    out.close();
                                }
                                data.finalizeBlock(b);

                                // 
                                // Tell the namenode that we've received this block 
                                // in full.
                                //
                                synchronized (receivedBlockList) {
                                    receivedBlockList.add(b);
                                    receivedBlockList.notifyAll();
                                }

                                //
                                // Tell client job is done
                                //
                                reply.writeLong(WRITE_COMPLETE);
                            } finally {
                                reply.close();
                            }
                        } else if (op == OP_READ_BLOCK || op == OP_READSKIP_BLOCK) {
                            //
                            // Read in the header
                            //
                            Block b = new Block();
                            b.readFields(in);

                            long toSkip = 0;
                            if (op == OP_READSKIP_BLOCK) {
                                toSkip = in.readLong();
                            }

                            //
                            // Open reply stream
                            //
                            DataOutputStream out = new DataOutputStream(new BufferedOutputStream(s.getOutputStream()));
                            try {
                                //
                                // Write filelen of -1 if error
                                //
                                if (! data.isValidBlock(b)) {
                                    out.writeLong(-1);
                                } else {
                                    //
                                    // Get blockdata from disk
                                    //
                                    long len = data.getLength(b);
                                    DataInputStream in2 = new DataInputStream(data.getBlockData(b));
                                    out.writeLong(len);

                                    if (op == OP_READSKIP_BLOCK) {
                                        if (toSkip > len) {
                                            toSkip = len;
                                        }
                                        long amtSkipped = in2.skip(toSkip);
                                        out.writeLong(amtSkipped);
                                    }

                                    byte buf[] = new byte[4096];
                                    try {
                                        int bytesRead = in2.read(buf);
                                        while (bytesRead >= 0) {
                                            out.write(buf, 0, bytesRead);
                                            len -= bytesRead;
                                            bytesRead = in2.read(buf);
                                        }
                                    } catch (SocketException se) {
                                        // This might be because the reader
                                        // closed the stream early
                                    } finally {
                                        in2.close();
                                    }
                                }
                                LOG.info("Served block " + b + " to " + s.getInetAddress());
                            } finally {
                                out.close();
                            }
                        } else {
                            while (op >= 0) {
                                System.out.println("Faulty op: " + op);
                                op = (byte) in.read();
                            }
                            throw new IOException("Unknown opcode for incoming data stream");
                        }
                    } finally {
                        in.close();
                    }
                } catch (IOException ie) {
                    ie.printStackTrace();
                } finally {
                    try {
                        s.close();
                    } catch (IOException ie2) {
                    }
                }
            }
        }

        /**
         * Used for transferring a block of data
         */
        class DataTransfer implements Runnable {
            InetSocketAddress curTarget;
            DatanodeInfo targets[];
            Block b;
            byte buf[];

            /**
             * Connect to the first item in the target list.  Pass along the 
             * entire target list, the block, and the data.
             */
            public DataTransfer(DatanodeInfo targets[], Block b) throws IOException {
                this.curTarget = createSocketAddr(targets[0].getName().toString());
                this.targets = targets;
                this.b = b;
                this.buf = new byte[2048];
            }

            /**
             * Do the deed, write the bytes
             */
            public void run() {
                try {
                    Socket s = new Socket(curTarget.getAddress(), curTarget.getPort());
                    DataOutputStream out = new DataOutputStream(new BufferedOutputStream(s.getOutputStream()));
                    try {
                        long filelen = data.getLength(b);
                        DataInputStream in = new DataInputStream(new BufferedInputStream(data.getBlockData(b)));
                        try {
                            //
                            // Header info
                            //
                            out.write(OP_WRITE_BLOCK);
                            b.write(out);
                            out.writeInt(targets.length);
                            for (int i = 0; i < targets.length; i++) {
                                targets[i].write(out);
                            }
                            out.write(RUNLENGTH_ENCODING);
                            out.writeLong(filelen);

                            //
                            // Write the data
                            //
                            while (filelen > 0) {
                                int bytesRead = in.read(buf, 0, (int) Math.min(filelen, buf.length));
                                out.write(buf, 0, bytesRead);
                                filelen -= bytesRead;
                            }
                        } finally {
                            in.close();
                        }
                    } finally {
                        out.close();
                    }
                    LOG.info("Replicated block " + b + " to " + curTarget);
                } catch (IOException ie) {
                }
            }
        }

        /**
         */
        public static void main(String argv[]) throws IOException {
            DataNode datanode = new DataNode();
            while (true) {
                try {
                    datanode.offerService();
                } catch (Exception ex) {
                    LOG.info("Lost connection to namenode [" + datanode.nameNodeAddr + "].  Retrying...");
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException ie) {
                    }
                }
            }
        }
    }
}
