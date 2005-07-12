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
import org.apache.nutch.fs.*;
import org.apache.nutch.ipc.*;
import org.apache.nutch.util.*;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.logging.*;

/********************************************************
 * NDFSClient does what's necessary to connect to a Nutch Filesystem
 * and perform basic file tasks.
 *
 * @author Mike Cafarella, Tessa MacDuff
 ********************************************************/
public class NDFSClient implements FSConstants {
    public static final Logger LOG = LogFormatter.getLogger("org.apache.nutch.fs.NDFSClient");
    static int BUFFER_SIZE = 4096;
    ClientProtocol namenode;
    boolean running = true;
    Random r = new Random();
    String clientName;
    Daemon leaseChecker;


    /**
     */
    public NDFSClient(InetSocketAddress nameNodeAddr) {
        this.namenode = (ClientProtocol) RPC.getProxy(ClientProtocol.class, nameNodeAddr);
        this.clientName = "NDFSClient_" + r.nextInt();
        this.leaseChecker = new Daemon(new LeaseChecker());
        this.leaseChecker.start();
    }

    /**
     */
    public void close() throws IOException {
        this.running = false;
        try {
            leaseChecker.join();
        } catch (InterruptedException ie) {
        }
    }

    /**
     * Create an input stream that obtains a nodelist from the
     * namenode, and then reads from all the right places.  Creates
     * inner subclass of InputStream that does the right out-of-band
     * work.
     */
    public NFSInputStream open(UTF8 src) throws IOException {
        // Get block info from namenode
        LocatedBlock results[] = namenode.open(src.toString());

        Vector blocks = new Vector();
        Vector locs = new Vector();
        for (int i = 0; i < results.length; i++) {
            blocks.add(results[i].getBlock());
            locs.add(results[i].getLocations());
        }
        return new NDFSInputStream((Block[]) blocks.toArray(new Block[blocks.size()]), (DatanodeInfo[][]) locs.toArray(new DatanodeInfo[locs.size()][]));
    }

    /**
     * Create an output stream that writes to all the right places.
     * Basically creates instance of inner subclass of OutputStream
     * that handles datanode/namenode negotiation.
     */
    public NFSOutputStream create(UTF8 src) throws IOException {
        return create(src, false);
    }
    public NFSOutputStream create(UTF8 src, boolean overwrite) throws IOException {
        return new NDFSOutputStream(src, overwrite);
    }

    /**
     * Make a direct connection to namenode and manipulate structures
     * there.
     */
    public boolean rename(UTF8 src, UTF8 dst) throws IOException {
        return namenode.rename(src.toString(), dst.toString());
    }

    /**
     * Make a direct connection to namenode and manipulate structures
     * there.
     */
    public boolean delete(UTF8 src) throws IOException {
        return namenode.delete(src.toString());
    }

    /**
     */
    public boolean exists(UTF8 src) throws IOException {
        return namenode.exists(src.toString());
    }

    /**
     */
    public boolean isDirectory(UTF8 src) throws IOException {
        return namenode.isDir(src.toString());
    }

    /**
     */
    public NDFSFileInfo[] listFiles(UTF8 src) throws IOException {
        return namenode.getListing(src.toString());
    }

    /**
     */
    public long totalRawCapacity() throws IOException {
        long rawNums[] = namenode.getStats();
        return rawNums[0];
    }

    /**
     */
    public long totalRawUsed() throws IOException {
        long rawNums[] = namenode.getStats();
        return rawNums[1];
    }

    public DatanodeInfo[] datanodeReport() throws IOException {
        return namenode.getDatanodeReport();
    }

    /**
     */
    public boolean mkdirs(UTF8 src) throws IOException {
        return namenode.mkdirs(src.toString());
    }

    /**
     */
    public void lock(UTF8 src, boolean exclusive) throws IOException {
        long start = System.currentTimeMillis();
        boolean hasLock = false;
        while (! hasLock) {
            hasLock = namenode.obtainLock(src.toString(), clientName, exclusive);
            if (! hasLock) {
                try {
                    Thread.sleep(400);
                    if (System.currentTimeMillis() - start > 5000) {
                        LOG.info("Waiting to retry lock for " + (System.currentTimeMillis() - start) + " ms.");
                        Thread.sleep(2000);
                    }
                } catch (InterruptedException ie) {
                }
            }
        }
    }

    /**
     */
    public void release(UTF8 src) throws IOException {
        boolean hasReleased = false;
        while (! hasReleased) {
            hasReleased = namenode.releaseLock(src.toString(), clientName);
            if (! hasReleased) {
                LOG.info("Could not release.  Retrying...");
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ie) {
                }
            }
        }
    }

    /**
     * Pick the best/closest node  which to stream the data.
     * For now, just pick the first on the list.
     */
    private DatanodeInfo bestNode(DatanodeInfo nodes[], TreeSet deadNodes) throws IOException {
        if ((nodes == null) || 
            (nodes.length - deadNodes.size() < 1)) {
            throw new IOException("No live nodes contain current block");
        }
        DatanodeInfo chosenNode = null;
        do {
            chosenNode = nodes[Math.abs(r.nextInt()) % nodes.length];
        } while (deadNodes.contains(chosenNode));
        return chosenNode;
    }

    /***************************************************************
     * If any leases are outstanding, periodically check in with the 
     * namenode and renew all the leases.
     ***************************************************************/
    class LeaseChecker implements Runnable {
        /**
         */
        public void run() {
            long lastRenewed = 0;
            while (running) {
                if (System.currentTimeMillis() - lastRenewed > (LEASE_PERIOD / 2)) {
                    try {
                        namenode.renewLease(clientName);
                        lastRenewed = System.currentTimeMillis();
                    } catch (IOException ie) {
                    }
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                }
            }
        }
    }

    /****************************************************************
     * NDFSInputStream provides bytes from a named file.  It handles 
     * negotiation of the namenode and various datanodes as necessary.
     ****************************************************************/
    class NDFSInputStream extends NFSInputStream {
        boolean closed = false;

        private DataInputStream blockStream;
        private DataOutputStream partnerStream;
        private Block blocks[];
        private int curBlock = 0;
        private DatanodeInfo nodes[][];
        private long pos = 0;
        private long bytesRemainingInBlock = 0, curBlockSize = 0;

        private int memoryBuf[] = new int[32 * 1024];
        private long memoryStartPos = 0;
        private long openPoint = 0;
        private int memoryBytes = 0;
        private int memoryBytesStart = 0;

        /**
         */
        public NDFSInputStream(Block blocks[], DatanodeInfo nodes[][]) throws IOException {
            this.blocks = blocks;
            this.nodes = nodes;
            this.blockStream = null;
            this.partnerStream = null;
        }

        /**
         * Open a DataInputStream to a DataNode so that it can be written to.
         * This happens when a file is created and each time a new block is allocated.
         * Must get block ID and the IDs of the destinations from the namenode.
         */
        private synchronized void nextBlockInputStream() throws IOException {
            nextBlockInputStream(0);
        }
        private synchronized void nextBlockInputStream(long preSkip) throws IOException {
            if (curBlock >= blocks.length) {
                throw new IOException("Attempted to read past end of file");
            }
            if (bytesRemainingInBlock > 0) {
                throw new IOException("Trying to skip to next block without reading all data");
            }

            if (blockStream != null) {
                blockStream.close();
                partnerStream.close();
            }

            //
            // Connect to best DataNode for current Block
            //
            InetSocketAddress target = null;
            Socket s = null;
            TreeSet deadNodes = new TreeSet();
            while (s == null) {
                DatanodeInfo chosenNode;

                try {
                    chosenNode = bestNode(nodes[curBlock], deadNodes);
                    target = DataNode.createSocketAddr(chosenNode.getName().toString());
                } catch (IOException ie) {
                    LOG.info("Could not obtain block from any node.  Retrying...");
                    try {
                        Thread.sleep(10000);
                    } catch (InterruptedException iex) {
                    }
                    deadNodes.clear();
                    continue;
                }
                try {
                    s = new Socket(target.getAddress(), target.getPort());
                    //LOG.info("Now downloading from " + target + ", block " + blocks[curBlock] + ", skipahead " + preSkip);

                    //
                    // Xmit header info to datanode
                    //
                    DataOutputStream out = new DataOutputStream(new BufferedOutputStream(s.getOutputStream()));
                    out.write(OP_READSKIP_BLOCK);
                    blocks[curBlock].write(out);
                    out.writeLong(preSkip);
                    out.flush();

                    //
                    // Get bytes in block, set streams
                    //
                    DataInputStream in = new DataInputStream(new BufferedInputStream(s.getInputStream()));
                    curBlockSize = in.readLong();
                    long amtSkipped = in.readLong();

                    pos += amtSkipped;
                    bytesRemainingInBlock = curBlockSize - amtSkipped;

                    if (amtSkipped > 0) {
                        memoryStartPos = pos;
                        memoryBytes = 0;
                        memoryBytesStart = 0;
                    }
                    blockStream = in;
                    partnerStream = out;
                    curBlock++;
                    openPoint = pos;
                } catch (IOException ex) {
                    // Put chosen node into dead list, continue
                    LOG.info("Could not connect to " + target);
                    deadNodes.add(chosenNode);
                    s = null;
                }
            }
        }

        /**
         */
        public synchronized void seek(long pos) throws IOException {
            if (pos < 0) {
                throw new IOException("Cannot seek to negative position " + pos);
            }
            if (pos == this.pos) {
                return;
            }

            //
            // If we have remembered enough bytes to seek backwards to the
            // desired pos, we can do so easily
            //
            if ((pos >= memoryStartPos) && (memoryStartPos + memoryBytes > pos)) {
                this.pos = pos;
            } else {
                //
                // If we are seeking backwards (and *don't* have enough memory bytes)
                // we need to reset the NDFS streams.  They will be reopened upon the
                // next call to nextBlockInputStream().  After this operation, all
                // seeks will be "forwardSeeks".
                //
                if (pos < memoryStartPos && blockStream != null) {
                    blockStream.close();
                    blockStream = null;
                    partnerStream.close();
                    partnerStream = null;
                    this.curBlock = 0;
                    this.bytesRemainingInBlock = 0;
                    this.pos = 0;
                    this.memoryStartPos = 0;
                    this.memoryBytes = 0;
                    this.memoryBytesStart = 0;
                    //
                    // REMIND - this could be made more efficient, to just
                    // skip back block-by-block
                    //
                }

                //
                // Now read ahead to the desired position.
                //
                long diff = pos - this.pos;
                while (diff > 0) {
                    long skipped = skip(diff);
                    if (skipped > 0) {
                        diff -= skipped;
                    }
                }
                // Pos will be incremented by skip()
            }
        }

        /**
         * Skip ahead some number of bytes
         */
        public synchronized long skip(long skip) throws IOException {
            long toSkip = 0;
            long toFastSkip = 0;
            if (skip > memoryBuf.length) {
                toSkip = memoryBuf.length;
                toFastSkip = skip - toSkip;
            } else {
                toSkip = skip;
            }
            long totalSkipped = 0;

            //
            // If there's a lot of fast-skipping to do within the current block,
            // close it and reopen, so we can fast-skip to the target
            //
            /**
            while (toFastSkip > 0) {
                long amtSkipped = super.skip(toFastSkip);
                toFastSkip -= amtSkipped;
                totalSkipped += amtSkipped;
            }
            **/
            long realBytesRemaining = bytesRemainingInBlock + (memoryBytes - (pos - memoryStartPos));
            if (toFastSkip > 0 && realBytesRemaining > 0 && 
                toFastSkip < realBytesRemaining) {

                blockStream.close();
                blockStream = null;
                partnerStream.close();
                partnerStream = null;

                long backwardsDistance = curBlockSize - realBytesRemaining;
                pos -= backwardsDistance;
                totalSkipped -= backwardsDistance;
                toFastSkip += backwardsDistance;
                bytesRemainingInBlock = 0;
                curBlock--;

                memoryStartPos = pos;
                memoryBytes = 0;
                memoryBytesStart = 0;
            }

            //
            // If there's any fast-skipping to do, we do it by opening a
            // new block and telling the datanode how many bytes to skip.
            //
            while (toFastSkip > 0 && curBlock < blocks.length) {

                if (bytesRemainingInBlock > 0) {
                    blockStream.close();
                    blockStream = null;
                    partnerStream.close();
                    partnerStream = null;

                    pos += bytesRemainingInBlock;
                    totalSkipped += bytesRemainingInBlock;
                    toFastSkip -= bytesRemainingInBlock;
                    bytesRemainingInBlock = 0;
                }

                long oldPos = pos;
                nextBlockInputStream(toFastSkip);
                long forwardDistance = (pos - oldPos);
                totalSkipped += forwardDistance;
                toFastSkip -= (pos - oldPos);

                memoryStartPos = pos;
                memoryBytes = 0;
                memoryBytesStart = 0;
            }

            //
            // If there's any remaining toFastSkip, well, there's
            // not much we can do about it.  We're at the end of
            // the stream!
            //
            if (toFastSkip > 0) {
                System.err.println("Trying to skip past end of file....");
                toFastSkip = 0;
            }

            //
            // Do a slow skip as we approach, so we can fill the client
            // history buffer
            //
            totalSkipped += super.skip(toSkip);
            toSkip = 0;
            return totalSkipped;
        }

        /**
         */
        public synchronized long getPos() throws IOException {
            return pos;
        }

        /**
         */
        public synchronized int available() throws IOException {
            if (closed) {
                throw new IOException("Stream closed");
            }
            return (int) Math.min((long) Integer.MAX_VALUE, bytesRemainingInBlock);
        }

        /**
         */
        public synchronized void close() throws IOException {
            if (closed) {
                throw new IOException("Stream closed");
            }

            if (blockStream != null) {
                blockStream.close();
                blockStream = null;
                partnerStream.close();
            }
            super.close();
            closed = true;
        }

        /**
         * Other read() functions are implemented in terms of
         * this one.
         */
        public synchronized int read() throws IOException {
            if (closed) {
                throw new IOException("Stream closed");
            }

            int b = 0;
            if (pos - memoryStartPos < memoryBytes) {
                //
                // Move the memoryStartPos up to current pos, if necessary.
                //
                int diff = (int) (pos - memoryStartPos);

                //
                // Fetch the byte
                //
                b = memoryBuf[(memoryBytesStart + diff) % memoryBuf.length];

                //
                // Bump the pos
                //
                pos++;
            } else {
                if (bytesRemainingInBlock == 0) {
                    if (curBlock < blocks.length) {
                        nextBlockInputStream();
                    } else {
                        return -1;
                    }
                }
                b = blockStream.read();
                if (b >= 0) {
                    //
                    // Remember byte so we can seek backwards at some later time
                    //
                    if (memoryBytes == memoryBuf.length) {
                        memoryStartPos++;
                    }

                    if (memoryBuf.length > 0) {
                        int target;
                        if (memoryBytes == memoryBuf.length) {
                            target = memoryBytesStart;
                            memoryBytesStart = (memoryBytesStart + 1) % memoryBuf.length;
                        } else {
                            target = (memoryBytesStart + memoryBytes) % memoryBuf.length;
                            memoryBytes++;
                        }
                        memoryBuf[target] = b;
                    }
                    bytesRemainingInBlock--;
                    pos++;
                }
            }
            return b;
        }

        /**
         * We definitely don't support marks
         */
        public boolean markSupported() {
            return false;
        }
        public void mark(int readLimit) {
        }
        public void reset() throws IOException {
            throw new IOException("Mark not supported");
        }
    }

    /****************************************************************
     * NDFSOutputStream creates files from a stream of bytes.
     ****************************************************************/
    class NDFSOutputStream extends NFSOutputStream {
        boolean closed = false;

        private byte outBuf[] = new byte[BUFFER_SIZE];
        private int pos = 0;

        private UTF8 src;
        private boolean overwrite;
        private boolean blockStreamWorking;
        private DataOutputStream blockStream;
        private DataInputStream blockReplyStream;
        private File backupFile;
        private OutputStream backupStream;
        private Block block;
        private DatanodeInfo targets[]; 
        private long filePos = 0;
        private int bytesWrittenToBlock = 0;

        /**
         * Create a new output stream to the given DataNode.
         */
        public NDFSOutputStream(UTF8 src, boolean overwrite) throws IOException {
            this.src = src;
            this.overwrite = overwrite;
            this.blockStream = null;
            this.blockReplyStream = null;
            this.blockStreamWorking = false;
            this.backupFile = File.createTempFile("ndfsout", "bak");
            this.backupStream = new BufferedOutputStream(new FileOutputStream(backupFile));
            nextBlockOutputStream(true);
        }

        /**
         * Open a DataOutputStream to a DataNode so that it can be written to.
         * This happens when a file is created and each time a new block is allocated.
         * Must get block ID and the IDs of the destinations from the namenode.
         */
        private synchronized void nextBlockOutputStream(boolean firstTime) throws IOException {
            if (! firstTime && blockStreamWorking) {
                blockStream.close();
                blockReplyStream.close();
                blockStreamWorking = false;
            }

            boolean retry = false;
            long start = System.currentTimeMillis();
            do {
                retry = false;
                
                long localstart = System.currentTimeMillis();
                boolean blockComplete = false;
                LocatedBlock lb = null;                
                while (! blockComplete) {
                    if (firstTime) {
                        lb = namenode.create(src.toString(), clientName.toString(), overwrite);
                    } else {
                        lb = namenode.addBlock(src.toString());
                    }

                    if (lb == null) {
                        try {
                            Thread.sleep(400);
                            if (System.currentTimeMillis() - localstart > 5000) {
                                LOG.info("Waiting to find new output block node for " + (System.currentTimeMillis() - start) + "ms");
                            }
                        } catch (InterruptedException ie) {
                        }

                    } else {
                        blockComplete = true;
                    }
                }

                Block block = lb.getBlock();
                DatanodeInfo nodes[] = lb.getLocations();

                //
                // Connect to first DataNode in the list.  Abort if this fails.
                //
                InetSocketAddress target = DataNode.createSocketAddr(nodes[0].getName().toString());
                Socket s = null;
                try {
                    //System.err.println("Trying to connect to " + target);
                    s = new Socket(target.getAddress(), target.getPort());
                } catch (IOException ie) {
                    // Connection failed.  Let's wait a little bit and retry
                    try {
                        if (System.currentTimeMillis() - start > 5000) {
                            LOG.info("Waiting to find target node");
                        }
                        Thread.sleep(6000);
                    } catch (InterruptedException iex) {
                    }
                    namenode.abandonBlock(block, src.toString());
                    retry = true;
                    continue;
                }

                //
                // Xmit header info to datanode
                //
                DataOutputStream out = new DataOutputStream(new BufferedOutputStream(s.getOutputStream()));
                out.write(OP_WRITE_BLOCK);
                block.write(out);
                out.writeInt(nodes.length);
                for (int i = 0; i < nodes.length; i++) {
                    nodes[i].write(out);
                }
                out.write(CHUNKED_ENCODING);
                bytesWrittenToBlock = 0;
                blockStream = out;
                blockReplyStream = new DataInputStream(new BufferedInputStream(s.getInputStream()));
                blockStreamWorking = true;
            } while (retry);
        }

        /**
         * We're referring to the file pos here
         */
        public synchronized long getPos() throws IOException {
            return filePos;
        }
			
        /**
         * Writes the specified byte to this output stream.
         * This is the only write method that needs to be implemented.
         */
        public synchronized void write(int b) throws IOException {
            if (closed) {
                throw new IOException("Stream closed");
            }

            if ((bytesWrittenToBlock + pos == BLOCK_SIZE) ||
                (pos >= BUFFER_SIZE)) {
                flush();
            }
            outBuf[pos++] = (byte) b;
            filePos++;
        }

        /**
         * Flush the buffer, getting a stream to a new block if necessary.
         */
        public synchronized void flush() throws IOException {
            if (closed) {
                throw new IOException("Stream closed");
            }

            if (bytesWrittenToBlock + pos >= BLOCK_SIZE) {
                flushData(BLOCK_SIZE - bytesWrittenToBlock);
            }
            if (bytesWrittenToBlock == BLOCK_SIZE) {
                endBlock();
                nextBlockOutputStream(false);
            }
            flushData(pos);
        }

        /**
         * Actually flush the accumulated bytes to the remote node,
         * but no more bytes than the indicated number.
         */
        private synchronized void flushData(int maxPos) throws IOException {
            int workingPos = Math.min(pos, maxPos);
            
            if (workingPos >= 0) {
                //
                // To the blockStream, write length, then bytes
                //
                if (blockStreamWorking) {
                    try {
                        blockStream.writeLong(workingPos);
                        blockStream.write(outBuf, 0, workingPos);
                    } catch (IOException ie) {
                        try {
                            blockStream.close();
                        } catch (IOException ie2) {
                        }
                        try {
                            blockReplyStream.close();
                        } catch (IOException ie2) {
                        }
                        namenode.abandonBlock(block, src.toString());
                        blockStreamWorking = false;
                    }
                }
                //
                // To the local block backup, write just the bytes
                //
                backupStream.write(outBuf, 0, workingPos);

                //
                // Track position
                //
                bytesWrittenToBlock += workingPos;
                System.arraycopy(outBuf, workingPos, outBuf, 0, pos - workingPos);
                pos -= workingPos;
            }
        }

        /**
         */
        private synchronized void endBlock() throws IOException {
            boolean mustRecover = ! blockStreamWorking;

            //
            // A zero-length set of data indicates the end of the block
            //
            if (blockStreamWorking) {
                try {
                    blockStream.writeLong(0);
                    blockStream.flush();

                    long complete = blockReplyStream.readLong();
                    if (complete != WRITE_COMPLETE) {
                        LOG.info("Did not receive WRITE_COMPLETE flag: " + complete);
                        throw new IOException("Did not receive WRITE_COMPLETE_FLAG: " + complete);
                    }
                    blockStream.close();
                    blockReplyStream.close();
                } catch (IOException ie) {
                    try {
                        blockStream.close();
                    } catch (IOException ie2) {
                    }
                    try {
                        blockReplyStream.close();
                    } catch (IOException ie2) {
                    }
                    namenode.abandonBlock(block, src.toString());
                    mustRecover = true;
                } finally {
                    blockStreamWorking = false;
                }
            }

            //
            // Done with local copy
            //
            backupStream.close();

            //
            // If necessary, recover from a failed datanode connection.
            //
            while (mustRecover) {
                nextBlockOutputStream(false);
                InputStream in = new FileInputStream(backupFile);
                try {
                    byte buf[] = new byte[4096];
                    int bytesRead = in.read(buf);
                    while (bytesRead >= 0) {
                        blockStream.writeLong((long) bytesRead);
                        blockStream.write(buf, 0, bytesRead);
                        bytesRead = in.read(buf);
                    }
                    blockStream.writeLong(0);
                    blockStream.close();
                    LOG.info("Recovered from failed datanode connection");
                    mustRecover = false;
                } catch (IOException ie) {
                    try {
                        blockStream.close();
                    } catch (IOException ie2) {
                    }
                    try {
                        blockReplyStream.close();
                    } catch (IOException ie2) {
                    }
                    namenode.abandonBlock(block, src.toString());
                    blockStreamWorking = false;
                }
            }

            //
            // Delete local backup, start new one
            //
            backupFile.delete();
            backupFile = File.createTempFile("ndfsout", "bak");
            backupStream = new BufferedOutputStream(new FileOutputStream(backupFile));
        }

        /**
         * Closes this output stream and releases any system 
         * resources associated with this stream.
         */
        public synchronized void close() throws IOException {
            if (closed) {
                throw new IOException("Stream closed");
            }

            flush();
            endBlock();

            backupStream.close();
            backupFile.delete();

            if (blockStreamWorking) {
                blockStream.close();                
                blockReplyStream.close();
                blockStreamWorking = false;
            }
            super.close();

            long localstart = System.currentTimeMillis();
            boolean fileComplete = false;
            while (! fileComplete) {
                fileComplete = namenode.complete(src.toString(), clientName.toString());
                if (!fileComplete) {
                    try {
                        Thread.sleep(400);
                        if (System.currentTimeMillis() - localstart > 5000) {
                            LOG.info("Could not complete file, retrying...");
                        }
                    } catch (InterruptedException ie) {
                    }
                }
            }
            closed = true;
        }
    }
}
