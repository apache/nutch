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

import java.io.*;
import java.util.*;

/*************************************************
 * FSDirectory stores the filesystem directory state.
 * It handles writing/loading values to disk, and logging
 * changes as we go.
 *
 * It keeps the filename->blockset mapping always-current
 * and logged to disk.
 * 
 * @author Mike Cafarella
 *************************************************/
public class FSDirectory implements FSConstants {
    static String FS_IMAGE = "fsimage";
    static String NEW_FS_IMAGE = "fsimage.new";
    static String OLD_FS_IMAGE = "fsimage.old";

    private static final byte OP_ADD = 0;
    private static final byte OP_RENAME = 1;
    private static final byte OP_DELETE = 2;
    private static final byte OP_MKDIR = 3;

    /******************************************************
     * We keep an in-memory representation of the file/block
     * hierarchy.
     ******************************************************/
    class INode {
        public String name;
        public INode parent;
        public Vector children = new Vector();
        public Block blocks[];

        /**
         */
        INode(String name, INode parent, Block blocks[]) {
            this.name = name;
            this.parent = parent;
            this.blocks = blocks;
        }

        /**
         */
        INode getNode(String target) {
            if (! target.startsWith("/")) {
                return null;
            }

            if (parent == null) {
                if ("/".equals(target)) {
                    return this;
                } else {
                    // Check with children
                    for (Iterator it = children.iterator(); it.hasNext(); ) {
                        INode child = (INode) it.next();
                        INode result = child.getNode(target);
                        if (result != null) {
                            return result;
                        }
                    }
                }
            } else {
                // Strip the leading slash
                if (target.length() > 1) {
                    target = target.substring(1);
                }

                // Check if it's the current node
                if (name.equals(target)) {
                    return this;
                }

                // Get the chunk up to the next slash
                String curComponent, remainder;
                int slash = target.indexOf('/');
                if (slash < 0) {
                    return null;
                } else {
                    curComponent = target.substring(0, slash);
                    remainder = target.substring(slash);
                }

                // Make sure we're on the right track
                if (! name.equals(curComponent)) {
                    return null;
                } 

                // Check with children
                for (Iterator it = children.iterator(); it.hasNext(); ) {
                    INode child = (INode) it.next();
                    INode result = child.getNode(remainder);
                    if (result != null) {
                        return result;
                    }
                }
            }
            return null;
        }

        /**
         */
        INode addNode(String target, Block blks[]) {
            if (getNode(target) != null) {
                return null;
            } else {
                String parentName = NDFSFile.getNDFSParent(target);
                if (parentName == null) {
                    return null;
                }

                INode parentNode = getNode(parentName);
                if (parentNode == null) {
                    return null;
                } else {
                    String targetName = new File(target).getName();
                    INode newItem = new INode(targetName, parentNode, blks);
                    parentNode.children.add(newItem);
                    return newItem;
                }
            }
        }

        /**
         */
        INode removeNode(String target) {
            INode targetNode = getNode(target);
            if (targetNode == null) {
                return null;
            } else {
                targetNode.parent.children.remove(targetNode);
                return targetNode;
            }
        }

        /**
         */
        int numItemsInTree() {
            int total = 0;
            for (Iterator it = children.iterator(); it.hasNext(); ) {
                INode child = (INode) it.next();
                total += child.numItemsInTree();
            }
            return total + 1;
        }

        /**
         */
        String computeName() {
            if (parent != null) {
                return parent.computeName() + "/" + name;
            } else {
                return name;
            }
        }

        /**
         */
        long computeFileLength() {
            long total = 0;
            if (blocks != null) {
                for (int i = 0; i < blocks.length; i++) {
                    total += blocks[i].getNumBytes();
                }
            }
            return total;
        }

        /**
         */
        long computeContentsLength() {
            long total = computeFileLength();
            for (Iterator it = children.iterator(); it.hasNext(); ) {
                INode child = (INode) it.next();
                total += child.computeContentsLength();
            }
            return total;
        }

        /**
         */
        void listContents(Vector v) {
            if (parent != null && blocks != null) {
                v.add(this);
            }

            for (Iterator it = children.iterator(); it.hasNext(); ) {
                INode child = (INode) it.next();
                v.add(child);
            }
        }

        /**
         */
        void saveImage(String parentPrefix, DataOutputStream out) throws IOException {
            String fullName = "";
            if (parent != null) {
                fullName = parentPrefix + "/" + name;
                new UTF8(fullName).write(out);
                if (blocks == null) {
                    out.writeInt(0);
                } else {
                    out.writeInt(blocks.length);
                    for (int i = 0; i < blocks.length; i++) {
                        blocks[i].write(out);
                    }
                }
            }
            for (Iterator it = children.iterator(); it.hasNext(); ) {
                INode child = (INode) it.next();
                child.saveImage(fullName, out);
            }
        }
    }

    INode rootDir = new INode("", null, null);
    TreeSet activeBlocks = new TreeSet();
    TreeMap activeLocks = new TreeMap();
    DataOutputStream editlog = null;
    boolean ready = false;

    /**
     * Create a FileSystem directory, and load its info
     * from the indicated place.
     */
    public FSDirectory(File dir) throws IOException {
        File fullimage = new File(dir, "image");
        if (! fullimage.exists()) {
            fullimage.mkdirs();
        }
        File edits = new File(dir, "edits");
        if (loadFSImage(fullimage, edits)) {
            saveFSImage(fullimage, edits);
        }

        synchronized (this) {
            this.ready = true;
            this.notifyAll();
            this.editlog = new DataOutputStream(new FileOutputStream(edits));
        }
    }

    /**
     * Shutdown the filestore
     */
    public void close() throws IOException {
        editlog.close();
    }

    /**
     * Block until the object is ready to be used.
     */
    void waitForReady() {
        if (! ready) {
            synchronized (this) {
                while (!ready) {
                    try {
                        this.wait(5000);
                    } catch (InterruptedException ie) {
                    }
                }
            }
        }
    }

    /**
     * Load in the filesystem image.  It's a big list of
     * filenames and blocks.  Return whether we should
     * "re-save" and consolidate the edit-logs
     */
    boolean loadFSImage(File fsdir, File edits) throws IOException {
        //
        // Atomic move sequence, to recover from interrupted save
        //
        File curFile = new File(fsdir, FS_IMAGE);
        File newFile = new File(fsdir, NEW_FS_IMAGE);
        File oldFile = new File(fsdir, OLD_FS_IMAGE);

        // Maybe we were interrupted between 2 and 4
        if (oldFile.exists() && curFile.exists()) {
            oldFile.delete();
            if (edits.exists()) {
                edits.delete();
            }
        } else if (oldFile.exists() && newFile.exists()) {
            // Or maybe between 1 and 2
            newFile.renameTo(curFile);
            oldFile.delete();
        } else if (curFile.exists() && newFile.exists()) {
            // Or else before stage 1, in which case we lose the edits
            newFile.delete();
        }

        //
        // Load in bits
        //
        if (curFile.exists()) {
            DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(curFile)));
            try {
                int numFiles = in.readInt();
                for (int i = 0; i < numFiles; i++) {
                    UTF8 name = new UTF8();
                    name.readFields(in);
                    int numBlocks = in.readInt();
                    if (numBlocks == 0) {
                        unprotectedAddFile(name, null);
                    } else {
                        Block blocks[] = new Block[numBlocks];
                        for (int j = 0; j < numBlocks; j++) {
                            blocks[j] = new Block();
                            blocks[j].readFields(in);
                        }
                        unprotectedAddFile(name, blocks);
                    }
                }
            } finally {
                in.close();
            }
        }

        if (edits.exists() && loadFSEdits(edits) > 0) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Load an edit log, and apply the changes to the in-memory structure
     *
     * This is where we apply edits that we've been writing to disk all
     * along.
     */
    int loadFSEdits(File edits) throws IOException {
        int numEdits = 0;

        if (edits.exists()) {
            DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(edits)));
            try {
                while (in.available() > 0) {
                    byte opcode = in.readByte();
                    numEdits++;
                    switch (opcode) {
                    case OP_ADD: {
                        UTF8 name = new UTF8();
                        name.readFields(in);
                        ArrayWritable aw = new ArrayWritable(Block.class);
                        aw.readFields(in);
                        Writable writables[] = (Writable[]) aw.get();
                        Block blocks[] = new Block[writables.length];
                        System.arraycopy(writables, 0, blocks, 0, blocks.length);
                        unprotectedAddFile(name, blocks);
                        break;
                    } 
                    case OP_RENAME: {
                        UTF8 src = new UTF8();
                        UTF8 dst = new UTF8();
                        src.readFields(in);
                        dst.readFields(in);
                        unprotectedRenameTo(src, dst);
                        break;
                    }
                    case OP_DELETE: {
                        UTF8 src = new UTF8();
                        src.readFields(in);
                        unprotectedDelete(src);
                        break;
                    }
                    case OP_MKDIR: {
                        UTF8 src = new UTF8();
                        src.readFields(in);
                        unprotectedMkdir(src.toString());
                        break;
                    }
                    default: {
                        throw new IOException("Never seen opcode " + opcode);
                    }
                    }
                }
            } finally {
                in.close();
            }
        }
        return numEdits;
    }

    /**
     * Save the contents of the FS image
     */
    void saveFSImage(File fullimage, File edits) throws IOException {
        File curFile = new File(fullimage, FS_IMAGE);
        File newFile = new File(fullimage, NEW_FS_IMAGE);
        File oldFile = new File(fullimage, OLD_FS_IMAGE);

        //
        // Write out data
        //
        DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(newFile)));
        try {
            out.writeInt(rootDir.numItemsInTree() - 1);
            rootDir.saveImage("", out);
        } finally {
            out.close();
        }

        //
        // Atomic move sequence
        //
        // 1.  Move cur to old
        curFile.renameTo(oldFile);
        
        // 2.  Move new to cur
        newFile.renameTo(curFile);

        // 3.  Remove pending-edits file (it's been integrated with newFile)
        edits.delete();
        
        // 4.  Delete old
        oldFile.delete();
    }

    /**
     * Write an operation to the edit log
     */
    void logEdit(byte op, Writable w1, Writable w2) {
        synchronized (editlog) {
            try {
                editlog.write(op);
                if (w1 != null) {
                    w1.write(editlog);
                }
                if (w2 != null) {
                    w2.write(editlog);
                }
            } catch (IOException ie) {
            }
        }
    }

    /**
     * Add the given filename to the fs.
     */
    public boolean addFile(UTF8 src, Block blocks[]) {
        waitForReady();

        // Always do an implicit mkdirs for parent directory tree
        mkdirs(NDFSFile.getNDFSParent(src.toString()));
        if (unprotectedAddFile(src, blocks)) {
            logEdit(OP_ADD, src, new ArrayWritable(Block.class, blocks));
            return true;
        } else {
            return false;
        }
    }
    
    /**
     */
    boolean unprotectedAddFile(UTF8 name, Block blocks[]) {
        synchronized (rootDir) {
            if (blocks != null) {
                // Add file->block mapping
                for (int i = 0; i < blocks.length; i++) {
                    activeBlocks.add(blocks[i]);
                }
            }
            return (rootDir.addNode(name.toString(), blocks) != null);
        }
    }

    /**
     * Change the filename
     */
    public boolean renameTo(UTF8 src, UTF8 dst) {
        waitForReady();
        if (unprotectedRenameTo(src, dst)) {
            logEdit(OP_RENAME, src, dst);
            return true;
        } else {
            return false;
        }
    }

    /**
     */
    boolean unprotectedRenameTo(UTF8 src, UTF8 dst) {
        synchronized(rootDir) {
            INode removedNode = rootDir.removeNode(src.toString());
            if (removedNode == null) {
                return false;
            }

            INode newNode = rootDir.addNode(dst.toString(), removedNode.blocks);
            if (newNode != null) {
                newNode.children = removedNode.children;
                for (Iterator it = newNode.children.iterator(); it.hasNext(); ) {
                    INode child = (INode) it.next();
                    child.parent = newNode;
                }
                return true;
            } else {
                removedNode.parent.children.add(removedNode);
                return false;
            }
        }
    }

    /**
     * Remove the file from management, return blocks
     */
    public Block[] delete(UTF8 src) {
        waitForReady();
        logEdit(OP_DELETE, src, null);
        return unprotectedDelete(src);
    }

    /**
     */
    Block[] unprotectedDelete(UTF8 src) {
        synchronized (rootDir) {
            INode targetNode = rootDir.getNode(src.toString());
            if (targetNode == null) {
                return null;
            } else {
                Vector allBlocks = new Vector();
                Vector contents = new Vector();
                targetNode.listContents(contents);

                for (Iterator it = contents.iterator(); it.hasNext(); ) {
                    INode cur = (INode) it.next();
                    INode removedNode = rootDir.removeNode(cur.computeName());
                    if (removedNode != null) {
                        Block blocks[] = removedNode.blocks;
                        if (blocks != null) {
                            for (int i = 0; i < blocks.length; i++) {
                                activeBlocks.remove(blocks[i]);
                                allBlocks.add(blocks[i]);
                            }
                        }
                    }
                }
                rootDir.removeNode(src.toString());
                return (Block[]) allBlocks.toArray(new Block[0]);
            }
        }
    }

    /**
     */
    public int obtainLock(UTF8 src, UTF8 holder, boolean exclusive) {
        TreeSet holders = (TreeSet) activeLocks.get(src);
        if (holders == null) {
            holders = new TreeSet();
            activeLocks.put(src, holders);
        }
        if (exclusive && holders.size() > 0) {
            return STILL_WAITING;
        } else {
            holders.add(holder);
            return COMPLETE_SUCCESS;
        }
    }

    /**
     */
    public int releaseLock(UTF8 src, UTF8 holder) {
        TreeSet holders = (TreeSet) activeLocks.get(src);
        if (holders != null && holders.contains(holder)) {
            holders.remove(holder);
            if (holders.size() == 0) {
                activeLocks.remove(src);
            }
            return COMPLETE_SUCCESS;
        } else {
            return OPERATION_FAILED;
        }
    }

    /**
     * Get a listing of files given path 'src'
     *
     * This function is admittedly very inefficient right now.  We'll
     * make it better later.
     */
    public NDFSFileInfo[] getListing(UTF8 src) {
        String srcs = normalizePath(src);

        synchronized (rootDir) {
            INode targetNode = rootDir.getNode(srcs);
            if (targetNode == null) {
                return null;
            } else {
                Vector contents = new Vector();
                targetNode.listContents(contents);

                NDFSFileInfo listing[] = new NDFSFileInfo[contents.size()];
                int i = 0;
                for (Iterator it = contents.iterator(); it.hasNext(); i++) {
                    INode cur = (INode) it.next();
                    UTF8 curName = new UTF8(cur.computeName());
                    listing[i] = new NDFSFileInfo(curName, cur.computeFileLength(), cur.computeContentsLength(), isDir(curName));
                    //listing[i] = new NDFSFileInfo(curName, cur.computeFileLength(), 0, isDir(curName));
                    //listing[i] = new NDFSFileInfo(curName, cur.computeFileLength(), 0, false);
                }
                return listing;
            }
        }
    }

    /**
     * Get the blocks associated with the file
     */
    public Block[] getFile(UTF8 src) {
        waitForReady();
        synchronized (rootDir) {
            INode targetNode = rootDir.getNode(src.toString());
            if (targetNode == null) {
                return null;
            } else {
                return targetNode.blocks;
            }
        }
    }

    /** 
     * Check whether the filepath could be created
     */
    public boolean isValidToCreate(UTF8 src) {
        String srcs = normalizePath(src);
        synchronized (rootDir) {
            if (srcs.startsWith("/") && 
                ! srcs.endsWith("/") && 
                rootDir.getNode(srcs) == null) {
                return true;
            } else {
                return false;
            }
        }
    }

    /**
     * Check whether it's a directory
     */
    public boolean isDir(UTF8 src) {
        synchronized (rootDir) {
            INode node = rootDir.getNode(normalizePath(src));
            if (node != null && node.blocks == null) {
                return true;
            } else {
                return false;
            }
        }
    }

    /**
     * Create the given directory and all its parent dirs.
     */
    public boolean mkdirs(UTF8 src) {
        return mkdirs(src.toString());
    }

    /**
     * Create directory entries for every item
     */
    boolean mkdirs(String src) {
        src = normalizePath(new UTF8(src));

        // Use this to collect all the dirs we need to construct
        Vector v = new Vector();

        // The dir itself
        v.add(src);

        // All its parents
        String parent = NDFSFile.getNDFSParent(src);
        while (parent != null) {
            v.add(parent);
            parent = NDFSFile.getNDFSParent(parent);
        }

        // Now go backwards through list of dirs, creating along
        // the way
        boolean lastSuccess = false;
        int numElts = v.size();
        for (int i = numElts - 1; i >= 0; i--) {
            String cur = (String) v.elementAt(i);
            INode inserted = unprotectedMkdir(cur);
            if (inserted != null) {
                logEdit(OP_MKDIR, new UTF8(inserted.computeName()), null);
                lastSuccess = true;
            } else {
                lastSuccess = false;
            }
        }
        return lastSuccess;
    }

    /**
     */
    INode unprotectedMkdir(String src) {
        synchronized (rootDir) {
            return rootDir.addNode(src, null);
        }
    }

    /**
     */
    String normalizePath(UTF8 src) {
        String srcs = src.toString();
        if (srcs.length() > 1 && srcs.endsWith("/")) {
            srcs = srcs.substring(0, srcs.length() - 1);
        }
        return srcs;
    }

    /**
     * Returns whether the given block is one pointed-to by a file.
     */
    public boolean isValidBlock(Block b) {
        synchronized (rootDir) {
            if (activeBlocks.contains(b)) {
                return true;
            } else {
                return false;
            }
        }
    }
}
