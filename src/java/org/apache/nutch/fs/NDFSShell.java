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
package org.apache.nutch.fs;

import org.apache.nutch.util.*;
import org.apache.nutch.ndfs.*;

import java.io.*;
import java.util.*;

/**************************************************
 * This class provides some NDFS administrative access.
 *
 * @author Mike Cafarella
 **************************************************/
public class NDFSShell {
    NutchFileSystem nfs;

    /**
     */
    public NDFSShell(NutchFileSystem nfs) {
        this.nfs = nfs;
    }


    /**
     * Add a local file to the indicated name in NDFS. src is kept.
     */
    void copyFromLocal(File src, String dstf) throws IOException {
        nfs.copyFromLocalFile(src, new File(dstf));
    }

    /**
     * Add a local file to the indicated name in NDFS. src is removed.
     */
    void moveFromLocal(File src, String dstf) throws IOException {
        nfs.moveFromLocalFile(src, new File(dstf));
    }

    /**
     * Obtain the indicated NDFS file and copy to the local name.
     * srcf is kept.
     */
    void copyToLocal(String srcf, File dst) throws IOException {
        nfs.copyToLocalFile(new File(srcf), dst);
    }

    /**
     * Obtain the indicated NDFS file and copy to the local name.
     * srcf is removed.
     */
    void moveToLocal(String srcf, File dst) throws IOException {
        System.err.println("Option '-moveToLocal' is not implemented yet.");
    }

    /**
     * Get a listing of all files in NDFS at the indicated name
     */
    public void ls(String src) throws IOException {
        File items[] = nfs.listFiles(new File(src));
        if (items == null) {
            System.out.println("Could not get listing for " + src);
        } else {
            System.out.println("Found " + items.length + " items");
            for (int i = 0; i < items.length; i++) {
                File cur = items[i];
                System.out.println(cur.getPath() + "\t" + (cur.isDirectory() ? "<dir>" : ("" + cur.length())));
            }
        }
    }

    /**
     */
    public void du(String src) throws IOException {
        File items[] = nfs.listFiles(new File(src));
        if (items == null) {
            System.out.println("Could not get listing for " + src);
        } else {
            System.out.println("Found " + items.length + " items");
            for (int i = 0; i < items.length; i++) {
                NDFSFile cur = (NDFSFile) items[i];
                System.out.println(cur.getPath() + "\t" + cur.getContentsLength());
            }
        }
    }

    /**
     * Create the given dir
     */
    public void mkdir(String src) throws IOException {
        File f = new File(src);
        nfs.mkdirs(f);
    }
    
    /**
     * Rename an NDFS file
     */
    public void rename(String srcf, String dstf) throws IOException {
        if (nfs.rename(new File(srcf), new File(dstf))) {
            System.out.println("Renamed " + srcf + " to " + dstf);
        } else {
            System.out.println("Rename failed");
        }
    }

    /**
     * Copy an NDFS file
     */
    public void copy(String srcf, String dstf, NutchConf nutchConf) throws IOException {
        if (FileUtil.copyContents(nfs, new File(srcf), new File(dstf), true, nutchConf)) {
            System.out.println("Copied " + srcf + " to " + dstf);
        } else {
            System.out.println("Copy failed");
        }
    }

    /**
     * Delete an NDFS file
     */
    public void delete(String srcf) throws IOException {
        if (nfs.delete(new File(srcf))) {
            System.out.println("Deleted " + srcf);
        } else {
            System.out.println("Delete failed");
        }
    }

    /**
     * Return an abbreviated English-language desc of the byte length
     */
    String byteDesc(long len) {
        double val = 0.0;
        String ending = "";
        if (len < 1024 * 1024) {
            val = (1.0 * len) / 1024;
            ending = " k";
        } else if (len < 1024 * 1024 * 1024) {
            val = (1.0 * len) / (1024 * 1024);
            ending = " Mb";
        } else {
            val = (1.0 * len) / (1024 * 1024 * 1024);
            ending = " Gb";
        }
        return limitDecimal(val, 2) + ending;
    }

    String limitDecimal(double d, int placesAfterDecimal) {
        String strVal = Double.toString(d);
        int decpt = strVal.indexOf(".");
        if (decpt >= 0) {
            strVal = strVal.substring(0, Math.min(strVal.length(), decpt + 1 + placesAfterDecimal));
        }
        return strVal;
    }

    /**
     * Gives a report on how the NutchFileSystem is doing
     */
    public void report() throws IOException {
        if (nfs instanceof NDFSFileSystem) {
            NDFSFileSystem ndfsfs = (NDFSFileSystem) nfs;
            NDFSClient ndfs = ndfsfs.getClient();
            long total = ndfs.totalRawCapacity();
            long used = ndfs.totalRawUsed();
            DatanodeInfo info[] = ndfs.datanodeReport();

            long totalEffectiveBytes = 0;
            File topItems[] = nfs.listFiles(new File("/"));
            for (int i = 0; i < topItems.length; i++) {
                NDFSFile cur = (NDFSFile) topItems[i];
                totalEffectiveBytes += cur.getContentsLength();
            }

            System.out.println("Total raw bytes: " + total + " (" + byteDesc(total) + ")");
            System.out.println("Used raw bytes: " + used + " (" + byteDesc(used) + ")");
            System.out.println("% used: " + limitDecimal(((1.0 * used) / total) * 100, 2) + "%");
            System.out.println();
            System.out.println("Total effective bytes: " + totalEffectiveBytes + " (" + byteDesc(totalEffectiveBytes) + ")");
            System.out.println("Effective replication multiplier: " + (1.0 * used / totalEffectiveBytes));

            System.out.println("-------------------------------------------------");
            System.out.println("Datanodes available: " + info.length);
            System.out.println();
            for (int i = 0; i < info.length; i++) {
                System.out.println("Name: " + info[i].getName().toString());
                long c = info[i].getCapacity();
                long r = info[i].getRemaining();
                long u = c - r;
                System.out.println("Total raw bytes: " + c + " (" + byteDesc(c) + ")");
                System.out.println("Used raw bytes: " + u + " (" + byteDesc(u) + ")");
                System.out.println("% used: " + limitDecimal(((1.0 * u) / c) * 100, 2) + "%");
                System.out.println("Last contact with namenode: " + new Date(info[i].lastUpdate()));
                System.out.println();
            }
        }
    }

    /**
     * main() has some simple utility methods
     */
    public static void main(String argv[]) throws IOException {
        if (argv.length < 1) {
            System.out.println("Usage: java NDFSShell [-local | -ndfs <namenode:port>]" +
                    " [-ls <path>] [-du <path>] [-mv <src> <dst>] [-cp <src> <dst>] [-rm <src>]" +
                    " [-put <localsrc> <dst>] [-copyFromLocal <localsrc> <dst>] [-moveFromLocal <localsrc> <dst>]" + 
                    " [-get <src> <localdst>] [-copyToLocal <src> <localdst>] [-moveToLocal <src> <localdst>]" +
                    " [-mkdir <path>] [-report]");
            return;
        }

        NutchConf nutchConf = new NutchConf();
        int i = 0;
        NutchFileSystem nfs = NutchFileSystem.parseArgs(argv, i, nutchConf);
        try {
            NDFSShell tc = new NDFSShell(nfs);

            String cmd = argv[i++];
            if ("-put".equals(cmd) || "-copyFromLocal".equals(cmd)) {
                tc.copyFromLocal(new File(argv[i++]), argv[i++]);
            } else if ("-moveFromLocal".equals(cmd)) {
                tc.moveFromLocal(new File(argv[i++]), argv[i++]);
            } else if ("-get".equals(cmd) || "-copyToLocal".equals(cmd)) {
                tc.copyToLocal(argv[i++], new File(argv[i++]));
            } else if ("-moveToLocal".equals(cmd)) {
                tc.moveToLocal(argv[i++], new File(argv[i++]));
            } else if ("-ls".equals(cmd)) {
                String arg = i < argv.length ? argv[i++] : "";
                tc.ls(arg);
            } else if ("-mv".equals(cmd)) {
                tc.rename(argv[i++], argv[i++]);
            } else if ("-cp".equals(cmd)) {
                tc.copy(argv[i++], argv[i++], nutchConf);
            } else if ("-rm".equals(cmd)) {
                tc.delete(argv[i++]);
            } else if ("-du".equals(cmd)) {
                String arg = i < argv.length ? argv[i++] : "";
                tc.du(arg);
            } else if ("-mkdir".equals(cmd)) {
                tc.mkdir(argv[i++]);
            } else if ("-report".equals(cmd)) {
                tc.report();
            }
            System.exit(0);
        } finally {
            nfs.close();
        }
    }
}
