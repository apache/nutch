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

package org.apache.nutch.tools;

import java.io.*;
import java.util.*;

import org.apache.nutch.fs.*;
import org.apache.nutch.util.*;

/***************************************
 * LinkAnalysisTool performs link-analysis by using the
 * DistributedAnalysisTool.  This single-process all-in-one
 * tool is a wrapper around the more complicated distributed
 * one.  
 *
 * @author Mike Cafarella
 ***************************************/
public class LinkAnalysisTool {   
    NutchFileSystem nfs;
    File dbDir;
    DistributedAnalysisTool dat = null;

    /**
     * We need a DistributedAnalysisTool in order to get
     * things done!
     */
    public LinkAnalysisTool(NutchFileSystem nfs, File dbDir) throws IOException {
        this.nfs = nfs;
        this.dbDir = dbDir;
        this.dat = new DistributedAnalysisTool(nfs, dbDir);
    }

    /**
     * Do a single-process iteration over the database.  Implemented
     * by calling the distributed tool's functions.
     */
    public void iterate(int numIterations, File scoreFile) throws IOException {
        for (int i = 0; i < numIterations; i++) {
            
            File tmpDir = new File(dbDir, "tmpdir-" + System.currentTimeMillis() + "-la");
            nfs.mkdirs(tmpDir);

            dat.initRound(1, tmpDir);
            dat.computeRound(0, tmpDir);
            dat.completeRound(tmpDir, scoreFile);
        }
    }

    /**
     * Kick off the link analysis.  Submit the location of the db
     * directory, as well as the cache size.
     */
    public static void main(String argv[]) throws IOException {
        if (argv.length < 2) {
            System.out.println("usage: java org.apache.nutch.tools.LinkAnalysisTool (-local | -ndfs <namenode:port>) <db_dir> <numIterations>");
            return;
        }

        NutchFileSystem nfs = NutchFileSystem.parseArgs(argv, 0);
        File dbDir = new File(argv[0]);
        int numIterations = Integer.parseInt(argv[1]);

        System.out.println("Started at " + new Date(System.currentTimeMillis()));
        try {
            LinkAnalysisTool lat = new LinkAnalysisTool(nfs, dbDir);
            lat.iterate(numIterations, new File(dbDir, "linkstats.txt"));
        } finally {
            nfs.close();
            System.out.println("Finished at " + new Date(System.currentTimeMillis()));
        }
    }
}
