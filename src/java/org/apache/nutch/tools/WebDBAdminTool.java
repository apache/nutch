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
import java.util.logging.*;

import org.apache.nutch.db.*;
import org.apache.nutch.fs.*;
import org.apache.nutch.util.*;
import org.apache.nutch.linkdb.*;
import org.apache.nutch.pagedb.*;
import org.apache.nutch.pagedb.*;

/******************************************
 * The WebDBAdminTool is for Nutch administrators
 * who need special access to the webdb.  It allows
 * for finer editing of the stored values.
 *
 * @author Mike Cafarella
 ******************************************/
public class WebDBAdminTool {
    public static final Logger LOG = LogFormatter.getLogger("org.apache.nutch.tools.WebDBAdminTool");

    IWebDBReader reader;

    public WebDBAdminTool(IWebDBReader reader) {
        this.reader = reader;
    }

    /**
     * Emit the webdb to 2 text files.
     */
    public void textDump(String dumpName) throws IOException {
        //
        // First the pages
        //
        PrintStream out = new PrintStream(new BufferedOutputStream(new FileOutputStream(new File(dumpName + ".pages"))));
        try {
            for (Enumeration e = reader.pages(); e.hasMoreElements(); ) {
                Page p = (Page) e.nextElement();
                out.println(p.toTabbedString());
            }
        } finally {
            out.close();
        }

        //
        // Then the links
        //
        out = new PrintStream(new BufferedOutputStream(new FileOutputStream(new File(dumpName + ".links"))));
        try {
            for (Enumeration e = reader.links(); e.hasMoreElements(); ) {
                Link l = (Link) e.nextElement();
                out.println(l.toTabbedString());
            }
        } finally {
            out.close();
        }                                    
    }

    /**
     * Emit the top K-rated Pages.
     */
    public void emitTopK(int k) throws IOException {
        // Create a sorted list
        SortedSet topSet = new TreeSet(new Comparator() {
            public int compare(Object o1, Object o2) {
                Page p1 = (Page) o1;
                Page p2 = (Page) o2;
                if (p1.getScore() < p2.getScore()) {
                    return -1;
                } else if (p1.getScore() == p2.getScore()) {
                    return 0;
                } else {
                    return 1;
                }
            }
        }
            );

        // Find the top k elts
        Page lowestPage = null;
        for (Enumeration e = reader.pages(); e.hasMoreElements(); ) {
            Page curPage = (Page) e.nextElement();
                    
            if (topSet.size() < k) {
                topSet.add(curPage);
                lowestPage = (Page) topSet.first();
            } else if (lowestPage.getScore() < curPage.getScore()) {
                topSet.remove(lowestPage);
                topSet.add(curPage);
                lowestPage = (Page) topSet.first();
            }
        }
            
        // Print them out
        int i = 0;
        for (Iterator it = topSet.iterator(); it.hasNext(); i++) {
            LOG.info("Page " + i + ": " + (Page) it.next());
        }
    }

    /**
     * Emit each page's score and link data
     */
    public void scoreDump() throws IOException {
        for (Enumeration e = reader.pages(); e.hasMoreElements(); ) {
            Page p = (Page) e.nextElement();
            Link links[] = reader.getLinks(p.getURL());
            int numLinks = 0;
            if (links != null) {
                numLinks = links.length;
            }

            LOG.info(p.getURL() + "\t" + p.getScore() + "\t" + numLinks);
        }
    }

    /**
     * This tool performs a number of generic db management tasks.
     * Right now, it only emits the text-format database.
     */
    public static void main(String argv[]) throws FileNotFoundException, IOException {
        if (argv.length < 2) {
            System.out.println("Usage: java org.apache.nutch.tools.WebDBAdminTool (-local | -ndfs <namenode:port>) db [-create] [-textdump dumpPrefix] [-scoredump] [-top k]");
            return;
        }

        boolean create = false;
        String command = null, dumpName = null;
        int k = 0;
        int i = 0;
        NutchFileSystem nfs = NutchFileSystem.parseArgs(argv, i);
        File root = new File(argv[i++]);
        for (; i < argv.length; i++) {
            if ("-create".equals(argv[i])) {
                command = argv[i];
                create = true;
            } else if ("-textdump".equals(argv[i])) {
                command = argv[i];
                i++;
                dumpName = argv[i];
            } else if ("-top".equals(argv[i])) {
                command = argv[i];
                i++;
                k = Integer.parseInt(argv[i]);
            } else if ("-scoredump".equals(argv[i])) {
                command = argv[i];
            }
        }

        //
        // For db creation
        //
        if ("-create".equals(command)) {
            WebDBWriter.createWebDB(nfs, root);
            LOG.info("Created webdb at " + nfs + "," + root);
            nfs.close();
            return;
        }

        //
        // For other functions
        //
        IWebDBReader reader = new WebDBReader(nfs, root);
        try {
            WebDBAdminTool admin = new WebDBAdminTool(reader);
            if ("-textdump".equals(command)) {
                admin.textDump(dumpName);
            } else if ("-top".equals(command)) {
                admin.emitTopK(k);
            } else if ("-scoredump".equals(command)) {
                admin.scoreDump();
            }
        } finally {
            reader.close();
            nfs.close();
        }
    }
}
