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
package org.apache.nutch.db;

import java.io.*;
import java.util.*;
import org.apache.nutch.io.*;

/************************************************
 * DBKeyDivision exists for other DB classes to
 * figure out how to find the right distributed-DB
 * section.  
 *
 * @author Mike Cafarella
 ************************************************/
public class DBKeyDivision {
    //
    // We need to divide the URL_UTF keyspace into equal portions.  
    // Some URLs are more likely than others, so we choose the
    // "dividing values" empirically.  These numbers are derived
    // from examining the DMOZ URL set.
    //
    public static String[] URL_KEYSPACE_DIVIDERS = {"",
                                                    "http://devrando",
                                                    "http://hoaxinh.",
                                                    "http://magyn.co",
                                                    "http://osha-enf",
                                                    "http://templeem",
                                                    "http://wiem.one",
                                                    "http://www.aero",
                                                    "http://www.anth",
                                                    "http://www.baro",
                                                    "http://www.bruc",
                                                    "http://www.chen",
                                                    "http://www.corh",
                                                    "http://www.dive",
                                                    "http://www.envi",
                                                    "http://www.fort",
                                                    "http://www.geoc",
                                                    "http://www.gree",
                                                    "http://www.htmi",
                                                    "http://www.j103",
                                                    "http://www.lake",
                                                    "http://www.masc",
                                                    "http://www.mrmp",
                                                    "http://www.njdr",
                                                    "http://www.pbs.",
                                                    "http://www.quee",
                                                    "http://www.saba",
                                                    "http://www.skee",
                                                    "http://www.surg",
                                                    "http://www.tony",
                                                    "http://www.uvm.",
                                                    "http://www.wigg"};

    //
    // For the MD5, we expect an even distribution.  
    //
    public static MD5Hash[] MD5_KEYSPACE_DIVIDERS = 
    {new MD5Hash("00000000000000000000000000000000"), 
     new MD5Hash("08000000000000000000000000000000"), 
     new MD5Hash("10000000000000000000000000000000"), 
     new MD5Hash("18000000000000000000000000000000"), 
     new MD5Hash("20000000000000000000000000000000"), 
     new MD5Hash("28000000000000000000000000000000"), 
     new MD5Hash("30000000000000000000000000000000"), 
     new MD5Hash("38000000000000000000000000000000"), 
     new MD5Hash("40000000000000000000000000000000"), 
     new MD5Hash("48000000000000000000000000000000"), 
     new MD5Hash("50000000000000000000000000000000"), 
     new MD5Hash("58000000000000000000000000000000"), 
     new MD5Hash("60000000000000000000000000000000"), 
     new MD5Hash("68000000000000000000000000000000"), 
     new MD5Hash("70000000000000000000000000000000"), 
     new MD5Hash("78000000000000000000000000000000"), 
     new MD5Hash("80000000000000000000000000000000"), 
     new MD5Hash("88000000000000000000000000000000"), 
     new MD5Hash("90000000000000000000000000000000"), 
     new MD5Hash("98000000000000000000000000000000"), 
     new MD5Hash("a0000000000000000000000000000000"), 
     new MD5Hash("a8000000000000000000000000000000"), 
     new MD5Hash("b0000000000000000000000000000000"), 
     new MD5Hash("b8000000000000000000000000000000"), 
     new MD5Hash("c0000000000000000000000000000000"), 
     new MD5Hash("c8000000000000000000000000000000"), 
     new MD5Hash("d0000000000000000000000000000000"), 
     new MD5Hash("d8000000000000000000000000000000"), 
     new MD5Hash("e0000000000000000000000000000000"), 
     new MD5Hash("e8000000000000000000000000000000"), 
     new MD5Hash("f0000000000000000000000000000000"), 
     new MD5Hash("f8000000000000000000000000000000")};

    //
    // I know it stinks that MAX_SECTIONS is only 32 right now.
    // We'll increase it after we compute more values for the
    // 'divider' arrays below.
    //
    public static int MAX_SECTIONS = URL_KEYSPACE_DIVIDERS.length;

    /**
     * Find the right section index for the given URL, and the number of
     * sections in the db overall.
     */
    public static int findURLSection(String url, int numSections) {
        if (numSections > URL_KEYSPACE_DIVIDERS.length) {
            throw new IllegalArgumentException("Too many db sections. " + numSections + " is greater than max of " + URL_KEYSPACE_DIVIDERS.length);
        }

        return binarySearch(url, URL_KEYSPACE_DIVIDERS, numSections);
    }

    /**
     * Find the right section index for the given MD5, and the number of
     * sections in the db overall.
     */
    public static int findMD5Section(MD5Hash md5, int numSections) {
        if (numSections > MD5_KEYSPACE_DIVIDERS.length) {
            throw new IllegalArgumentException("Too many db sections. " + numSections + " is greater than max of " + MD5_KEYSPACE_DIVIDERS.length);
        }

        return binarySearch(md5, MD5_KEYSPACE_DIVIDERS, numSections);
    }

    /**
     * Utility method for finding the right place in the section array.
     */
    static int binarySearch(Comparable obj, Comparable keyArray[], int numSections) {
        double stepSize = keyArray.length / (numSections * 1.0);
        int lastBest = -1;
        int low = 0;
        int high = numSections - 1;
        
        while (low <= high) {
            int mid = (low + high) / 2;
            Comparable midObj = keyArray[(int) Math.round(mid * stepSize)];
            int cmp = obj.compareTo(midObj);
            if (cmp < 0) {
                high = mid - 1;
            } else if (cmp > 0) {
                lastBest = mid;
                low = mid + 1;
            } else {
                return mid;
            }
        }
        return lastBest;
    }

}
