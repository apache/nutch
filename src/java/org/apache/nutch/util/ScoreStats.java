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

package org.apache.nutch.util;

import java.io.*;
import java.util.*;

import org.apache.nutch.db.*;
import org.apache.nutch.fs.*;

/*************************************************************
 * When we generate a fetchlist, we need to choose a "cutoff"
 * score, such that any scores above that cutoff will be included
 * in the fetchlist.  Any scores below will not be.  (It is too
 * hard to do the obvious thing, which is to sort the list of all
 * pages by score, and pick the top K.)
 *
 * We need a good way to choose that cutoff.  ScoreStats is used 
 * during LinkAnalysis to track the distribution of scores that
 * we compute.  We bucketize the scorespace into 2000 buckets.
 * the first 1000 are equally-spaced counts for the range 0..1.0
 * (non-inclusive).  The 2nd buckets are logarithmically spaced
 * between 1 and Float.MAX_VALUE.
 *
 * If the score is < 1, then choose a bucket by (score / 1000) and
 * choosing the incrementing the resulting slot.
 *
 * If the score is >1, then take the base-10 log, and take the
 * integer floor.  This should be an int no greater than 9.  This
 * is the hundreds-place digit for the index.  (Since '1' is in
 * the thousands-place.)  Next, find where the score appears in
 * the range between floor(log(score)), and ceiling(log(score)).
 * The percentage of the distance between these two values is
 * reflected in the final two digits for the index.
 *
 * @author Mike Cafarella
 ***************************************************************/
public class ScoreStats {
    private final static double INVERTED_LOG_BASE_TEN = (1.0 / Math.log(10));
    private final static double EXP_127_MODIFIER = (1000.0 / (Math.log(Float.MAX_VALUE)  * INVERTED_LOG_BASE_TEN));

    private final static double RANGE_COMPRESSOR = INVERTED_LOG_BASE_TEN * EXP_127_MODIFIER;
    long totalScores = 0;

    //
    // For bucketizing score counts
    //
    long buckets[] = new long[2001];

    /**
     */
    public ScoreStats() {
    }

    /**
     * Increment the counter in the right place.  We keep
     * 2000 different buckets.  Half of them are <1, and
     * half are >1.
     
     * Dies when it tries to fill bucket "1132"
     */
    public void addScore(float score) {
        if (score < 1) {
            int index = (int) Math.floor(score * 1000);
            buckets[index]++;
        } else {
            // Here we need to find the floor'ed base-10 logarithm.
            int index = (int) Math.floor(Math.log(score) * RANGE_COMPRESSOR);
            index += 1000;
            buckets[index]++;
        }
        totalScores++;
    }

    /**
     * Print out the distribution, with greater specificity
     * for percentiles 90th - 100th.
     */
    public void emitDistribution(PrintStream pout) {
        pout.println("***** Estimated Score Distribution *****");
        pout.println("  (to choose a fetchlist cutoff score)");
        pout.println();

        // Figure out how big each percentile chunk is.
        double decileChunk = totalScores / 10.0;
        double percentileChunk = totalScores / 100.0;

        // Now, emit everything
        double grandTotal = 0, minScore = Double.MAX_VALUE, maxScore = Double.MIN_VALUE;
        long scoresSoFar = 0;
        int decileCount = 0, percentileCount = 0;

        // Go through all the sample buckets
        for (int i = 0; i < buckets.length; i++) {
            //
            // Always increment the
            // seen-sample counter by the number of samples
            // in the current bucket.
            //
            scoresSoFar += buckets[i];

            // From the bucket index, recreate the
            // original score (as best we can)
            double reconstructedValue = 0.0;
            if (i < 1000) {
                reconstructedValue = i / 1000.0;
            } else {
                int localIndex = i - 1000;
                reconstructedValue = Math.exp(localIndex / RANGE_COMPRESSOR);
            }

            // Keep running stats on min, max, avg scores
            grandTotal += (reconstructedValue * buckets[i]);
            if (buckets[i] > 0) {
                if (minScore > reconstructedValue) {
                    minScore = reconstructedValue;
                }
                if (maxScore < reconstructedValue) {
                    maxScore = reconstructedValue;
                }
            }

            //
            // If the number of samples we've seen so far is
            // GTE the predicted percentile break, then we want to
            // emit a println().
            //
            if (scoresSoFar >= ((decileCount * decileChunk) + (percentileCount * percentileChunk))) {

                // Compute what percentile of the items
                // we've reached
                double precisePercentile = ((int) Math.round(((totalScores - scoresSoFar) / (totalScores * 1.0)) * 10000)) / 100.0;

                // Emit
                String equalityOperator = ">=";
                if ((totalScores - scoresSoFar) == 0) {
                    equalityOperator = ">";
                }

                pout.println(precisePercentile + "% (" + (totalScores - scoresSoFar) + ")  have score " + equalityOperator + " " + reconstructedValue);

                // Bump our decile and percentile counters.
                // We may have to bump multiple times if
                // a single bucket carried us across several
                // boundaries.
                while (decileCount < 9 && scoresSoFar >= (decileCount * decileChunk) + (percentileCount * percentileChunk)) {
                    decileCount++;
                }
                if (decileCount >= 9) {
                    while (percentileCount < 10 && scoresSoFar >= (decileCount * decileChunk) + (percentileCount * percentileChunk)) {
                        percentileCount++;
                    }
                }

                // If we've reached the top percentile, then we're done!
                if (percentileCount >= 10) {
                    break;
                }
            }
        }

        pout.println();
        pout.println();
        pout.println("Min score is " + minScore);
        pout.println("Max score is " + maxScore);
        pout.println("Average score is " + (grandTotal / scoresSoFar));
    }

    /**
     */
    public static void main(String argv[]) throws IOException {
        if (argv.length < 1) {
            System.out.println("Usage: java org.apache.nutch.util.ScoreStats [-real (-local | -ndfs <namenode:port>) <db>]  [-simulated <numScores> <min> <max> [seed]]");
            return;
        }

        NutchFileSystem nfs = null;
        File root = null;
        long seed = new Random().nextLong();
        boolean simulated = false;
        int numScores = 0;
        float min = 0, max = 0;

        if ("-real".equals(argv[0])) {
            nfs = NutchFileSystem.parseArgs(argv, 1);
            root = new File(argv[1]);
        } else if ("-simulated".equals(argv[0])) {
            simulated = true;
            numScores = Integer.parseInt(argv[1]);
            min = Float.parseFloat(argv[2]);
            max = Float.parseFloat(argv[3]);
            if (argv.length > 4) {
                seed = Long.parseLong(argv[4]);
            }
        } else {
            System.out.println("No command specified");
        }

        System.out.println("Using seed: " + seed);
        ScoreStats ss = new ScoreStats();
        if (simulated) {
            Random r = new Random(seed);
            for (int i = 0; i < numScores; i++) {
                float newScore = min + (r.nextFloat() * (max - min));
                ss.addScore(newScore);
            }
        } else {
            IWebDBReader reader = new WebDBReader(nfs, root);
            try {
                for (Enumeration e = reader.pages(); e.hasMoreElements(); ) {
                    Page p = (Page) e.nextElement();
                    ss.addScore(p.getScore());
                }
            } finally {
                reader.close();
            }
        }
        
        ss.emitDistribution(System.out);
    }
}

