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

package org.apache.nutch.linkdb;

import java.io.*;
import java.util.*;
import org.apache.nutch.io.*;

/**********************************************
 * An entry in the LinkAnalysisTool's output.  Consists
 * of a single float for every entry in a table administered
 * by LinkAnalysisTool.
 *
 * @author Mike Cafarella
 *********************************************/
public class LinkAnalysisEntry extends VersionedWritable {
    private final static byte VERSION = 1;

    float score;

    /**
     */
    public LinkAnalysisEntry() {
        score = 0.0f;
    }

    public byte getVersion() { return VERSION; }

    /**
     */
    public void setScore(float score) {
        this.score = score;
    }

    /**
     */
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);

        score = in.readFloat();
    }

    /**
     */
    public void write(DataOutput out) throws IOException {
        super.write(out);

        out.writeFloat(score);
    }

    /**
     */
    public static LinkAnalysisEntry read(DataInput in) throws IOException {
        LinkAnalysisEntry lae = new LinkAnalysisEntry();
        lae.readFields(in);
        return lae;
    }

    //
    // Accessors
    //
    public float getScore() {
        return score;
    }

    /**
     */
    public boolean equals(Object o) {
        LinkAnalysisEntry other = (LinkAnalysisEntry) o;

        if (score == other.score) {
            return true;
        }
        return false;
    }

}
