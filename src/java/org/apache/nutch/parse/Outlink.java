/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nutch.parse;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Map.Entry;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/* An outgoing link from a page. */
public class Outlink implements Writable {

    private String toUrl;
    private String anchor;
    private MapWritable md;

    public Outlink() {
    }

    public Outlink(String toUrl, String anchor) throws MalformedURLException {
        this.toUrl = toUrl;
        if (anchor == null)
            anchor = "";
        this.anchor = anchor;
        md = null;
    }

    public void readFields(DataInput in) throws IOException {
        toUrl = Text.readString(in);
        anchor = Text.readString(in);
        boolean hasMD = in.readBoolean();
        if (hasMD) {
            md = new org.apache.hadoop.io.MapWritable();
            md.readFields(in);
        } else
            md = null;
    }

    /** Skips over one Outlink in the input. */
    public static void skip(DataInput in) throws IOException {
        Text.skip(in); // skip toUrl
        Text.skip(in); // skip anchor
        boolean hasMD = in.readBoolean();
        if (hasMD) {
            MapWritable metadata = new org.apache.hadoop.io.MapWritable();
            metadata.readFields(in);
            ;
        }
    }

    public void write(DataOutput out) throws IOException {
        Text.writeString(out, toUrl);
        Text.writeString(out, anchor);
        if (md != null && md.size() > 0) {
            out.writeBoolean(true);
            md.write(out);
        } else {
            out.writeBoolean(false);
        }
    }

    public static Outlink read(DataInput in) throws IOException {
        Outlink outlink = new Outlink();
        outlink.readFields(in);
        return outlink;
    }

    public String getToUrl() {
        return toUrl;
    }

    public void setUrl(String toUrl) {
        this.toUrl = toUrl;
    }

    public String getAnchor() {
        return anchor;
    }

    public MapWritable getMetadata() {
        return md;
    }

    public void setMetadata(MapWritable md) {
        this.md = md;
    }

    public boolean equals(Object o) {
        if (!(o instanceof Outlink))
            return false;
        Outlink other = (Outlink) o;
        return this.toUrl.equals(other.toUrl)
                && this.anchor.equals(other.anchor);
    }

    public String toString() {
        StringBuffer repr = new StringBuffer("toUrl: ");
        repr.append(toUrl);
        repr.append(" anchor: ");
        repr.append(anchor);
        if (md != null && !md.isEmpty()) {
            for (Entry<Writable, Writable> e : md.entrySet()) {
                repr.append(" ");
                repr.append(e.getKey());
                repr.append(": ");
                repr.append(e.getValue());
            }
        }
        return repr.toString();
    }

}
