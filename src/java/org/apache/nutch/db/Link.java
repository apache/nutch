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
import java.net.*;
import java.net.MalformedURLException;

import org.apache.nutch.io.*;
import org.apache.nutch.util.*;
import org.apache.nutch.net.UrlNormalizerFactory;

/*********************************************
 * This is the field in the Link Database.
 * <pre>
 * Each row is a Link:
 *   type   name    description
 * ---------------------------------------------------------------
 * byte   VERSION - A byte indicating the version of this entry.
 * 128bit FROM_ID - The MD5 hash of the source of the link.
 * 64bit  DOMAIN_ID - The 8-byte MD5Hash of the source's domain.
 * string TO_URL  - The URL destination of the link.
 * string ANCHOR  - The anchor text of the link.
 * boolean TARGET_HAS_OUTLINK   - Whether the target of the link has outlinks.
 * </pre>
 * 
 * @author Mike Cafarella
 *************************************************/
public class Link implements WritableComparable {
    public static final int MAX_ANCHOR_LENGTH =
      NutchConf.get().getInt("db.max.anchor.length", 100);

    private final static byte VERSION_1 = 1;
    private final static byte VERSION_2 = 2;
    private final static byte CUR_VERSION = 5;

    private MD5Hash fromID;
    private UTF8 url;
    private long domainID;
    private UTF8 anchor;
    private boolean targetHasOutlink;

    /**
     * Create the Link with no data
     */
    public Link() {
        this.fromID = new MD5Hash();
        this.url = new UTF8();
        this.domainID = 0;
        this.anchor = new UTF8();
        this.targetHasOutlink = false;
    }

    /**
     * Create the record
     */
    public Link(MD5Hash fromID, long domainID, String urlString, String anchorText)
      throws MalformedURLException {
        this.fromID = fromID;
        this.url = new UTF8(UrlNormalizerFactory.getNormalizer().normalize(urlString));
        this.domainID = domainID;
        
        // truncate long anchors
        if (anchorText.length() > MAX_ANCHOR_LENGTH)
          anchorText = anchorText.substring(0, MAX_ANCHOR_LENGTH);

        this.anchor = new UTF8(anchorText);
        this.targetHasOutlink = false;
    }

    /**
     * Read in fields from a bytestream
     */
    public void readFields(DataInput in) throws IOException {
        byte version = in.readByte();
        
        if (version > CUR_VERSION)
          throw new VersionMismatchException(CUR_VERSION, version);

        if (fromID == null)
          fromID = new MD5Hash();
        fromID.readFields(in);

        if (url == null)
          url = new UTF8();
        url.readFields(in);

        // 'domainID' was addded in Version 4
        domainID = (version > 4) ? in.readLong() : 0;
        
        if (anchor == null)
          anchor = new UTF8();
        anchor.readFields(in);

        // 'targetHasOutlink' added in Version 3.
        targetHasOutlink = (version > 3) ? in.readBoolean() : false;
    }

    /**
     */
    public void set(Link that) {
        this.fromID.set(that.fromID);
        this.url.set(that.url);
        this.domainID = that.getDomainID();
        this.anchor.set(that.anchor);
        this.targetHasOutlink = that.targetHasOutlink;
    }

    /**
     * Write bytes out to stream
     */
    public void write(DataOutput out) throws IOException {
        out.write(CUR_VERSION);
        fromID.write(out);
        url.write(out);
        out.writeLong(domainID);
        anchor.write(out);
        out.writeBoolean(targetHasOutlink);
    }

    public static Link read(DataInput in) throws IOException {
        Link lr = new Link();
        lr.readFields(in);
        return lr;
    }

    //
    // Accessors
    //
    public MD5Hash getFromID() {
        return fromID;
    }
    public UTF8 getURL() {
        return url;
    }
    public long getDomainID() {
        return domainID;
    }
    public UTF8 getAnchorText() {
        return anchor;
    }
    public boolean targetHasOutlink() {
        return targetHasOutlink;
    }
    public void setTargetHasOutlink(boolean targetHasOutlink) {
        this.targetHasOutlink = targetHasOutlink;
    }

    /**
     * Print out the record
     */
    public String toString() {
        StringBuffer buf = new StringBuffer();
        buf.append("Version: " + CUR_VERSION + "\n");
        buf.append("ID: " + getFromID() + "\n");
        buf.append("DomainID: " + getDomainID() + "\n");
        buf.append("URL: " + getURL() + "\n");
        buf.append("AnchorText: " + getAnchorText() + "\n");
        buf.append("targetHasOutlink: " + targetHasOutlink() + "\n");
        return buf.toString();
    }

    /**
     * Get a tab-delimited version of the text data.
     */
    public String toTabbedString() {
        StringBuffer buf = new StringBuffer();
        buf.append("" + CUR_VERSION); buf.append("\t");
        buf.append(getFromID().toString()); buf.append("\t");
        buf.append(getDomainID()); buf.append("\t");
        buf.append(getURL()); buf.append("\t");
        buf.append(getAnchorText()); buf.append("\t");
        buf.append(targetHasOutlink()); buf.append("\t");

        return buf.toString();
    }

    /**
     */
    public int compareTo(Object o) {
        return urlCompare(o);
    }

    /**
     * Compare URLs, then compare MD5s.
     */
    public int urlCompare(Object o) {
        int urlResult = this.url.compareTo(((Link) o).url);
        if (urlResult != 0) {
            return urlResult;
        }

        return this.fromID.compareTo(((Link) o).fromID);
    }

    /**
     * Compare MD5s, then compare URLs.
     */
    public int md5Compare(Object o) {
        int md5Result = this.fromID.compareTo(((Link) o).fromID);
        if (md5Result != 0) {
            return md5Result;
        }

        return this.url.compareTo(((Link) o).url);
    }

    /**
     * URLComparator uses the standard method where, uh,
     * the URL comes first.
     */
    public static class UrlComparator extends WritableComparator {
        public UrlComparator() {
            super(Link.class);
        }
        public int compare(WritableComparable a, WritableComparable b) {
            return ((Link) a).urlCompare(b);
        }

        /** Optimized comparator. */
        public int compare(byte[] b1, int s1, int l1,
                           byte[] b2, int s2, int l2) {
          int md5Start1 = s1 + 1;                 // skip version
          int md5Start2 = s2 + 1;
          int urlLenStart1 = md5Start1 + MD5Hash.MD5_LEN;
          int urlLenStart2 = md5Start2 + MD5Hash.MD5_LEN;
          int urlLen1 = readUnsignedShort(b1, urlLenStart1);
          int urlLen2 = readUnsignedShort(b2, urlLenStart2);
          int urlStart1 = urlLenStart1+2;
          int urlStart2 = urlLenStart2+2;
          // compare urls
          int c = compareBytes(b1, urlStart1, urlLen1, b2, urlStart2, urlLen2);
          if (c != 0)
            return c;
          // compare md5s
          return compareBytes(b1, md5Start1, MD5Hash.MD5_LEN,
                              b2, md5Start2, MD5Hash.MD5_LEN);
        }
    }

    /**
     * MD5Comparator is the opposite.
     */
    public static class MD5Comparator extends WritableComparator {
        public MD5Comparator() {
            super(Link.class);
        }
        public int compare(WritableComparable a, WritableComparable b) {
            return ((Link) a).md5Compare(b);
        }
      
        /** Optimized comparator. */
        public int compare(byte[] b1, int s1, int l1,
                           byte[] b2, int s2, int l2) {
          // compare md5s
          int md5Start1 = s1 + 1;                 // skip version
          int md5Start2 = s2 + 1;
          int c = compareBytes(b1, md5Start1, MD5Hash.MD5_LEN,
                               b2, md5Start2, MD5Hash.MD5_LEN);
          if (c != 0)
            return c;

          // compare urls
          int urlLenStart1 = md5Start1 + MD5Hash.MD5_LEN;
          int urlLenStart2 = md5Start2 + MD5Hash.MD5_LEN;
          int urlLen1 = readUnsignedShort(b1, urlLenStart1);
          int urlLen2 = readUnsignedShort(b2, urlLenStart2);
          int urlStart1 = urlLenStart1+2;
          int urlStart2 = urlLenStart2+2;
          return compareBytes(b1, urlStart1, urlLen1, b2, urlStart2, urlLen2);
        }
    }
}


