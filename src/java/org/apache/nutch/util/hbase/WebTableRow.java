package org.apache.nutch.util.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NavigableMap;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import static org.apache.nutch.util.hbase.WebTableColumns.*;

import org.apache.nutch.crawl.Inlink;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.ParseStatus;
import org.apache.nutch.protocol.ProtocolStatus;

public class WebTableRow extends TableRow {
  public WebTableRow() { // do not use!
    super();
  }
  
  public WebTableRow(byte[] row) {
    super(row);
  }

  public WebTableRow(Result result) {
    super(result);  
  }
  
  private String stringify(byte[] val) {
    if (val == null)
      return null;
    return Bytes.toString(val);
  }

  public String getBaseUrl() {
    return stringify(get(BASE_URL, null));
  }

  public byte getStatus() {
    return get(STATUS, null)[0];
  }

  public byte[] getSignature() {
    return get(SIGNATURE, null);
  }

  public byte[] getPrevSignature() {
    return get(PREV_SIGNATURE, null);
  }

  public long getFetchTime() {
    return Bytes.toLong(get(FETCH_TIME, null));
  }

  public long getPrevFetchTime() {
    byte[] val = get(PREV_FETCH_TIME, null);
    if (val == null)
      return 0L;

    return Bytes.toLong(val);
  }

  public long getModifiedTime() {
    return Bytes.toLong(get(MODIFIED_TIME, null));
  }

  public int getFetchInterval() {
    return Bytes.toInt(get(FETCH_INTERVAL, null));
  }

  public int getRetriesSinceFetch() {
    return Bytes.toInt(get(RETRIES, null));
  }

  public ProtocolStatus getProtocolStatus() throws IOException {
    final ProtocolStatus protocolStatus = new ProtocolStatus();
    final byte[] val = get(PROTOCOL_STATUS, null);
    return (ProtocolStatus) Writables.getWritable(val, protocolStatus);
  }

  public float getScore() {
    return Bytes.toFloat(get(SCORE, null));
  }

  public byte[] getContent() {
    return get(CONTENT, null);
  }

  public String getContentType() {
    return stringify(get(CONTENT_TYPE, null));
  }

  public String getText() {
    return stringify(get(TEXT, null));
  }

  public String getTitle() {
    return stringify(get(TITLE, null));
  }

  public ParseStatus getParseStatus() {
    final ParseStatus parseStatus = new ParseStatus();
    final byte[] val = get(PARSE_STATUS, null);
    try {
      return (ParseStatus) Writables.getWritable(val, parseStatus);
    } catch (final IOException e) {
      return null;
    }
  }

  public String getReprUrl() {
    return stringify(get(REPR_URL, null));
  }
  
  public void setBaseUrl(String baseUrl) {
    put(BASE_URL, null, baseUrl);
  }

  public void setContent(byte[] content) {
    put(CONTENT, null, content);
  }

  public void setContentType(String contentType) {
    put(CONTENT_TYPE, null, contentType);
  }

  public void setFetchInterval(int fetchInterval) {
    put(FETCH_INTERVAL, null, fetchInterval);
  }

  public void setFetchTime(long fetchTime) {
    put(FETCH_TIME, null, fetchTime);
  }

  public void setModifiedTime(long modifiedTime) {
    put(MODIFIED_TIME, null, modifiedTime);
  }

  public void setParseStatus(ParseStatus parseStatus) throws IOException {
    put(PARSE_STATUS, null, Writables.getBytes(parseStatus));
  }

  public void setPrevFetchTime(long prevFetchTime) {
    put(PREV_FETCH_TIME, null, prevFetchTime);
  }

  public void setPrevSignature(byte[] prevSig) {
    put(PREV_SIGNATURE, null, prevSig);
  }

  public void setProtocolStatus(ProtocolStatus protocolStatus)
  throws IOException {
    put(PROTOCOL_STATUS, null, Writables.getBytes(protocolStatus));
  }

  public void setReprUrl(String reprUrl) {
    put(REPR_URL, null, reprUrl);
  }

  public void setRetriesSinceFetch(int retries) {
    put(RETRIES, null, retries);
  }

  public void setScore(float score) {
    put(SCORE, null, score);
  }

  public void setSignature(byte[] signature) {
    put(SIGNATURE, null, signature);
  }

  public void setStatus(byte status) {
    put(STATUS, null, new byte[] { status });
  }

  public void setText(String text) {
    put(TEXT, null, text);
  }

  public void setTitle(String title) {
    put(TITLE, null, title);
  }

  public Collection<Outlink> getOutlinks() {
    final List<Outlink> outlinks = new ArrayList<Outlink>();
    NavigableMap<byte[], NavigableMap<Long, ColumnData>> outlinkMap =
      map.get(OUTLINKS);
    if (outlinkMap == null) {
      return outlinks;
    }
    for (final Entry<byte[], NavigableMap<Long, ColumnData>> e : outlinkMap.entrySet()) {
      final String toUrl = Bytes.toString(e.getKey());
      ColumnData latestColumnData = e.getValue().firstEntry().getValue();
      final String anchor = Bytes.toString(latestColumnData.getData());
      outlinks.add(new Outlink(toUrl, anchor));
    }
    return outlinks;
  }

  public void addOutlink(Outlink outlink) {
    put(OUTLINKS, Bytes.toBytes(outlink.getToUrl()), 
        Bytes.toBytes(outlink.getAnchor()));
  }
  
  public void deleteAllOutlinks() {
    deleteFamily(OUTLINKS);
  }

  public Collection<Inlink> getInlinks() {
    final List<Inlink> inlinks = new ArrayList<Inlink>();
    NavigableMap<byte[], NavigableMap<Long, ColumnData>> inlinkMap =
      map.get(INLINKS);
    if (inlinkMap == null) {
      return inlinks;
    }
    for (final Entry<byte[], NavigableMap<Long, ColumnData>> e : inlinkMap.entrySet()) {
      final String fromUrl = Bytes.toString(e.getKey());
      ColumnData latestColumnData = e.getValue().firstEntry().getValue();
      final String anchor = Bytes.toString(latestColumnData.getData());
      inlinks.add(new Inlink(fromUrl, anchor));
    }

    return inlinks;
  }
  
  public void addInlink(Inlink inlink) {
    put(INLINKS, Bytes.toBytes(inlink.getFromUrl()), 
        Bytes.toBytes(inlink.getAnchor()));
  }
  
  public void deleteAllInlinks() {
    deleteFamily(INLINKS);
  }


  /** Returns a header.
   * @param key Header-key
   * @return headers if it exists, null otherwise
   */
  public String getHeader(String key) {
    return getHeader(Bytes.toBytes(key));
  }
  
  /** Returns a header.
   * @param key Header-key
   * @return headers if it exists, null otherwise
   */
   public String getHeader(byte[] key) {
    byte[] val = get(HEADERS, key);
    return val == null ? null : stringify(val);
  }

  public void addHeader(String key, String value) {
    put(HEADERS, Bytes.toBytes(key), Bytes.toBytes(value));
  }
  
  public void deleteHeaders() {
    deleteFamily(HEADERS);
  }

  /** Checks if a metadata key exists in "metadata" column.
   * @param metaKey Key to search in metadata column
   * @return true if key exists
   */
  public boolean hasMeta(String metaKey) {
    return hasMeta(Bytes.toBytes(metaKey));
  }
  

  /** Checks if a metadata key exists in "metadata" column.
   * @param metaKey Key to search in metadata column
   * @return true if key exists
   */
  public boolean hasMeta(byte[] metaKey) {
    return hasColumn(METADATA, metaKey);
  }

  /** Read a metadata key from "metadata" column.
   * @param metaKey Key to search in metadata column
   * @return Value in byte array form or null if metadata doesn't exist
   */ 
  public byte[] getMeta(String metaKey) {
    return getMeta(Bytes.toBytes(metaKey));
  }
  
  /** Read a metadata key from "metadata" column.
   * @param metaKey Key to search in metadata column
   * @return Value in byte array form or null if metadata doesn't exist
   */ 
  public byte[] getMeta(byte[] metaKey) {
    return get(METADATA, metaKey);
  }

  public String getMetaAsString(String metaKey) {
    final byte[] val = getMeta(metaKey);
    return val == null ? null : Bytes.toString(val);
  }
  
  public void putMeta(String metaKey, byte[] val) {
    putMeta(Bytes.toBytes(metaKey), val);
  }

  public void putMeta(byte[] metaKey, byte[] val) {
    put(METADATA, metaKey, val);
  }
  
  public void deleteMeta(String metaKey) {
    deleteMeta(Bytes.toBytes(metaKey));
  }

  public void deleteMeta(byte[] metaKey) {
    delete(METADATA, metaKey);
  }
}
