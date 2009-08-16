package org.apache.nutch.util.hbase;

import org.apache.hadoop.hbase.util.Bytes;

public interface WebTableColumns {
  public static final String BASE_URL_STR         = "bas";
  public static final String STATUS_STR           = "stt";
  public static final String FETCH_TIME_STR       = "fcht";
  public static final String RETRIES_STR          = "rtrs";
  public static final String FETCH_INTERVAL_STR   = "fchi";
  public static final String SCORE_STR            = "scr";
  public static final String MODIFIED_TIME_STR    = "modt";
  public static final String SIGNATURE_STR        = "sig";
  public static final String CONTENT_STR          = "cnt";
  public static final String CONTENT_TYPE_STR     = "cnttyp:";
  public static final String TITLE_STR            = "ttl:";
  public static final String OUTLINKS_STR         = "olnk:";
  public static final String INLINKS_STR          = "ilnk:";
  public static final String PARSE_STATUS_STR     = "prsstt:";
  public static final String PROTOCOL_STATUS_STR  = "prtstt:";
  public static final String TEXT_STR             = "txt:";
  public static final String REPR_URL_STR         = "repr:";
  public static final String HEADERS_STR          = "hdrs:";
  public static final String METADATA_STR         = "mtdt:";

  // Hackish solution to access previous versions of some columns
  public static final String PREV_SIGNATURE_STR      = "prvsig";
  public static final String PREV_FETCH_TIME_STR     = "prvfch";

  @ColumnDescriptor
  public static final byte[] BASE_URL          = Bytes.toBytes(BASE_URL_STR);
  @ColumnDescriptor
  public static final byte[] STATUS            = Bytes.toBytes(STATUS_STR);
  @ColumnDescriptor
  public static final byte[] FETCH_TIME        = Bytes.toBytes(FETCH_TIME_STR);
  @ColumnDescriptor
  public static final byte[] RETRIES           = Bytes.toBytes(RETRIES_STR);
  @ColumnDescriptor
  public static final byte[] FETCH_INTERVAL    = Bytes.toBytes(FETCH_INTERVAL_STR);
  @ColumnDescriptor
  public static final byte[] SCORE             = Bytes.toBytes(SCORE_STR);
  @ColumnDescriptor
  public static final byte[] MODIFIED_TIME     = Bytes.toBytes(MODIFIED_TIME_STR);
  @ColumnDescriptor
  public static final byte[] SIGNATURE         = Bytes.toBytes(SIGNATURE_STR);
  @ColumnDescriptor
  public static final byte[] CONTENT           = Bytes.toBytes(CONTENT_STR);
  @ColumnDescriptor
  public static final byte[] CONTENT_TYPE      = Bytes.toBytes(CONTENT_TYPE_STR);
  @ColumnDescriptor
  public static final byte[] TITLE             = Bytes.toBytes(TITLE_STR);
  @ColumnDescriptor
  public static final byte[] OUTLINKS          = Bytes.toBytes(OUTLINKS_STR);
  @ColumnDescriptor
  public static final byte[] INLINKS           = Bytes.toBytes(INLINKS_STR);
  @ColumnDescriptor
  public static final byte[] PARSE_STATUS      = Bytes.toBytes(PARSE_STATUS_STR);
  @ColumnDescriptor
  public static final byte[] PROTOCOL_STATUS   = Bytes.toBytes(PROTOCOL_STATUS_STR);
  @ColumnDescriptor
  public static final byte[] TEXT              = Bytes.toBytes(TEXT_STR);
  @ColumnDescriptor
  public static final byte[] REPR_URL          = Bytes.toBytes(REPR_URL_STR);
  @ColumnDescriptor
  public static final byte[] HEADERS           = Bytes.toBytes(HEADERS_STR);
  @ColumnDescriptor
  public static final byte[] METADATA          = Bytes.toBytes(METADATA_STR);

  // Hackish solution to access previous versions of some columns
  @ColumnDescriptor
  public static final byte[] PREV_SIGNATURE     = Bytes.toBytes(PREV_SIGNATURE_STR);
  @ColumnDescriptor
  public static final byte[] PREV_FETCH_TIME    = Bytes.toBytes(PREV_FETCH_TIME_STR);

}
