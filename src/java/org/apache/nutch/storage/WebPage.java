package org.apache.nutch.storage;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.HashMap;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Protocol;
import org.apache.avro.util.Utf8;
import org.apache.avro.ipc.AvroRemoteException;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.specific.SpecificExceptionBase;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificFixed;
import org.apache.gora.persistency.StateManager;
import org.apache.gora.persistency.impl.PersistentBase;
import org.apache.gora.persistency.impl.StateManagerImpl;
import org.apache.gora.persistency.StatefulHashMap;
import org.apache.gora.persistency.ListGenericArray;

@SuppressWarnings("all")
public class WebPage extends PersistentBase {
  public static final Schema _SCHEMA = Schema.parse("{\"type\":\"record\",\"name\":\"WebPage\",\"namespace\":\"org.apache.nutch.storage\",\"fields\":[{\"name\":\"baseUrl\",\"type\":\"string\"},{\"name\":\"status\",\"type\":\"int\"},{\"name\":\"fetchTime\",\"type\":\"long\"},{\"name\":\"prevFetchTime\",\"type\":\"long\"},{\"name\":\"fetchInterval\",\"type\":\"int\"},{\"name\":\"retriesSinceFetch\",\"type\":\"int\"},{\"name\":\"modifiedTime\",\"type\":\"long\"},{\"name\":\"protocolStatus\",\"type\":{\"type\":\"record\",\"name\":\"ProtocolStatus\",\"fields\":[{\"name\":\"code\",\"type\":\"int\"},{\"name\":\"args\",\"type\":{\"type\":\"array\",\"items\":\"string\"}},{\"name\":\"lastModified\",\"type\":\"long\"}]}},{\"name\":\"content\",\"type\":\"bytes\"},{\"name\":\"contentType\",\"type\":\"string\"},{\"name\":\"prevSignature\",\"type\":\"bytes\"},{\"name\":\"signature\",\"type\":\"bytes\"},{\"name\":\"title\",\"type\":\"string\"},{\"name\":\"text\",\"type\":\"string\"},{\"name\":\"parseStatus\",\"type\":{\"type\":\"record\",\"name\":\"ParseStatus\",\"fields\":[{\"name\":\"majorCode\",\"type\":\"int\"},{\"name\":\"minorCode\",\"type\":\"int\"},{\"name\":\"args\",\"type\":{\"type\":\"array\",\"items\":\"string\"}}]}},{\"name\":\"score\",\"type\":\"float\"},{\"name\":\"reprUrl\",\"type\":\"string\"},{\"name\":\"headers\",\"type\":{\"type\":\"map\",\"values\":\"string\"}},{\"name\":\"outlinks\",\"type\":{\"type\":\"map\",\"values\":\"string\"}},{\"name\":\"inlinks\",\"type\":{\"type\":\"map\",\"values\":\"string\"}},{\"name\":\"markers\",\"type\":{\"type\":\"map\",\"values\":\"string\"}},{\"name\":\"metadata\",\"type\":{\"type\":\"map\",\"values\":\"bytes\"}}]}");
  public static enum Field {
    BASE_URL(0,"baseUrl"),
    STATUS(1,"status"),
    FETCH_TIME(2,"fetchTime"),
    PREV_FETCH_TIME(3,"prevFetchTime"),
    FETCH_INTERVAL(4,"fetchInterval"),
    RETRIES_SINCE_FETCH(5,"retriesSinceFetch"),
    MODIFIED_TIME(6,"modifiedTime"),
    PROTOCOL_STATUS(7,"protocolStatus"),
    CONTENT(8,"content"),
    CONTENT_TYPE(9,"contentType"),
    PREV_SIGNATURE(10,"prevSignature"),
    SIGNATURE(11,"signature"),
    TITLE(12,"title"),
    TEXT(13,"text"),
    PARSE_STATUS(14,"parseStatus"),
    SCORE(15,"score"),
    REPR_URL(16,"reprUrl"),
    HEADERS(17,"headers"),
    OUTLINKS(18,"outlinks"),
    INLINKS(19,"inlinks"),
    MARKERS(20,"markers"),
    METADATA(21,"metadata"),
    ;
    private int index;
    private String name;
    Field(int index, String name) {this.index=index;this.name=name;}
    public int getIndex() {return index;}
    public String getName() {return name;}
    public String toString() {return name;}
  };
  public static final String[] _ALL_FIELDS = {"baseUrl","status","fetchTime","prevFetchTime","fetchInterval","retriesSinceFetch","modifiedTime","protocolStatus","content","contentType","prevSignature","signature","title","text","parseStatus","score","reprUrl","headers","outlinks","inlinks","markers","metadata",};
  static {
    PersistentBase.registerFields(WebPage.class, _ALL_FIELDS);
  }
  private Utf8 baseUrl;
  private int status;
  private long fetchTime;
  private long prevFetchTime;
  private int fetchInterval;
  private int retriesSinceFetch;
  private long modifiedTime;
  private ProtocolStatus protocolStatus;
  private ByteBuffer content;
  private Utf8 contentType;
  private ByteBuffer prevSignature;
  private ByteBuffer signature;
  private Utf8 title;
  private Utf8 text;
  private ParseStatus parseStatus;
  private float score;
  private Utf8 reprUrl;
  private Map<Utf8,Utf8> headers;
  private Map<Utf8,Utf8> outlinks;
  private Map<Utf8,Utf8> inlinks;
  private Map<Utf8,Utf8> markers;
  private Map<Utf8,ByteBuffer> metadata;
  public WebPage() {
    this(new StateManagerImpl());
  }
  public WebPage(StateManager stateManager) {
    super(stateManager);
    headers = new StatefulHashMap<Utf8,Utf8>();
    outlinks = new StatefulHashMap<Utf8,Utf8>();
    inlinks = new StatefulHashMap<Utf8,Utf8>();
    markers = new StatefulHashMap<Utf8,Utf8>();
    metadata = new StatefulHashMap<Utf8,ByteBuffer>();
  }
  public WebPage newInstance(StateManager stateManager) {
    return new WebPage(stateManager);
  }
  public Schema getSchema() { return _SCHEMA; }
  public Object get(int _field) {
    switch (_field) {
    case 0: return baseUrl;
    case 1: return status;
    case 2: return fetchTime;
    case 3: return prevFetchTime;
    case 4: return fetchInterval;
    case 5: return retriesSinceFetch;
    case 6: return modifiedTime;
    case 7: return protocolStatus;
    case 8: return content;
    case 9: return contentType;
    case 10: return prevSignature;
    case 11: return signature;
    case 12: return title;
    case 13: return text;
    case 14: return parseStatus;
    case 15: return score;
    case 16: return reprUrl;
    case 17: return headers;
    case 18: return outlinks;
    case 19: return inlinks;
    case 20: return markers;
    case 21: return metadata;
    default: throw new AvroRuntimeException("Bad index");
    }
  }
  @SuppressWarnings(value="unchecked")
  public void put(int _field, Object _value) {
    if(isFieldEqual(_field, _value)) return;
    getStateManager().setDirty(this, _field);
    switch (_field) {
    case 0:baseUrl = (Utf8)_value; break;
    case 1:status = (Integer)_value; break;
    case 2:fetchTime = (Long)_value; break;
    case 3:prevFetchTime = (Long)_value; break;
    case 4:fetchInterval = (Integer)_value; break;
    case 5:retriesSinceFetch = (Integer)_value; break;
    case 6:modifiedTime = (Long)_value; break;
    case 7:protocolStatus = (ProtocolStatus)_value; break;
    case 8:content = (ByteBuffer)_value; break;
    case 9:contentType = (Utf8)_value; break;
    case 10:prevSignature = (ByteBuffer)_value; break;
    case 11:signature = (ByteBuffer)_value; break;
    case 12:title = (Utf8)_value; break;
    case 13:text = (Utf8)_value; break;
    case 14:parseStatus = (ParseStatus)_value; break;
    case 15:score = (Float)_value; break;
    case 16:reprUrl = (Utf8)_value; break;
    case 17:headers = (Map<Utf8,Utf8>)_value; break;
    case 18:outlinks = (Map<Utf8,Utf8>)_value; break;
    case 19:inlinks = (Map<Utf8,Utf8>)_value; break;
    case 20:markers = (Map<Utf8,Utf8>)_value; break;
    case 21:metadata = (Map<Utf8,ByteBuffer>)_value; break;
    default: throw new AvroRuntimeException("Bad index");
    }
  }
  public Utf8 getBaseUrl() {
    return (Utf8) get(0);
  }
  public void setBaseUrl(Utf8 value) {
    put(0, value);
  }
  public int getStatus() {
    return (Integer) get(1);
  }
  public void setStatus(int value) {
    put(1, value);
  }
  public long getFetchTime() {
    return (Long) get(2);
  }
  public void setFetchTime(long value) {
    put(2, value);
  }
  public long getPrevFetchTime() {
    return (Long) get(3);
  }
  public void setPrevFetchTime(long value) {
    put(3, value);
  }
  public int getFetchInterval() {
    return (Integer) get(4);
  }
  public void setFetchInterval(int value) {
    put(4, value);
  }
  public int getRetriesSinceFetch() {
    return (Integer) get(5);
  }
  public void setRetriesSinceFetch(int value) {
    put(5, value);
  }
  public long getModifiedTime() {
    return (Long) get(6);
  }
  public void setModifiedTime(long value) {
    put(6, value);
  }
  public ProtocolStatus getProtocolStatus() {
    return (ProtocolStatus) get(7);
  }
  public void setProtocolStatus(ProtocolStatus value) {
    put(7, value);
  }
  public ByteBuffer getContent() {
    return (ByteBuffer) get(8);
  }
  public void setContent(ByteBuffer value) {
    put(8, value);
  }
  public Utf8 getContentType() {
    return (Utf8) get(9);
  }
  public void setContentType(Utf8 value) {
    put(9, value);
  }
  public ByteBuffer getPrevSignature() {
    return (ByteBuffer) get(10);
  }
  public void setPrevSignature(ByteBuffer value) {
    put(10, value);
  }
  public ByteBuffer getSignature() {
    return (ByteBuffer) get(11);
  }
  public void setSignature(ByteBuffer value) {
    put(11, value);
  }
  public Utf8 getTitle() {
    return (Utf8) get(12);
  }
  public void setTitle(Utf8 value) {
    put(12, value);
  }
  public Utf8 getText() {
    return (Utf8) get(13);
  }
  public void setText(Utf8 value) {
    put(13, value);
  }
  public ParseStatus getParseStatus() {
    return (ParseStatus) get(14);
  }
  public void setParseStatus(ParseStatus value) {
    put(14, value);
  }
  public float getScore() {
    return (Float) get(15);
  }
  public void setScore(float value) {
    put(15, value);
  }
  public Utf8 getReprUrl() {
    return (Utf8) get(16);
  }
  public void setReprUrl(Utf8 value) {
    put(16, value);
  }
  public Map<Utf8, Utf8> getHeaders() {
    return (Map<Utf8, Utf8>) get(17);
  }
  public Utf8 getFromHeaders(Utf8 key) {
    if (headers == null) { return null; }
    return headers.get(key);
  }
  public void putToHeaders(Utf8 key, Utf8 value) {
    getStateManager().setDirty(this, 17);
    headers.put(key, value);
  }
  public Utf8 removeFromHeaders(Utf8 key) {
    if (headers == null) { return null; }
    getStateManager().setDirty(this, 17);
    return headers.remove(key);
  }
  public Map<Utf8, Utf8> getOutlinks() {
    return (Map<Utf8, Utf8>) get(18);
  }
  public Utf8 getFromOutlinks(Utf8 key) {
    if (outlinks == null) { return null; }
    return outlinks.get(key);
  }
  public void putToOutlinks(Utf8 key, Utf8 value) {
    getStateManager().setDirty(this, 18);
    outlinks.put(key, value);
  }
  public Utf8 removeFromOutlinks(Utf8 key) {
    if (outlinks == null) { return null; }
    getStateManager().setDirty(this, 18);
    return outlinks.remove(key);
  }
  public Map<Utf8, Utf8> getInlinks() {
    return (Map<Utf8, Utf8>) get(19);
  }
  public Utf8 getFromInlinks(Utf8 key) {
    if (inlinks == null) { return null; }
    return inlinks.get(key);
  }
  public void putToInlinks(Utf8 key, Utf8 value) {
    getStateManager().setDirty(this, 19);
    inlinks.put(key, value);
  }
  public Utf8 removeFromInlinks(Utf8 key) {
    if (inlinks == null) { return null; }
    getStateManager().setDirty(this, 19);
    return inlinks.remove(key);
  }
  public Map<Utf8, Utf8> getMarkers() {
    return (Map<Utf8, Utf8>) get(20);
  }
  public Utf8 getFromMarkers(Utf8 key) {
    if (markers == null) { return null; }
    return markers.get(key);
  }
  public void putToMarkers(Utf8 key, Utf8 value) {
    getStateManager().setDirty(this, 20);
    markers.put(key, value);
  }
  public Utf8 removeFromMarkers(Utf8 key) {
    if (markers == null) { return null; }
    getStateManager().setDirty(this, 20);
    return markers.remove(key);
  }
  public Map<Utf8, ByteBuffer> getMetadata() {
    return (Map<Utf8, ByteBuffer>) get(21);
  }
  public ByteBuffer getFromMetadata(Utf8 key) {
    if (metadata == null) { return null; }
    return metadata.get(key);
  }
  public void putToMetadata(Utf8 key, ByteBuffer value) {
    getStateManager().setDirty(this, 21);
    metadata.put(key, value);
  }
  public ByteBuffer removeFromMetadata(Utf8 key) {
    if (metadata == null) { return null; }
    getStateManager().setDirty(this, 21);
    return metadata.remove(key);
  }
}
