/**
 *Licensed to the Apache Software Foundation (ASF) under one
 *or more contributor license agreements.  See the NOTICE file
 *distributed with this work for additional information
 *regarding copyright ownership.  The ASF licenses this file
 *to you under the Apache License, Version 2.0 (the"
 *License"); you may not use this file except in compliance
 *with the License.  You may obtain a copy of the License at
 *
  * http://www.apache.org/licenses/LICENSE-2.0
 * 
 *Unless required by applicable law or agreed to in writing, software
 *distributed under the License is distributed on an "AS IS" BASIS,
 *WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *See the License for the specific language governing permissions and
 *limitations under the License.
 */
package org.apache.nutch.storage;  

/** A Duplicate is a data structure in Nutch representing crawl data for urls whose content is identical. */
public class Duplicate extends org.apache.gora.persistency.impl.PersistentBase implements org.apache.avro.specific.SpecificRecord, org.apache.gora.persistency.Persistent {
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Duplicate\",\"namespace\":\"org.apache.nutch.storage\",\"doc\":\"A Duplicate is a data structure in Nutch representing crawl data for urls whose content is identical.\",\"fields\":[{\"name\":\"urls\",\"type\":{\"type\":\"array\",\"items\":\"string\"},\"doc\":\"The web pages with identical content.\",\"default\":[]}]}");
  private static final long serialVersionUID = 7971980383643642679L;
  /** Enum containing all data bean's fields. */
  public static enum Field {
    URLS(0, "urls"),
    ;
    /**
     * Field's index.
     */
    private int index;

    /**
     * Field's name.
     */
    private String name;

    /**
     * Field's constructor
     * @param index field's index.
     * @param name field's name.
     */
    Field(int index, String name) {this.index=index;this.name=name;}

    /**
     * Gets field's index.
     * @return int field's index.
     */
    public int getIndex() {return index;}

    /**
     * Gets field's name.
     * @return String field's name.
     */
    public String getName() {return name;}

    /**
     * Gets field's attributes to string.
     * @return String field's attributes to string.
     */
    public String toString() {return name;}
  };

  public static final String[] _ALL_FIELDS = {
  "urls",
  };

  /**
   * Gets the total field count.
   * @return int field count
   */
  public int getFieldsCount() {
    return Duplicate._ALL_FIELDS.length;
  }

  /** The web pages with identical content. */
  private java.util.List<java.lang.CharSequence> urls;
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return urls;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value) {
    switch (field$) {
    case 0: urls = (java.util.List<java.lang.CharSequence>)((value instanceof org.apache.gora.persistency.Dirtyable) ? value : new org.apache.gora.persistency.impl.DirtyListWrapper((java.util.List)value)); break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'urls' field.
   * The web pages with identical content.   */
  public java.util.List<java.lang.CharSequence> getUrls() {
    return urls;
  }

  /**
   * Sets the value of the 'urls' field.
   * The web pages with identical content.   * @param value the value to set.
   */
  public void setUrls(java.util.List<java.lang.CharSequence> value) {
    this.urls = (value instanceof org.apache.gora.persistency.Dirtyable) ? value : new org.apache.gora.persistency.impl.DirtyListWrapper(value);
    setDirty(0);
  }
  
  /**
   * Checks the dirty status of the 'urls' field. A field is dirty if it represents a change that has not yet been written to the database.
   * The web pages with identical content.   * @param value the value to set.
   */
  public boolean isUrlsDirty() {
    return isDirty(0);
  }

  /** Creates a new Duplicate RecordBuilder */
  public static org.apache.nutch.storage.Duplicate.Builder newBuilder() {
    return new org.apache.nutch.storage.Duplicate.Builder();
  }
  
  /** Creates a new Duplicate RecordBuilder by copying an existing Builder */
  public static org.apache.nutch.storage.Duplicate.Builder newBuilder(org.apache.nutch.storage.Duplicate.Builder other) {
    return new org.apache.nutch.storage.Duplicate.Builder(other);
  }
  
  /** Creates a new Duplicate RecordBuilder by copying an existing Duplicate instance */
  public static org.apache.nutch.storage.Duplicate.Builder newBuilder(org.apache.nutch.storage.Duplicate other) {
    return new org.apache.nutch.storage.Duplicate.Builder(other);
  }
  
  private static java.nio.ByteBuffer deepCopyToReadOnlyBuffer(
      java.nio.ByteBuffer input) {
    java.nio.ByteBuffer copy = java.nio.ByteBuffer.allocate(input.capacity());
    int position = input.position();
    input.reset();
    int mark = input.position();
    int limit = input.limit();
    input.rewind();
    input.limit(input.capacity());
    copy.put(input);
    input.rewind();
    copy.rewind();
    input.position(mark);
    input.mark();
    copy.position(mark);
    copy.mark();
    input.position(position);
    copy.position(position);
    input.limit(limit);
    copy.limit(limit);
    return copy.asReadOnlyBuffer();
  }
  
  /**
   * RecordBuilder for Duplicate instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<Duplicate>
    implements org.apache.avro.data.RecordBuilder<Duplicate> {

    private java.util.List<java.lang.CharSequence> urls;

    /** Creates a new Builder */
    private Builder() {
      super(org.apache.nutch.storage.Duplicate.SCHEMA$);
    }
    
    /** Creates a Builder by copying an existing Builder */
    private Builder(org.apache.nutch.storage.Duplicate.Builder other) {
      super(other);
    }
    
    /** Creates a Builder by copying an existing Duplicate instance */
    private Builder(org.apache.nutch.storage.Duplicate other) {
            super(org.apache.nutch.storage.Duplicate.SCHEMA$);
      if (isValidValue(fields()[0], other.urls)) {
        this.urls = (java.util.List<java.lang.CharSequence>) data().deepCopy(fields()[0].schema(), other.urls);
        fieldSetFlags()[0] = true;
      }
    }

    /** Gets the value of the 'urls' field */
    public java.util.List<java.lang.CharSequence> getUrls() {
      return urls;
    }
    
    /** Sets the value of the 'urls' field */
    public org.apache.nutch.storage.Duplicate.Builder setUrls(java.util.List<java.lang.CharSequence> value) {
      validate(fields()[0], value);
      this.urls = value;
      fieldSetFlags()[0] = true;
      return this; 
    }
    
    /** Checks whether the 'urls' field has been set */
    public boolean hasUrls() {
      return fieldSetFlags()[0];
    }
    
    /** Clears the value of the 'urls' field */
    public org.apache.nutch.storage.Duplicate.Builder clearUrls() {
      urls = null;
      fieldSetFlags()[0] = false;
      return this;
    }
    
    @Override
    public Duplicate build() {
      try {
        Duplicate record = new Duplicate();
        record.urls = fieldSetFlags()[0] ? this.urls : (java.util.List<java.lang.CharSequence>) new org.apache.gora.persistency.impl.DirtyListWrapper((java.util.List)defaultValue(fields()[0]));
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }
  
  public Duplicate.Tombstone getTombstone(){
  	return TOMBSTONE;
  }

  public Duplicate newInstance(){
    return newBuilder().build();
  }

  private static final Tombstone TOMBSTONE = new Tombstone();
  
  public static final class Tombstone extends Duplicate implements org.apache.gora.persistency.Tombstone {
  
      private Tombstone() { }
  
	  		  /**
	   * Gets the value of the 'urls' field.
	   * The web pages with identical content.	   */
	  public java.util.List<java.lang.CharSequence> getUrls() {
	    throw new java.lang.UnsupportedOperationException("Get is not supported on tombstones");
	  }
	
	  /**
	   * Sets the value of the 'urls' field.
	   * The web pages with identical content.	   * @param value the value to set.
	   */
	  public void setUrls(java.util.List<java.lang.CharSequence> value) {
	    throw new java.lang.UnsupportedOperationException("Set is not supported on tombstones");
	  }
	  
	  /**
	   * Checks the dirty status of the 'urls' field. A field is dirty if it represents a change that has not yet been written to the database.
	   * The web pages with identical content.	   * @param value the value to set.
	   */
	  public boolean isUrlsDirty() {
	    throw new java.lang.UnsupportedOperationException("IsDirty is not supported on tombstones");
	  }
	
		  
  }

  private static final org.apache.avro.io.DatumWriter
            DATUM_WRITER$ = new org.apache.avro.specific.SpecificDatumWriter(SCHEMA$);
  private static final org.apache.avro.io.DatumReader
            DATUM_READER$ = new org.apache.avro.specific.SpecificDatumReader(SCHEMA$);

  /**
   * Writes AVRO data bean to output stream in the form of AVRO Binary encoding format. This will transform
   * AVRO data bean from its Java object form to it s serializable form.
   *
   * @param out java.io.ObjectOutput output stream to write data bean in serializable form
   */
  @Override
  public void writeExternal(java.io.ObjectOutput out)
          throws java.io.IOException {
    out.write(super.getDirtyBytes().array());
    DATUM_WRITER$.write(this, org.apache.avro.io.EncoderFactory.get()
            .directBinaryEncoder((java.io.OutputStream) out,
                    null));
  }

  /**
   * Reads AVRO data bean from input stream in it s AVRO Binary encoding format to Java object format.
   * This will transform AVRO data bean from it s serializable form to deserialized Java object form.
   *
   * @param in java.io.ObjectOutput input stream to read data bean in serializable form
   */
  @Override
  public void readExternal(java.io.ObjectInput in)
          throws java.io.IOException {
    byte[] __g__dirty = new byte[getFieldsCount()];
    in.read(__g__dirty);
    super.setDirtyBytes(java.nio.ByteBuffer.wrap(__g__dirty));
    DATUM_READER$.read(this, org.apache.avro.io.DecoderFactory.get()
            .directBinaryDecoder((java.io.InputStream) in,
                    null));
  }
  
}

