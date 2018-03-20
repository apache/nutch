/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the"
 * License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nutch.storage;

import java.util.List;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.data.RecordBuilder;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificRecordBuilderBase;
import org.apache.gora.persistency.Dirtyable;
import org.apache.gora.persistency.Persistent;
import org.apache.gora.persistency.impl.DirtyListWrapper;
import org.apache.gora.persistency.impl.PersistentBase;

/** Duplicate represents a list of urls with matching digest values */
public class Duplicate extends PersistentBase implements SpecificRecord, Persistent {
  public static final Schema SCHEMA$ = new Schema.Parser()
      .parse("{\"type\":\"record\",\"name\":\"Duplicate\",\"namespace\":\"org.apache.nutch.storage\",\"doc\":\"A Duplicate is a data structure in Nutch representing crawl data for urls whose content is identical.\",\"fields\":[{\"name\":\"urls\",\"type\":{\"type\":\"array\",\"items\":\"string\"},\"doc\":\"The web pages with identical content.\"}]}");

  /** Enum containing all data bean's fields. */
  public static enum Field {
    URLS(0, "urls");
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
     * 
     * @param index
     *          field's index.
     * @param name
     *          field's name.
     */
    Field(int index, String name) {
      this.index = index;
      this.name = name;
    }

    /**
     * Gets field's index.
     * 
     * @return int field's index.
     */
    public int getIndex() {
      return index;
    }

    /**
     * Gets field's name.
     * 
     * @return String field's name.
     */
    public String getName() {
      return name;
    }

    /**
     * Gets field's attributes to string.
     * 
     * @return String field's attributes to string.
     */
    public String toString() {
      return name;
    }
  };

  public static final String[] _ALL_FIELDS = { "urls" };

  /**
   * Gets the total field count.
   * 
   * @return int field count
   */
  public int getFieldsCount() {
    return Duplicate._ALL_FIELDS.length;
  }
  
  /** The urls with the same digest. */
  private List<CharSequence> urls;

  public Schema getSchema() {
    return SCHEMA$;
  }

  // Used by DatumWriter. Applications should not call.
  public Object get(int field$) {
    switch (field$) {
    case 0:
      return urls;
    default:
      throw new AvroRuntimeException("Bad index");
    }
  }

  // Used by DatumReader. Applications should not call.
  @SuppressWarnings(value = "unchecked")
  public void put(int field$, Object value) {
    switch (field$) {
    case 0:
      urls = (List<CharSequence>) ((value instanceof Dirtyable) ? value : new DirtyListWrapper<CharSequence>((List<CharSequence>) value));
      break;
    default:
      throw new AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'urls' field. The urls with the same digest
   */
  public List<CharSequence> getURLs() {
    return urls;
  }

  /**
   * Sets the value of the 'urls' field. The urls with the same digest
   * @param value the value to set.
   */
  public void setURLs(List<CharSequence> value) {
    this.urls = value;
    setDirty(0);
  }

  /**
   * Checks the dirty status of the 'urls' field. A field is dirty if it
   * represents a change that has not yet been written to the database.
   * The urls with the same digest
   * @param value the value to set.
   */
  public boolean isURLsDirty(long value) {
    return isDirty(0);
  }

  /** Creates a new Duplicate RecordBuilder */
  public static Builder newBuilder() {
    return new Builder();
  }

  /** Creates a new Duplicate RecordBuilder by copying an existing Builder */
  public static Builder newBuilder(Builder other) {
    return new Builder(other);
  }

  /**
   * Creates a new Duplicate RecordBuilder by copying an existing Duplicate instance
   */
  public static Builder newBuilder(Duplicate other) {
    return new Builder(other);
  }

  /**
   * RecordBuilder for Duplicate instances.
   */
  public static class Builder extends SpecificRecordBuilderBase<Duplicate> implements RecordBuilder<Duplicate> {
    
    private List<CharSequence> urls;

    /** Creates a new Builder */
    private Builder() {
      super(Duplicate.SCHEMA$);
    }

    /** Creates a Builder by copying an existing Builder */
    private Builder(Builder other) {
      super(other);
    }

    /** Creates a Builder by copying an existing Duplicate instance */
    private Builder(Duplicate other) {
      super(Duplicate.SCHEMA$);
      if (isValidValue(fields()[0], other.urls)) {
        this.urls = (List<CharSequence>) data().deepCopy(fields()[0].schema(), other.urls);
        fieldSetFlags()[0] = true;
      }
    }

    /** Gets the value of the 'urls' field */
    public List<CharSequence> getURLs() {
      return urls;
    }

    /** Sets the value of the 'urls' field */
    public Builder setURLs(List<CharSequence> value) {
      validate(fields()[0], value);
      this.urls = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /** Checks whether the 'urls' field has been set */
    public boolean hasURLs() {
      return fieldSetFlags()[0];
    }

    /** Clears the value of the 'urls' field */
    public Builder clearURLs() {
      urls = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    @Override
    @SuppressWarnings(value = "unchecked")
    public Duplicate build() {
      try {
        Duplicate record = new Duplicate();
        record.urls = fieldSetFlags()[0] ? this.urls : (List<CharSequence>) defaultValue(fields()[0]);
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  public Tombstone getTombstone() {
    return TOMBSTONE;
  }

  public Duplicate newInstance() {
    return newBuilder().build();
  }

  private static final Tombstone TOMBSTONE = new Tombstone();

  public static final class Tombstone extends Duplicate implements
      org.apache.gora.persistency.Tombstone {

    private Tombstone() {}
    
    /** Gets the value of the 'urls' field */
    public List<CharSequence> getURLs() {
      throw new UnsupportedOperationException("IsDirty is not supported on tombstones");
    }

    /** Sets the value of the 'urls' field */
    public void setURLs(List<CharSequence> value) {
      throw new UnsupportedOperationException("IsDirty is not supported on tombstones");
    }

    /** Checks the dirty status of the 'urls' field. */
    public boolean isURLsDirty() {
      throw new UnsupportedOperationException("IsDirty is not supported on tombstones");
    }

  }

}
