package org.apache.nutch.indexer.field;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/** 
 * A class that holds a single field of content to be placed into an index.
 * 
 * This class has options type of content as well as for how the field is to 
 * be indexed.
 */
public class FieldWritable
  implements Writable {

  private String name;
  private String value;
  private FieldType type = FieldType.CONTENT;
  private float boost;
  private boolean indexed = true;
  private boolean stored = false;
  private boolean tokenized = true;

  public FieldWritable() {

  }

  public FieldWritable(String name, String value, FieldType type, float boost) {
    this(name, value, type, boost, true, false, true);
  }

  public FieldWritable(String name, String value, FieldType type,
    boolean indexed, boolean stored, boolean tokenized) {
    this(name, value, type, 0.0f, indexed, stored, tokenized);
  }

  public FieldWritable(String name, String value, FieldType type, float boost,
    boolean indexed, boolean stored, boolean tokenized) {
    this.name = name;
    this.value = value;
    this.type = type;
    this.boost = boost;
    this.indexed = indexed;
    this.stored = stored;
    this.tokenized = tokenized;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }

  public FieldType getType() {
    return type;
  }

  public void setType(FieldType type) {
    this.type = type;
  }

  public float getBoost() {
    return boost;
  }

  public void setBoost(float boost) {
    this.boost = boost;
  }

  public boolean isIndexed() {
    return indexed;
  }

  public void setIndexed(boolean indexed) {
    this.indexed = indexed;
  }

  public boolean isStored() {
    return stored;
  }

  public void setStored(boolean stored) {
    this.stored = stored;
  }

  public boolean isTokenized() {
    return tokenized;
  }

  public void setTokenized(boolean tokenized) {
    this.tokenized = tokenized;
  }

  public void readFields(DataInput in)
    throws IOException {
    name = Text.readString(in);
    value = Text.readString(in);
    type = FieldType.valueOf(Text.readString(in));
    boost = in.readFloat();
    indexed = in.readBoolean();
    stored = in.readBoolean();
    tokenized = in.readBoolean();
  }

  public void write(DataOutput out)
    throws IOException {
    Text.writeString(out, name);
    Text.writeString(out, value);
    Text.writeString(out, type.toString());
    out.writeFloat(boost);
    out.writeBoolean(indexed);
    out.writeBoolean(stored);
    out.writeBoolean(tokenized);
  }

}
