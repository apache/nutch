package org.apache.nutch.indexer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VersionMismatchException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.nutch.metadata.Metadata;

/** A {@link NutchDocument} is the unit of indexing.*/
public class NutchDocument
implements Writable, Iterable<Entry<String, List<String>>> {

  public static final byte VERSION = 1;

  private Map<String, List<String>> fields;

  private Metadata documentMeta;

  private float score;

  public NutchDocument() {
    fields = new HashMap<String, List<String>>();
    documentMeta = new Metadata();
    score = 0.0f;
  }

  public void add(String name, String value) {
    List<String> fieldValues = fields.get(name);
    if (fieldValues == null) {
      fieldValues = new ArrayList<String>();
    }
    fieldValues.add(value);
    fields.put(name, fieldValues);
  }

  private void addFieldUnprotected(String name, String value) {
    fields.get(name).add(value);
  }

  public String getFieldValue(String name) {
    List<String> fieldValues = fields.get(name);
    if (fieldValues == null) {
      return null;
    }
    if (fieldValues.size() == 0) {
      return null;
    }
    return fieldValues.get(0);
  }

  public List<String> getFieldValues(String name) {
    return fields.get(name);
  }

  public List<String> removeField(String name) {
    return fields.remove(name);
  }

  public Collection<String> getFieldNames() {
    return fields.keySet();
  }

  /** Iterate over all fields. */
  public Iterator<Entry<String, List<String>>> iterator() {
    return fields.entrySet().iterator();
  }

  public float getScore() {
    return score;
  }

  public void setScore(float score) {
    this.score = score;
  }

  public Metadata getDocumentMeta() {
    return documentMeta;
  }

  public void readFields(DataInput in) throws IOException {
    byte version = in.readByte();
    if (version != VERSION) {
      throw new VersionMismatchException(VERSION, version);
    }
    int size = WritableUtils.readVInt(in);
    for (int i = 0; i < size; i++) {
      String name = Text.readString(in);
      int numValues = WritableUtils.readVInt(in);
      fields.put(name, new ArrayList<String>());
      for (int j = 0; j < numValues; j++) {
        String value = Text.readString(in);
        addFieldUnprotected(name, value);
      }
    }
    score = in.readFloat();
    documentMeta.readFields(in);
  }

  public void write(DataOutput out) throws IOException {
    out.writeByte(VERSION);
    WritableUtils.writeVInt(out, fields.size());
    for (Map.Entry<String, List<String>> entry : fields.entrySet()) {
      Text.writeString(out, entry.getKey());
      List<String> values = entry.getValue();
      WritableUtils.writeVInt(out, values.size());
      for (String value : values) {
        Text.writeString(out, value);
      }
    }
    out.writeFloat(score);
    documentMeta.write(out);
  }

}
