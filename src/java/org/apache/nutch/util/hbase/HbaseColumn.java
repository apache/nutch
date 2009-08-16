package org.apache.nutch.util.hbase;

public class HbaseColumn {
  private byte[] family;
  private byte[] qualifier;
  private int versions;

  public HbaseColumn(byte[] family) {
    this(family, null, 1);
  }

  public HbaseColumn(byte[] family, byte[] qualifier) {
    this(family, qualifier, 1);
  }

  public HbaseColumn(byte[] family, byte[] qualifier, int versions) {
    this.family = family;
    this.qualifier = qualifier;
    this.versions = versions;
  }

  public byte[] getFamily() {
    return family;
  }

  public byte[] getQualifier() {
    return qualifier;
  }

  public int getVersions() {
    return versions;
  }

}
