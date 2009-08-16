package org.apache.nutch.plugin;

import java.util.Collection;
import java.util.HashSet;

import org.apache.nutch.util.hbase.HbaseColumn;

public interface TablePluggable extends Pluggable {

  public static final Collection<HbaseColumn> EMPTY_COLUMNS =
    new HashSet<HbaseColumn>();
  
  public Collection<HbaseColumn> getColumns();

}
