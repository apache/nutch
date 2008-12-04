package org.apache.nutch.indexer.field;

/**
 * The different types of fields. Different types of fields will be handled by
 * different FieldFilter implementations during indexing.
 */
public enum FieldType {
  
  CONTENT,
  BOOST,
  COMPUTATION,
  ACTION;
  
}
