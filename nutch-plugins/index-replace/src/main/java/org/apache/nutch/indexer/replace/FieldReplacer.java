/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nutch.indexer.replace;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * POJO to store a filename, its match pattern and its replacement string.
 *
 * A checkAndReplace method is provided where you can simultaneously check if
 * the field matches this replacer and if the pattern matches your field value.
 *
 * @author Peter Ciuffetti
 */
public class FieldReplacer {

  private static final Log LOG = LogFactory.getLog(FieldReplacer.class
      .getName());

  private final String fieldName;
  private final String toFieldName;
  private final Pattern pattern;
  private final String replacement;
  private boolean isValid;

  /**
   * Create a FieldReplacer for a field.
   *
   * Any pattern exceptions are caught within this constructor and the object is
   * marked inValid. The error will be logged. This prevents this caller from
   * attempting invalid replacements.
   *
   * @param fieldName
   *          the name of the source field to operate on. Required.
   * @param toFieldName
   *          the name of the target field. Required.
   * @param pattern
   *          the pattern the field must match. Required.
   * @param replacement
   *          the replacement string
   * @param flags
   *          the Pattern flags value, or null if no flags are needed
   */
  public FieldReplacer(String fieldName, String toFieldName, String pattern,
      String replacement, Integer flags) {

    this.isValid = true;
    // Must have a non-empty field name and pattern.
    if (fieldName == null || fieldName.trim().length() == 0) {
      LOG.error("Empty fieldName provided, FieldReplacer marked invalid.");
      this.isValid = false;
    }
    if (pattern == null || pattern.trim().length() == 0) {
      LOG.error("Empty pattern for field " + fieldName
          + "provided, FieldReplacer marked invalid.");
      this.isValid = false;
    }

    if (replacement == null) {
      this.replacement = "";
    } else {
      this.replacement = replacement;
    }

    this.fieldName = fieldName.trim();
    this.toFieldName = toFieldName.trim();

    if (this.isValid) {
      LOG.info("Compiling pattern " + pattern + " for field " + fieldName);
      Pattern myPattern = null;
      try {
        if (flags != null) {
          myPattern = Pattern.compile(pattern, flags);
        } else {
          myPattern = Pattern.compile(pattern);
        }
      } catch (PatternSyntaxException e) {
        LOG.error("Pattern " + pattern + " for field " + fieldName
            + " failed to compile: " + e.toString());
        this.isValid = false;
      }
      this.pattern = myPattern;
    } else {
      this.pattern = null;
    }
  }

  /**
   * Field replacer with the input and output field the same.
   *
   * @param fieldName
   * @param pattern
   * @param replacement
   * @param flags
   */
  public FieldReplacer(String fieldName, String pattern, String replacement,
      Integer flags) {
    this(fieldName, fieldName, pattern, replacement, flags);
  }

  public String getFieldName() {
    return this.fieldName;
  }

  public String getToFieldName() {
    return this.toFieldName;
  }

  public Pattern getPattern() {
    return this.pattern;
  }

  public String getReplacement() {
    return this.replacement;
  }

  /**
   * Does this FieldReplacer have a valid fieldname and pattern?
   *
   * @return
   */
  public boolean isValid() {
    return this.isValid;
  }

  /**
   * Return the replacement value for a field value.
   *
   * This does not check for a matching field; the caller must decide if this
   * FieldReplacer should operate on this value by checking getFieldName().
   *
   * The method returns the value with the replacement. If the value returned is
   * not different then eiher the pattern didn't match or the replacement was a
   * no-op.
   *
   * @param value
   * @return
   */
  public String replace(String value) {
    if (this.isValid) {
      return this.pattern.matcher(value).replaceAll(replacement);
    } else {
      return value;
    }
  }

  /**
   * Return a replacement value for a field.
   *
   * This is designed to fail fast and trigger a replacement only when
   * necessary. If this method returns null, either the field does not match or
   * the value does not match the pattern (or possibly the pattern is invalid).
   *
   * So only if the method returns a non-null value will you need to replace the
   * value for the field.
   *
   * @param fieldName
   *          the name of the field you are checking
   * @param value
   *          the value of the field you are checking
   * @return a replacement value. If null, either the field does not match or
   *         the value does not match.
   */
  public String checkAndReplace(String fieldName, String value) {
    if (this.fieldName.equals(fieldName)) {
      if (value != null && value.length() > 0) {
        if (this.isValid) {
          Matcher m = this.pattern.matcher(value);
          if (m.find()) {
            return m.replaceAll(this.replacement);
          }
        }
      }
    }
    return null;
  }
}
