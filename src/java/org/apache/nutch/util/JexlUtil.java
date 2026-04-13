/*
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
package org.apache.nutch.util;

import java.lang.invoke.MethodHandles;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlFeatures;
import org.apache.commons.jexl3.JexlScript;
import org.apache.commons.jexl3.introspection.JexlSandbox;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.hadoop.conf.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility methods for handling JEXL expressions used in crawl and index
 * pipelines. Expressions are evaluated under a {@link JexlSandbox} with
 * {@link JexlFeatures#newInstance(boolean)} disabled so arbitrary classes cannot
 * be instantiated from user-supplied configuration.
 */
public class JexlUtil {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  /**
   * When {@code true}, JEXL parsing skips the sandbox (unsafe). For trusted
   * environments only; not recommended.
   */
  public static final String DISABLE_SANDBOX_KEY = "nutch.jexl.disable-sandbox";

  /** Supported format for date parsing yyyy-MM-ddTHH:mm:ssZ */
  private static final Pattern DATE_PATTERN = Pattern
      .compile("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}Z");

  /**
   * Classes and interfaces that may be introspected when evaluating Nutch JEXL
   * scripts. Default-deny sandbox: anything not listed is blocked.
   */
  private static final String[] SANDBOX_ALLOW_CLASSES = {
      "java.lang.String",
      "java.lang.Boolean",
      "java.lang.Byte",
      "java.lang.Character",
      "java.lang.Short",
      "java.lang.Integer",
      "java.lang.Long",
      "java.lang.Float",
      "java.lang.Double",
      "java.lang.Number",
      "java.lang.Math",
      "java.lang.Comparable",
      "java.lang.CharSequence",
      "java.util.Map",
      "java.util.List",
      "java.util.Collection",
      "java.util.Set",
      "java.util.SortedMap",
      "java.util.SortedSet",
      "java.util.Iterator",
      "java.lang.Iterable",
      "java.util.AbstractList",
      "java.util.AbstractCollection",
      "java.util.AbstractMap",
      "java.util.AbstractSet",
      "java.util.ArrayList",
      "java.util.LinkedList",
      "java.util.HashMap",
      "java.util.LinkedHashMap",
      "java.util.HashSet",
      "java.util.LinkedHashSet",
      "java.util.TreeMap",
      "java.util.TreeSet",
      "java.util.Collections",
      "java.util.Arrays",
      "java.util.regex.Pattern",
      "java.util.regex.Matcher",
      "org.apache.commons.jexl3.MapContext",
      "org.apache.nutch.indexer.NutchDocument",
      "org.apache.nutch.indexer.NutchField",
  };

  private static volatile JexlEngine sandboxedEngine;
  private static volatile JexlEngine legacyEngine;

  private JexlUtil() {
  }

  private static JexlSandbox createSandbox() {
    JexlSandbox sandbox = new JexlSandbox(false);
    for (String name : SANDBOX_ALLOW_CLASSES) {
      sandbox.allow(name);
    }
    return sandbox;
  }

  private static JexlFeatures createFeatures() {
    return new JexlFeatures(JexlFeatures.createDefault()).newInstance(false);
  }

  private static JexlEngine getSandboxedEngine() {
    if (sandboxedEngine == null) {
      synchronized (JexlUtil.class) {
        if (sandboxedEngine == null) {
          sandboxedEngine = new JexlBuilder().silent(true).strict(true)
              .sandbox(createSandbox()).features(createFeatures()).create();
        }
      }
    }
    return sandboxedEngine;
  }

  private static JexlEngine getLegacyEngine() {
    if (legacyEngine == null) {
      synchronized (JexlUtil.class) {
        if (legacyEngine == null) {
          legacyEngine = new JexlBuilder().silent(true).strict(true).create();
        }
      }
    }
    return legacyEngine;
  }

  private static JexlEngine engineFor(Configuration conf) {
    if (conf != null && conf.getBoolean(DISABLE_SANDBOX_KEY, false)) {
      LOG.warn("{}=true: JEXL sandbox is disabled; only use in fully trusted environments.",
          DISABLE_SANDBOX_KEY);
      return getLegacyEngine();
    }
    return getSandboxedEngine();
  }

  /**
   * Parses a JEXL expression using the default (sandboxed) engine. Use
   * {@link #parseExpression(Configuration, String)} when a {@link Configuration}
   * is available so {@link #DISABLE_SANDBOX_KEY} can be honored.
   *
   * @param expr string JEXL expression
   * @return parsed JEXL expression or null in case of parse error
   */
  public static JexlScript parseExpression(String expr) {
    return parseExpression(null, expr);
  }

  /**
   * Parses a JEXL expression. Unless {@link #DISABLE_SANDBOX_KEY} is set to
   * {@code true} in {@code conf}, the expression is parsed for execution under
   * a restrictive sandbox.
   *
   * @param conf Hadoop configuration, or null to always use the sandbox
   * @param expr string JEXL expression
   * @return parsed JEXL expression or null in case of parse error
   */
  public static JexlScript parseExpression(Configuration conf, String expr) {
    if (expr == null) {
      return null;
    }

    try {
      // Translate any date object into a long. Dates must be in the DATE_PATTERN
      // format. For example: 2016-03-20T00:00:00Z
      Matcher matcher = DATE_PATTERN.matcher(expr);

      if (matcher.find()) {
        String date = matcher.group();

        // parse the matched substring and get the epoch
        Date parsedDate = DateUtils.parseDateStrictly(date,
            new String[] { "yyyy-MM-dd'T'HH:mm:ss'Z'" });
        long time = parsedDate.getTime();

        // replace the original string date with the numeric value
        expr = expr.replace(date, Long.toString(time));
      }

      return engineFor(conf).createScript(expr);
    } catch (Exception e) {
      LOG.error(e.getMessage());
    }

    return null;
  }
}
