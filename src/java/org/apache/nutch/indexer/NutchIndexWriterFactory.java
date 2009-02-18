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
package org.apache.nutch.indexer;

import org.apache.hadoop.conf.Configuration;

public class NutchIndexWriterFactory {
  @SuppressWarnings("unchecked")
  public static NutchIndexWriter[] getNutchIndexWriters(Configuration conf) {
    final String[] classes = conf.getStrings("indexer.writer.classes");
    final NutchIndexWriter[] writers = new NutchIndexWriter[classes.length];
    for (int i = 0; i < classes.length; i++) {
      final String clazz = classes[i];
      try {
        final Class<NutchIndexWriter> implClass =
          (Class<NutchIndexWriter>) Class.forName(clazz);
        writers[i] = implClass.newInstance();
      } catch (final Exception e) {
        throw new RuntimeException("Couldn't create " + clazz, e);
      }
    }
    return writers;
  }

  public static void addClassToConf(Configuration conf,
                                    Class<? extends NutchIndexWriter> clazz) {
    final String classes = conf.get("indexer.writer.classes");
    final String newClass = clazz.getName();

    if (classes == null) {
      conf.set("indexer.writer.classes", newClass);
    } else {
      conf.set("indexer.writer.classes", classes + "," + newClass);
    }

  }

}
