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
package org.apache.nutch.parse.tika;

import java.lang.invoke.MethodHandles;
import java.lang.ClassLoader;
import java.lang.InstantiationException;
import java.util.WeakHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.tika.parser.html.BoilerpipeContentHandler;
import de.l3s.boilerpipe.BoilerpipeExtractor;
import de.l3s.boilerpipe.extractors.*;

class BoilerpipeExtractorRepository {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());
  public static final WeakHashMap<String, BoilerpipeExtractor> extractorRepository = new WeakHashMap<String, BoilerpipeExtractor>();
 
    /**
     * Returns an instance of the specified extractor
     */
    public static BoilerpipeExtractor getExtractor(String boilerpipeExtractorName) {
      // Check if there's no instance of this extractor
      if (!extractorRepository.containsKey(boilerpipeExtractorName)) {
        // FQCN
        boilerpipeExtractorName = "de.l3s.boilerpipe.extractors." + boilerpipeExtractorName;

        // Attempt to load the class
        try {
          ClassLoader loader = BoilerpipeExtractor.class.getClassLoader();
          Class extractorClass = loader.loadClass(boilerpipeExtractorName);

          // Add an instance to the repository
          extractorRepository.put(boilerpipeExtractorName, (BoilerpipeExtractor)extractorClass.newInstance());

        } catch (ClassNotFoundException e) {
          LOG.error("BoilerpipeExtractor {} not found!", boilerpipeExtractorName);
        } catch (InstantiationException e) {
          LOG.error("Could not instantiate {}!", boilerpipeExtractorName);
        } catch (Exception e) {
          LOG.error("Error due to the {}!",boilerpipeExtractorName, e);
        }
      }

      return extractorRepository.get(boilerpipeExtractorName);
    }

}
