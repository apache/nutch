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

import java.util.HashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import de.l3s.boilerpipe.BoilerpipeExtractor;

class BoilerpipeExtractorRepository {

    public static final Log LOG = LogFactory.getLog(BoilerpipeExtractorRepository.class);
    public static final HashMap<String, BoilerpipeExtractor> extractorRepository = new HashMap<>();
 
    /**
     * Returns an instance of the specified extractor
     */
    public static synchronized BoilerpipeExtractor getExtractor(String boilerpipeExtractorName) {
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
          LOG.error("BoilerpipeExtractor " + boilerpipeExtractorName + " not found!");
        } catch (InstantiationException e) {
          LOG.error("Could not instantiate " + boilerpipeExtractorName);
        } catch (Exception e) {
          LOG.error(e);
        }
      }

      return extractorRepository.get(boilerpipeExtractorName);
    }

}
