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
package org.apache.nutch.anthelion.mao;

import moa.core.InstancesHeader;
import moa.streams.filters.AbstractStreamFilter;
import weka.core.Instance;

/**
 * Stream which manipulates the instances within the stream before returning
 * them. This is especially used to transform the already labelled input data
 * and create a realistic setting for the simulation (e.g. removing knowledge
 * about siblings).
 * 
 * @author Robert Meusel (robert@dwslab.de)
 * 
 */
public class DataManipulationFilter extends AbstractStreamFilter {

  private static final long serialVersionUID = 1L;

  public InstancesHeader getHeader() {
    return this.inputStream.getHeader();
  }

  public Instance nextInstance() {
    Instance inst = this.inputStream.nextInstance();
    // here we manipulate fields

    for (int i = 0; i < inst.numAttributes(); i++) {
      // set siblings to 1 or 0
      // if (inst.attribute(i).name().equals("semsib") && inst.value(i) >
      // 0) {
      // inst.setValue(i, 1);
      // }
      // if (inst.attribute(i).name().equals("nonsemsib")
      // && inst.value(i) > 0) {
      // inst.setValue(i, 1);
      // }

      if (inst.attribute(i).name().equals("id")) {
        inst.setValue(i, 0);
      }
      // we know nothing about siblings
      if (inst.attribute(i).name().equals("semsib") && inst.value(i) > 0) {
        inst.setValue(i, 1);
      }
      if (inst.attribute(i).name().equals("nonsemsib")
          && inst.value(i) > 0) {
        inst.setValue(i, 1);
      }
      // set parents to 1 or 0
      if (inst.attribute(i).name().equals("sempar") && inst.value(i) > 0) {
        inst.setValue(i, 1);
      }
      if (inst.attribute(i).name().equals("nonsempar")
          && inst.value(i) > 0) {
        inst.setValue(i, 1);
      }

    }

    return inst;
  }

  public void getDescription(StringBuilder sb, int indent) {
    // we do not need this

  }

  @Override
  protected void restartImpl() {
    // do nothing
  }

}
