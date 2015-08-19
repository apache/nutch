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
package org.apache.nutch.webui.pages.components;

import java.util.Map;

import org.apache.wicket.model.IModel;

import com.google.common.collect.Maps;

import de.agilecoders.wicket.core.markup.html.bootstrap.block.LabelType;

public class ColorEnumLabelBuilder<E extends Enum<E>> {
  private Map<E, LabelType> labelTypeMap = Maps.newHashMap();
  private IModel<E> model;
  private String id;

  public ColorEnumLabelBuilder(String id) {
    this.id = id;
  }

  public ColorEnumLabelBuilder<E> withModel(IModel<E> model) {
    this.model = model;
    return this;
  }

  public ColorEnumLabelBuilder<E> withEnumColor(E e, LabelType type) {
    labelTypeMap.put(e, type);
    return this;
  }

  public ColorEnumLabel<E> build() {
    return new ColorEnumLabel<E>(id, model, labelTypeMap);
  }
}