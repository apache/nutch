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

import org.apache.wicket.markup.html.basic.EnumLabel;
import org.apache.wicket.model.AbstractReadOnlyModel;
import org.apache.wicket.model.IModel;

import de.agilecoders.wicket.core.markup.html.bootstrap.block.LabelBehavior;
import de.agilecoders.wicket.core.markup.html.bootstrap.block.LabelType;

/**
 * Label which renders connection status as bootstrap label
 * 
 * @author feodor
 * 
 */
public class ColorEnumLabel<E extends Enum<E>> extends EnumLabel<E> {
  private Map<E, LabelType> labelTypeMap;

  ColorEnumLabel(String id, IModel<E> model, Map<E, LabelType> labelTypeMap) {
    super(id, model);
    this.labelTypeMap = labelTypeMap;
  }

  @Override
  protected void onInitialize() {
    super.onInitialize();
    setOutputMarkupId(true);
    add(new LabelBehavior(new EnumCssModel(getModel())));
  }

  private class EnumCssModel extends AbstractReadOnlyModel<LabelType> {
    private IModel<E> model;

    public EnumCssModel(IModel<E> model) {
      this.model = model;
    }

    @Override
    public LabelType getObject() {
      LabelType labelType = labelTypeMap.get(model.getObject());
      if (labelType == null) {
        return LabelType.Default;
      }
      return labelType;
    }
  }

  public static <E extends Enum<E>> ColorEnumLabelBuilder<E> getBuilder(
      String id) {
    return new ColorEnumLabelBuilder<E>(id);
  }

}
